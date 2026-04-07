const std = @import("std");
const net = std.net;
const http = std.http;
const engine = @import("../pipeline/engine.zig");

pub const HttpServer = struct {
    allocator: std.mem.Allocator,
    engine: *engine.Engine,
    port: u16 = 8080,
    running: bool = false,
    thread: ?std.Thread = null,

    pub fn init(allocator: std.mem.Allocator, e: *engine.Engine, port: u16) !*HttpServer {
        const self = try allocator.create(HttpServer);
        self.* = .{
            .allocator = allocator,
            .engine = e,
            .port = port,
        };
        return self;
    }

    pub fn start(self: *HttpServer) !void {
        self.running = true;
        self.thread = try std.Thread.spawn(.{}, runServer, .{self});
        std.log.info("HTTP Dashboard available at http://localhost:{d}", .{self.port});
    }

    pub fn stop(self: *HttpServer) void {
        self.running = false;
        if (self.thread) |t| t.join();
        self.thread = null;
    }

    pub fn deinit(self: *HttpServer) void {
        self.stop();
        self.allocator.destroy(self);
    }

    fn runServer(self: *HttpServer) !void {
        const address = net.Address.parseIp("0.0.0.0", self.port) catch unreachable;
        var server = try address.listen(.{ .reuse_address = true });
        defer server.deinit();

        while (self.running) {
            var conn = server.accept() catch continue;
            defer conn.stream.close();

            var read_buffer: [1024 * 4]u8 = undefined;
            var server_http = http.Server.init(conn, &read_buffer);
            
            var request = server_http.receiveHead() catch continue;
            
            if (std.mem.eql(u8, request.head.target, "/api/metrics")) {
                try handleMetrics(self, &request);
            } else if (std.mem.eql(u8, request.head.target, "/") or std.mem.eql(u8, request.head.target, "/index.html")) {
                try handleIndex(self, &request);
            } else {
                try request.respond("", .{ .status = .not_found });
            }
        }
    }

    fn handleMetrics(self: *HttpServer, request: *http.Server.Request) !void {
        const metrics = self.engine.metrics;
        const now = std.time.timestamp();
        const duration = now - metrics.start_time;
        const rate = if (duration > 0) @as(f64, @floatFromInt(metrics.processed_count)) / @as(f64, @floatFromInt(duration)) else 0;

        var json_buf: [1024]u8 = undefined;
        const json = try std.fmt.bufPrint(&json_buf, 
            \\{{
            \\  "processed_count": {d},
            \\  "skipped_count": {d},
            \\  "rate": {d:.2},
            \\  "uptime_sec": {d}
            \\}}
        , .{ metrics.processed_count, metrics.skipped_count, rate, duration });

        try request.respond(json, .{
            .extra_headers = &.{.{ .name = "Content-Type", .value = "application/json" }},
        });
    }

    fn handleIndex(self: *HttpServer, request: *http.Server.Request) !void {
        _ = self;
        // In a full implementation, we'd @embedFile("dashboard.html")
        // For now, we'll serve a basic but beautiful HTML string
        const html = 
            \\<!DOCTYPE html>
            \\<html>
            \\<head>
            \\  <title>zdze Dashboard</title>
            \\  <style>
            \\    body { font-family: 'Inter', sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 2rem; }
            \\    .container { max-width: 800px; margin: 0 auto; }
            \\    .card { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(10px); border-radius: 1rem; padding: 2rem; border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5); }
            \\    h1 { margin-top: 0; background: linear-gradient(to right, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2.5rem; }
            \\    .stats { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; margin-top: 2rem; }
            \\    .stat-item { background: rgba(15, 23, 42, 0.5); padding: 1.5rem; border-radius: 0.75rem; }
            \\    .stat-label { color: #94a3b8; font-size: 0.875rem; text-transform: uppercase; letter-spacing: 0.05em; }
            \\    .stat-value { font-size: 2rem; font-weight: bold; margin-top: 0.5rem; }
            \\    .loading { color: #38bdf8; }
            \\  </style>
            \\  <script>
            \\    async function updateMetrics() {
            \\      try {
            \\        const res = await fetch('/api/metrics');
            \\        const data = await res.json();
            \\        document.getElementById('processed').innerText = data.processed_count.toLocaleString();
            \\        document.getElementById('rate').innerText = data.rate.toFixed(2) + ' ev/s';
            \\        document.getElementById('uptime').innerText = data.uptime_sec + 's';
            \\      } catch (e) {
            \\        console.error('Failed to fetch metrics', e);
            \\      }
            \\    }
            \\    setInterval(updateMetrics, 1000);
            \\  </script>
            \\</head>
            \\<body>
            \\  <div class="container">
            \\    <div class="card">
            \\      <h1>zdze Control Center</h1>
            \\      <p style="color: #94a3b8;">Real-time Pipeline Monitoring</p>
            \\      <div class="stats">
            \\        <div class="stat-item">
            \\          <div class="stat-label">Processed Events</div>
            \\          <div id="processed" class="stat-value">...</div>
            \\        </div>
            \\        <div class="stat-item">
            \\          <div class="stat-label">Throughput</div>
            \\          <div id="rate" class="stat-value">...</div>
            \\        </div>
            \\        <div class="stat-item">
            \\          <div class="stat-label">Uptime</div>
            \\          <div id="uptime" class="stat-value">...</div>
            \\        </div>
            \\        <div class="stat-item">
            \\          <div class="stat-label">Status</div>
            \\          <div class="stat-value" style="color: #4ade80;">Active</div>
            \\        </div>
            \\      </div>
            \\    </div>
            \\  </div>
            \\</body>
            \\</html>
        ;
        try request.respond(html, .{
            .extra_headers = &.{.{ .name = "Content-Type", .value = "text/html" }},
        });
    }
};
