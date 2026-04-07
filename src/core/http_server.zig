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
    
    // History buffer for charts (last 60 seconds)
    history: std.array_list.Managed(MetricPoint),
    mutex: std.Thread.Mutex = .{},

    pub const MetricPoint = struct {
        ts: i64,
        rate: f64,
    };

    pub fn init(allocator: std.mem.Allocator, e: *engine.Engine, port: u16) !*HttpServer {
        const self = try allocator.create(HttpServer);
        self.* = .{
            .allocator = allocator,
            .engine = e,
            .port = port,
            .history = std.array_list.Managed(MetricPoint).init(allocator),
        };
        return self;
    }

    pub fn start(self: *HttpServer) !void {
        self.running = true;
        self.thread = try std.Thread.spawn(.{}, runServer, .{self});
        
        // Metrics collection thread
        _ = try std.Thread.spawn(.{}, collectMetrics, .{self});
        
        std.log.info("Elite Dashboard available at http://localhost:{d}", .{self.port});
    }

    fn collectMetrics(self: *HttpServer) !void {
        while (self.running) {
            std.Thread.sleep(1 * std.time.ns_per_s);
            
            const metrics = self.engine.metrics;
            const now = std.time.timestamp();
            const duration = now - metrics.start_time;
            const rate = if (duration > 0) @as(f64, @floatFromInt(metrics.processed_count)) / @as(f64, @floatFromInt(duration)) else 0;

            self.mutex.lock();
            if (self.history.items.len >= 60) {
                _ = self.history.orderedRemove(0);
            }
            self.history.append(.{ .ts = now, .rate = rate }) catch {};
            self.mutex.unlock();
        }
    }

    pub fn stop(self: *HttpServer) void {
        self.running = false;
        if (self.thread) |t| t.join();
        self.thread = null;
    }

    pub fn deinit(self: *HttpServer) void {
        self.stop();
        self.history.deinit();
        self.allocator.destroy(self);
    }

    fn runServer(self: *HttpServer) !void {
        const address = net.Address.parseIp("0.0.0.0", self.port) catch unreachable;
        var server = try address.listen(.{ .reuse_address = true });
        defer server.deinit();

        while (self.running) {
            var conn = server.accept() catch continue;
            defer conn.stream.close();

            var read_buffer: [1024 * 8]u8 = undefined;
            var write_buffer: [1024]u8 = undefined;
            var reader = conn.stream.reader(&read_buffer);
            var writer = conn.stream.writer(&write_buffer);
            var server_http = http.Server.init(&reader.file_reader.interface, &writer.interface);
            
            var request = server_http.receiveHead() catch continue;
            
            if (std.mem.eql(u8, request.head.target, "/api/metrics")) {
                try handleMetrics(self, &request);
            } else if (std.mem.eql(u8, request.head.target, "/api/history")) {
                try handleHistory(self, &request);
            } else if (std.mem.eql(u8, request.head.target, "/") or std.mem.eql(u8, request.head.target, "/index.html")) {
                try handleIndex(self, &request);
            } else {
                try request.respond("", .{ .status = .not_found });
            }
        }
    }

    fn handleHistory(self: *HttpServer, request: *http.Server.Request) !void {
        var out = std.io.Writer.Allocating.init(self.allocator);
        defer out.deinit();

        try out.writer.writeByte('[');
        self.mutex.lock();
        const items = self.history.items;
        for (items, 0..) |item, i| {
            try std.json.Stringify.value(item, .{}, &out.writer);
            if (i < items.len - 1) try out.writer.writeByte(',');
        }
        self.mutex.unlock();
        try out.writer.writeByte(']');
        try request.respond(out.written(), .{
            .extra_headers = &.{.{ .name = "content-type", .value = "application/json" }},
        });
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
        const html = 
            \\<!DOCTYPE html>
            \\<html>
            \\<head>
            \\  <title>zdze Elite Dashboard</title>
            \\  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
            \\  <style>
            \\    body { font-family: 'Inter', sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 2rem; }
            \\    .container { max-width: 1000px; margin: 0 auto; }
            \\    .card { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(10px); border-radius: 1rem; padding: 2rem; border: 1px solid rgba(255,255,255,0.1); box-shadow: 0 25px 50px -12px rgba(0,0,0,0.5); margin-bottom: 2rem; }
            \\    h1 { margin-top: 0; background: linear-gradient(to right, #38bdf8, #818cf8); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2.5rem; }
            \\    .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem; margin-top: 2rem; }
            \\    .stat-item { background: rgba(15, 23, 42, 0.5); padding: 1.5rem; border-radius: 0.75rem; border: 1px solid rgba(255,255,255,0.05); }
            \\    .stat-label { color: #94a3b8; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
            \\    .stat-value { font-size: 1.5rem; font-weight: bold; }
            \\    .chart-container { height: 300px; margin-top: 2rem; }
            \\  </style>
            \\</head>
            \\<body>
            \\  <div class="container">
            \\    <div class="card">
            \\      <h1>zdze Elite Dashboard</h1>
            \\      <p style="color: #94a3b8;">Real-time Pipeline Intelligence</p>
            \\      
            \\      <div class="stats">
            \\        <div class="stat-item">
            \\          <div class="stat-label">Processed</div>
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
            \\          <div class="stat-label">Engine Status</div>
            \\          <div class="stat-value" style="color: #4ade80;">Running</div>
            \\        </div>
            \\      </div>
            \\
            \\      <div class="chart-container">
            \\        <canvas id="myChart"></canvas>
            \\      </div>
            \\    </div>
            \\  </div>
            \\
            \\  <script>
            \\    const ctx = document.getElementById('myChart').getContext('2d');
            \\    const myChart = new Chart(ctx, {
            \\      type: 'line',
            \\      data: {
            \\        labels: [],
            \\        datasets: [{
            \\          label: 'Events / Second',
            \\          data: [],
            \\          borderColor: '#38bdf8',
            \\          backgroundColor: 'rgba(56, 189, 248, 0.1)',
            \\          tension: 0.4,
            \\          fill: true
            \\        }]
            \\      },
            \\      options: {
            \\        responsive: true,
            \\        maintainAspectRatio: false,
            \\        scales: {
            \\          y: { beginAtZero: true, grid: { color: 'rgba(255,255,255,0.05)' }, border: { display: false } },
            \\          x: { grid: { display: false }, border: { display: false } }
            \\        },
            \\        plugins: { legend: { display: false } }
            \\      }
            \\    });
            \\
            \\    async function updateDashboard() {
            \\      try {
            \\        const metricsRes = await fetch('/api/metrics');
            \\        const metrics = await metricsRes.json();
            \\        document.getElementById('processed').innerText = metrics.processed_count.toLocaleString();
            \\        document.getElementById('rate').innerText = metrics.rate.toFixed(2) + ' ev/s';
            \\        document.getElementById('uptime').innerText = metrics.uptime_sec + 's';
            \\
            \\        const historyRes = await fetch('/api/history');
            \\        const history = await historyRes.json();
            \\        
            \\        myChart.data.labels = history.map(p => new Date(p.ts * 1000).toLocaleTimeString());
            \\        myChart.data.datasets[0].data = history.map(p => p.rate);
            \\        myChart.update('none');
            \\      } catch (e) { console.error(e); }
            \\    }
            \\    setInterval(updateDashboard, 1000);
            \\  </script>
            \\</body>
            \\</html>
        ;
        try request.respond(html, .{
            .extra_headers = &.{.{ .name = "Content-Type", .value = "text/html" }},
        });
    }
};
