const std = @import("std");
const core = @import("../core/interface.zig");
const types = @import("../core/types.zig");
const core_err = @import("../core/error.zig");
const filter = @import("filter.zig");
const config = @import("../core/config.zig");
const dispatcher = @import("dispatcher.zig");
const dlq = @import("dlq.zig");

pub const Engine = struct {
    allocator: std.mem.Allocator,
    source: core.Source,
    sink: core.Sink,
    dispatcher: ?*dispatcher.Dispatcher = null,
    dlq: ?*dlq.Dlq = null,
    filter: filter.Filter,
    running: bool = false,
    
    metrics: struct {
        processed_count: u64 = 0,
        skipped_count: u64 = 0,
        start_time: i64 = 0,
    } = .{},

    pub fn init(allocator: std.mem.Allocator, source: core.Source, sink: core.Sink, dlq_ptr: ?*dlq.Dlq, cfg: ?config.Config.FilterConfig) !*Engine {
        const self = try allocator.create(Engine);
        self.* = .{
            .allocator = allocator,
            .source = source,
            .sink = sink,
            .dlq = dlq_ptr,
            .filter = filter.Filter.init(cfg),
        };

        // Initialize Dispatcher (default max_size 10,000)
        self.dispatcher = try dispatcher.Dispatcher.init(allocator, sink, dlq_ptr, 10000);
        return self;
    }

    pub fn run(self: *Engine) !void {
        if (self.dispatcher) |d| try d.start();
        self.running = true;
        std.log.info("Engine started", .{});

        while (self.running) {
            const event = self.source.next() catch |err| {
                std.log.err("Source error: {s}", .{@errorName(err)});
                return err;
            };

            if (event) |ev| {
                if (self.filter.shouldPass(&ev)) {
                    // Dispatch event asynchronously
                    if (self.dispatcher) |d| {
                        try d.push(ev);
                        // ev ownership transferred to dispatcher
                    } else {
                        var ev_copy = ev;
                        try self.sink.emit(ev_copy);
                        ev_copy.deinit(self.allocator);
                    }
                    self.metrics.processed_count += 1;
                } else {
                    var ev_mut = ev;
                    ev_mut.deinit(self.allocator);
                    self.metrics.skipped_count += 1;
                }
                
                // Periodically log metrics (simplified 1000 events)
                if (self.metrics.processed_count % 1000 == 0) {
                    self.logMetrics();
                }
            } else {
                std.log.info("End of stream reached", .{});
                break;
            }
        }
    }

    pub fn stop(self: *Engine) void {
        self.running = false;
    }

    fn logMetrics(self: *Engine) void {
        const now = std.time.timestamp();
        const duration = now - self.metrics.start_time;
        const rate = if (duration > 0) @as(f64, @floatFromInt(self.metrics.processed_count)) / @as(f64, @floatFromInt(duration)) else 0;
        
        std.log.info("Metrics: processed={d}, skipped={d}, rate={d:.2} ev/s", .{
            self.metrics.processed_count,
            self.metrics.skipped_count,
            rate,
        });
    }

    pub fn deinit(self: *Engine) void {
        self.logMetrics();
        if (self.dispatcher) |d| {
            d.deinit();
        } else {
            self.sink.deinit();
        }
        if (self.dlq) |d| d.deinit();
        self.allocator.destroy(self);
    }
};
