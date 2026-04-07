const std = @import("std");
const core = @import("../core/interface.zig");
const types = @import("../core/types.zig");
const core_err = @import("../core/error.zig");
const dlq = @import("dlq.zig");

pub const Dispatcher = struct {
    allocator: std.mem.Allocator,
    sink: core.Sink,
    dlq: ?*dlq.Dlq = null,
    queue: std.fifo.LinearFifo(types.CdcEvent, .Dynamic),
    mutex: std.Thread.Mutex = .{},
    not_empty: std.Thread.Condition = .{},
    not_full: std.Thread.Condition = .{},
    max_size: usize,
    running: bool = false,
    thread: ?std.Thread = null,

    pub fn init(allocator: std.mem.Allocator, sink: core.Sink, dlq_ptr: ?*dlq.Dlq, max_size: usize) !*Dispatcher {
        const self = try allocator.create(Dispatcher);
        self.* = .{
            .allocator = allocator,
            .sink = sink,
            .dlq = dlq_ptr,
            .queue = std.fifo.LinearFifo(types.CdcEvent, .Dynamic).init(allocator),
            .max_size = max_size,
        };
        return self;
    }

    pub fn start(self: *Dispatcher) !void {
        self.running = true;
        self.thread = try std.Thread.spawn(.{}, runConsumer, .{self});
        std.log.info("Dispatcher started (max_size={d})", .{self.max_size});
    }

    pub fn stop(self: *Dispatcher) void {
        self.mutex.lock();
        self.running = false;
        self.mutex.unlock();
        self.not_empty.signal(); // Wake up consumer if it's waiting
        if (self.thread) |t| t.join();
        self.thread = null;
    }

    pub fn deinit(self: *Dispatcher) void {
        self.stop();
        // Clean up remaining events in queue
        while (self.queue.readItem()) |*ev| {
            ev.deinit(self.allocator);
        }
        self.queue.deinit();
        self.allocator.destroy(self);
    }

    pub fn push(self: *Dispatcher, event: types.CdcEvent) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        while (self.queue.readableLength() >= self.max_size and self.running) {
            // Backpressure: wait until there is space
            self.not_full.wait(&self.mutex);
        }

        if (!self.running) return error.DispatcherStopped;

        try self.queue.writeItem(event);
        self.not_empty.signal();
    }

    fn runConsumer(self: *Dispatcher) void {
        while (true) {
            self.mutex.lock();
            while (self.queue.readableLength() == 0 and self.running) {
                self.not_empty.wait(&self.mutex);
            }

            if (!self.running and self.queue.readableLength() == 0) {
                self.mutex.unlock();
                break;
            }

            var event = self.queue.readItem().?;
            self.not_full.signal();
            self.mutex.unlock();

            // Emit event to sink
            self.sink.emit(event) catch |err| {
                std.log.err("Dispatcher: Sink emit error: {s}", .{@errorName(err)});
                if (self.dlq) |d| {
                    d.save(event, @errorName(err)) catch |dlq_err| {
                        std.log.err("Dispatcher: Failed to save to DLQ: {s}", .{@errorName(dlq_err)});
                    };
                }
            };

            // Cleanup event after emission
            event.deinit(self.allocator);
        }
    }
};

const MockSink = struct {
    processed: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    delay_ms: u64 = 0,

    pub fn emit(ptr: *anyopaque, event: types.CdcEvent) core_err.Error!void {
        const self: *MockSink = @ptrCast(@alignCast(ptr));
        _ = event;
        if (self.delay_ms > 0) std.Thread.sleep(self.delay_ms * std.time.ns_per_ms);
        _ = self.processed.fetchAdd(1, .monotonic);
    }

    pub fn sink(self: *MockSink) core.Sink {
        return .{
            .ptr = self,
            .vtable = &.{
                .emit = emit,
                .deinit = vtableDeinit,
            },
        };
    }

    fn vtableDeinit(ptr: *anyopaque) void {
        _ = ptr;
    }
};

test "Dispatcher: basic processing" {
    const allocator = std.testing.allocator;
    var mock = MockSink{};
    const d = try Dispatcher.init(allocator, mock.sink(), null, 100);
    defer d.deinit();

    try d.start();
    
    // Push 10 events
    for (0..10) |_| {
        try d.push(.{
            .op = .insert,
            .table = try allocator.dupe(u8, "users"),
            .schema = try allocator.dupe(u8, "public"),
            .rows = try allocator.alloc(types.Column, 0),
            .timestamp = 0,
        });
    }

    // Wait for consumer to finish
    var attempts: usize = 0;
    while (mock.processed.load(.monotonic) < 10 and attempts < 100) : (attempts += 1) {
        std.Thread.sleep(10 * std.time.ns_per_ms);
    }

    try std.testing.expectEqual(@as(usize, 10), mock.processed.load(.monotonic));
}

test "Dispatcher: backpressure" {
    const allocator = std.testing.allocator;
    var mock = MockSink{ .delay_ms = 50 }; // Slow sink
    const d = try Dispatcher.init(allocator, mock.sink(), null, 2); // Small queue
    defer d.deinit();

    try d.start();
    
    const start_time = std.time.milliTimestamp();
    
    // Push 5 events. Should be blocked by backpressure
    for (0..5) |_| {
        try d.push(.{
            .op = .insert,
            .table = try allocator.dupe(u8, "users"),
            .schema = try allocator.dupe(u8, "public"),
            .rows = try allocator.alloc(types.Column, 0),
            .timestamp = 0,
        });
    }
    
    const end_time = std.time.milliTimestamp();
    const duration = end_time - start_time;
    
    // Each event takes ~50ms. With 5 events, it should take at least ~150-200ms 
    // because the queue of size 2 will fill up and block the producer.
    try std.testing.expect(duration >= 100);
}
