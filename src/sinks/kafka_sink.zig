const std = @import("std");
const core = @import("../core/interface.zig");
const types = @import("../core/types.zig");
const core_err = @import("../core/error.zig");

pub const KafkaSink = struct {
    allocator: std.mem.Allocator,
    brokers: []const u8,
    topic: []const u8,
    latency_ms: u64 = 100, // Simulated network latency

    pub fn init(allocator: std.mem.Allocator, brokers: []const u8, topic: []const u8) !*KafkaSink {
        const self = try allocator.create(KafkaSink);
        self.* = .{
            .allocator = allocator,
            .brokers = try allocator.dupe(u8, brokers),
            .topic = try allocator.dupe(u8, topic),
        };
        std.log.info("KafkaSink initialized (brokers={s}, topic={s})", .{self.brokers, self.topic});
        return self;
    }

    pub fn deinit(self: *KafkaSink) void {
        self.allocator.free(self.brokers);
        self.allocator.free(self.topic);
        self.allocator.destroy(self);
    }

    pub fn emit(ptr: *anyopaque, event: types.CdcEvent) core_err.Error!void {
        const self: *KafkaSink = @ptrCast(@alignCast(ptr));
        
        // Simulation of message handshake/sending
        std.Thread.sleep(self.latency_ms * std.time.ns_per_ms);
        
        // Log to stdout as simulation
        std.log.debug("KafkaSink: [topic={s}] Sent event: op={s}, table={s}.{s}", 
            .{self.topic, @tagName(event.op), event.schema, event.table});
    }

    pub fn sink(self: *KafkaSink) core.Sink {
        return .{
            .ptr = self,
            .vtable = &.{
                .emit = emit,
                .deinit = vtableDeinit,
            },
        };
    }

    fn vtableDeinit(ptr: *anyopaque) void {
        const self: *KafkaSink = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};
