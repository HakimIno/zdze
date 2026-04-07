const std = @import("std");
const core = @import("../core/interface.zig");
const types = @import("../core/types.zig");
const core_err = @import("../core/error.zig");

pub const MockSource = struct {
    allocator: std.mem.Allocator,
    count: usize = 0,
    max_count: usize,

    pub fn init(allocator: std.mem.Allocator, max: usize) !*MockSource {
        const self = try allocator.create(MockSource);
        self.* = .{
            .allocator = allocator,
            .max_count = max,
        };
        return self;
    }

    pub fn vtableDeinit(ptr: *anyopaque) void {
        const self: *MockSource = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    pub fn deinit(self: *MockSource) void {
        self.allocator.destroy(self);
    }

    pub fn next(ptr: *anyopaque) core_err.Error!?types.CdcEvent {
        const self: *MockSource = @ptrCast(@alignCast(ptr));
        if (self.count >= self.max_count) return null;

        const row = self.allocator.alloc(types.Column, 1) catch return core_err.Error.OutOfMemory;
        row[0] = .{
            .name = self.allocator.dupe(u8, "id") catch return core_err.Error.OutOfMemory,
            .value = .{ .integer = @intCast(self.count) },
        };

        const event = types.CdcEvent{
            .op = .insert,
            .table = self.allocator.dupe(u8, "users") catch return core_err.Error.OutOfMemory,
            .schema = self.allocator.dupe(u8, "public") catch return core_err.Error.OutOfMemory,
            .rows = row,
            .timestamp = std.time.microTimestamp(),
            .lsn = 0,
        };

        self.count += 1;
        return event;
    }

    pub fn source(self: *MockSource) core.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .next = next,
                .deinit = vtableDeinit,
            },
        };
    }
};
