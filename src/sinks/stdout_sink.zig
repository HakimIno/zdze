const std = @import("std");
const core = @import("../core/interface.zig");
const types = @import("../core/types.zig");
const core_err = @import("../core/error.zig");

pub const StdoutSink = struct {
    allocator: std.mem.Allocator,
    out: std.fs.File,

    pub fn init(allocator: std.mem.Allocator) !*StdoutSink {
        const self = try allocator.create(StdoutSink);
        self.* = .{
            .allocator = allocator,
            .out = std.fs.File.stdout(),
        };
        return self;
    }

    pub fn vtableDeinit(ptr: *anyopaque) void {
        const self: *StdoutSink = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    pub fn deinit(self: *StdoutSink) void {
        self.allocator.destroy(self);
    }

    pub fn emit(ptr: *anyopaque, event: types.CdcEvent) core_err.Error!void {
        const self: *StdoutSink = @ptrCast(@alignCast(ptr));
        
        var buffer: [1]u8 = undefined;
        var w = self.out.writer(&buffer);
        std.json.Stringify.value(event, .{}, &w.interface) catch return core_err.Error.WriteFailed;
        w.interface.writeByte('\n') catch return core_err.Error.WriteFailed;
    }

    pub fn sink(self: *StdoutSink) core.Sink {
        return .{
            .ptr = self,
            .vtable = &.{
                .emit = emit,
                .deinit = vtableDeinit,
            },
        };
    }
};
