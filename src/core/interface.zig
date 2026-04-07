const std = @import("std");
const types = @import("types.zig");
const core_error = @import("error.zig");

pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        next: *const fn (ptr: *anyopaque) core_error.Error!?types.CdcEvent,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn next(self: Source) core_error.Error!?types.CdcEvent {
        return self.vtable.next(self.ptr);
    }

    pub fn deinit(self: Source) void {
        self.vtable.deinit(self.ptr);
    }
};

pub const Sink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        emit: *const fn (ptr: *anyopaque, event: types.CdcEvent) core_error.Error!void,
        deinit: *const fn (ptr: *anyopaque) void,
    };

    pub fn emit(self: Sink, event: types.CdcEvent) core_error.Error!void {
        return self.vtable.emit(self.ptr, event);
    }

    pub fn deinit(self: Sink) void {
        self.vtable.deinit(self.ptr);
    }
};
