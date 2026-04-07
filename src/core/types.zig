const std = @import("std");
const core_error = @import("error.zig");

pub const Operation = enum {
    insert,
    update,
    delete,
    truncate,
};

pub const Value = union(enum) {
    null: void,
    boolean: bool,
    integer: i64,
    float: f64,
    string: []const u8,
    bytes: []const u8,
    timestamp: i64, // Microseconds since epoch
};

pub const Column = struct {
    name: []const u8,
    value: Value,
};

pub const CdcEvent = struct {
    op: Operation,
    table: []const u8,
    schema: []const u8,
    rows: []const Column,
    timestamp: i64,
    lsn: u64,

    pub fn deinit(self: *CdcEvent, allocator: std.mem.Allocator) void {
        for (self.rows) |col| {
            switch (col.value) {
                .string => |s| allocator.free(s),
                .bytes => |b| allocator.free(b),
                else => {},
            }
            allocator.free(col.name);
        }
        allocator.free(self.rows);
        allocator.free(self.table);
        allocator.free(self.schema);
    }
};
