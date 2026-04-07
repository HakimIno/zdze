const std = @import("std");
const types = @import("../core/types.zig");

pub const Dlq = struct {
    allocator: std.mem.Allocator,
    path: []const u8,
    file: std.fs.File,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !*Dlq {
        const file = try std.fs.cwd().createFile(path, .{ .truncate = false });
        // Move to end if file exists
        try file.seekFromEnd(0);
        
        const self = try allocator.create(Dlq);
        self.* = .{
            .allocator = allocator,
            .path = try allocator.dupe(u8, path),
            .file = file,
        };
        return self;
    }

    pub fn deinit(self: *Dlq) void {
        self.file.close();
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    pub fn save(self: *Dlq, event: types.CdcEvent, err_msg: []const u8) !void {
        var buf = std.ArrayList(u8).init(self.allocator);
        defer buf.deinit();

        try std.json.stringify(.{
            .timestamp = event.timestamp,
            .op = @tagName(event.op),
            .table = event.table,
            .schema = event.schema,
            .@"error" = err_msg,
        }, .{}, buf.writer());
        
        try buf.append('\n');
        try self.file.writeAll(buf.items);
        try self.file.sync();
        
        std.log.warn("Event moved to DLQ: {s}.{s} (Error: {s})", .{event.schema, event.table, err_msg});
    }
};
