const std = @import("std");
const types = @import("../core/types.zig");
const config = @import("../core/config.zig");

pub const Filter = struct {
    include_tables: ?[][]const u8,
    exclude_tables: ?[][]const u8,

    pub fn init(cfg: ?config.Config.FilterConfig) Filter {
        if (cfg) |f| {
            return .{
                .include_tables = f.include_tables,
                .exclude_tables = f.exclude_tables,
            };
        }
        return .{
            .include_tables = null,
            .exclude_tables = null,
        };
    }

    pub fn shouldPass(self: Filter, event: *const types.CdcEvent) bool {
        // 1. Check Exclude List (High priority)
        if (self.exclude_tables) |excl| {
            for (excl) |table| {
                if (std.mem.eql(u8, event.table, table)) {
                    return false;
                }
            }
        }

        // 2. Check Include List
        if (self.include_tables) |incl| {
            if (incl.len == 0) return true; // Empty include means all
            for (incl) |table| {
                if (std.mem.eql(u8, event.table, table)) {
                    return true;
                }
            }
            return false; // Not in include list
        }

        return true; // No filters defined
    }
};

test "Filter inclusion" {
    const incl = [_][]const u8{"users"};
    const f = Filter{ .include_tables = @constCast(&incl), .exclude_tables = null };
    
    const ev1 = types.CdcEvent{ .table = "users", .schema = "public", .op = .insert, .rows = &.{}, .timestamp = 0 };
    const ev2 = types.CdcEvent{ .table = "orders", .schema = "public", .op = .insert, .rows = &.{}, .timestamp = 0 };
    
    try std.testing.expect(f.shouldPass(&ev1) == true);
    try std.testing.expect(f.shouldPass(&ev2) == false);
}

test "Filter exclusion" {
    const excl = [_][]const u8{"secret"};
    const f = Filter{ .include_tables = null, .exclude_tables = @constCast(&excl) };
    
    const ev1 = types.CdcEvent{ .table = "users", .schema = "public", .op = .insert, .rows = &.{}, .timestamp = 0 };
    const ev2 = types.CdcEvent{ .table = "secret", .schema = "public", .op = .insert, .rows = &.{}, .timestamp = 0 };
    
    try std.testing.expect(f.shouldPass(&ev1) == true);
    try std.testing.expect(f.shouldPass(&ev2) == false);
}
