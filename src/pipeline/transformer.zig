const std = @import("std");
const types = @import("../core/types.zig");
const config = @import("../core/config.zig");

pub const Transformer = struct {
    allocator: std.mem.Allocator,
    configs: ?[]const config.Config.TransformationConfig,

    pub fn init(allocator: std.mem.Allocator, configs: ?[]const config.Config.TransformationConfig) !*Transformer {
        const self = try allocator.create(Transformer);
        self.* = .{
            .allocator = allocator,
            .configs = configs,
        };
        return self;
    }

    pub fn deinit(self: *Transformer) void {
        self.allocator.destroy(self);
    }

    pub fn transform(self: *Transformer, event: *types.CdcEvent) !void {
        const configs = self.configs orelse return;

        var new_rows = std.array_list.Managed(types.Column).init(self.allocator);
        errdefer new_rows.deinit();

        for (event.rows) |col| {
            var action: ?config.Config.TransformationAction = null;
            for (configs) |c| {
                if (std.mem.eql(u8, c.table, event.table) and std.mem.eql(u8, c.column, col.name)) {
                    action = c.action;
                    break;
                }
            }

            if (action) |act| {
                switch (act) {
                    .drop => {
                        // Skip adding this column
                        self.allocator.free(col.name);
                        switch (col.value) {
                            .string => |s| self.allocator.free(s),
                            else => {},
                        }
                    },
                    .mask => {
                        var masked_col = col;
                        switch (col.value) {
                            .string => |s| {
                                const masked = try self.allocator.alloc(u8, s.len);
                                @memset(masked, '*');
                                if (s.len > 4) {
                                    // Keep first and last char if long enough
                                    masked[0] = s[0];
                                    masked[s.len - 1] = s[s.len - 1];
                                }
                                self.allocator.free(s);
                                masked_col.value = .{ .string = masked };
                            },
                            else => {},
                        }
                        try new_rows.append(masked_col);
                    },
                }
            } else {
                try new_rows.append(col);
            }
        }

        // Replace old rows with transformed rows
        self.allocator.free(event.rows);
        event.rows = try new_rows.toOwnedSlice();
    }
};
