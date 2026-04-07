const std = @import("std");
const fs = std.fs;

pub const State = struct {
    last_lsn: u64,
    updated_at: i64,

    pub fn init(lsn: u64) State {
        return .{
            .last_lsn = lsn,
            .updated_at = std.time.microTimestamp(),
        };
    }
};

pub const Persistence = struct {
    allocator: std.mem.Allocator,
    path: []const u8,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Persistence {
        return Persistence{
            .allocator = allocator,
            .path = try allocator.dupe(u8, path),
        };
    }

    pub fn deinit(self: Persistence) void {
        self.allocator.free(self.path);
    }

    pub fn save(self: Persistence, state: State) !void {
        const tmp_path = try std.fmt.allocPrint(self.allocator, "{s}.tmp", .{self.path});
        defer self.allocator.free(tmp_path);

        const file = try fs.cwd().createFile(tmp_path, .{});
        defer file.close();

        var write_buffer: [1024]u8 = undefined;
        var w = file.writer(&write_buffer);
        
        var lsn_buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &lsn_buf, state.last_lsn, .little);
        try (&w.interface).writeAll(&lsn_buf);

        var ts_buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &ts_buf, state.updated_at, .little);
        try (&w.interface).writeAll(&ts_buf);

        // Ensure data is on disk
        try file.sync();
        file.close();

        // Atomic rename
        try fs.cwd().rename(tmp_path, self.path);
    }

    pub fn load(self: Persistence) !State {
        const file = try fs.cwd().openFile(self.path, .{});
        defer file.close();

        var read_buffer: [1024]u8 = undefined;
        var r = file.reader(&read_buffer);
        
        var lsn_buf: [8]u8 = undefined;
        try (&r.interface).readSliceAll(&lsn_buf);
        const lsn = std.mem.readInt(u64, &lsn_buf, .little);

        var ts_buf: [8]u8 = undefined;
        try (&r.interface).readSliceAll(&ts_buf);
        const ts = std.mem.readInt(i64, &ts_buf, .little);

        return State{
            .last_lsn = lsn,
            .updated_at = ts,
        };
    }
};
