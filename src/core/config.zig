const std = @import("std");
const postgres = @import("../sources/postgres/source.zig");

pub const Config = struct {
    source: SourceConfig,
    sink: SinkConfig,
    filters: ?FilterConfig = null,
    state_path: []const u8 = "zdze_state.bin",

    pub const FilterConfig = struct {
        include_tables: ?[][]const u8 = null,
        exclude_tables: ?[][]const u8 = null,
    };

    pub const SourceConfig = struct {
        type: enum { postgres, mock },
        postgres: ?postgres.PostgresSource.Config = null,
        mock_count: usize = 10,
    };

    pub const SinkConfig = struct {
        type: enum { stdout, webhook },
        webhook_url: ?[]const u8 = null,
    };

    pub fn load(allocator: std.mem.Allocator, path: []const u8) !Config {
        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        const content = try file.readToEndAlloc(allocator, 1024 * 1024);
        defer allocator.free(content);

        const parsed = try std.json.parseFromSlice(Config, allocator, content, .{
            .ignore_unknown_fields = true,
        });
        defer parsed.deinit();

        // Deep copy because parseFromSlice's result is tied to the internal arena
        return Config{
            .source = .{
                .type = parsed.value.source.type,
                .postgres = if (parsed.value.source.postgres) |p| try dupePostgres(allocator, p) else null,
                .mock_count = parsed.value.source.mock_count,
            },
            .sink = .{
                .type = parsed.value.sink.type,
                .webhook_url = if (parsed.value.sink.webhook_url) |url| try allocator.dupe(u8, url) else null,
            },
            .filters = if (parsed.value.filters) |f| try dupeFilters(allocator, f) else null,
            .state_path = try allocator.dupe(u8, parsed.value.state_path),
        };
    }

    fn dupeFilters(allocator: std.mem.Allocator, f: FilterConfig) !FilterConfig {
        var inc: ?[][]const u8 = null;
        if (f.include_tables) |it| {
            inc = try allocator.alloc([]const u8, it.len);
            for (it, 0..) |t, i| {
                inc.?[i] = try allocator.dupe(u8, t);
            }
        }
        var exc: ?[][]const u8 = null;
        if (f.exclude_tables) |et| {
            exc = try allocator.alloc([]const u8, et.len);
            for (et, 0..) |t, i| {
                exc.?[i] = try allocator.dupe(u8, t);
            }
        }
        return FilterConfig{
            .include_tables = inc,
            .exclude_tables = exc,
        };
    }

    fn dupePostgres(allocator: std.mem.Allocator, p: postgres.PostgresSource.Config) !postgres.PostgresSource.Config {
        return .{
            .host = try allocator.dupe(u8, p.host),
            .port = p.port,
            .user = try allocator.dupe(u8, p.user),
            .database = try allocator.dupe(u8, p.database),
            .password = if (p.password) |pw| try allocator.dupe(u8, pw) else null,
            .slot_name = try allocator.dupe(u8, p.slot_name),
        };
    }

    pub fn deinit(self: Config, allocator: std.mem.Allocator) void {
        allocator.free(self.state_path);
        if (self.sink.webhook_url) |url| allocator.free(url);
        if (self.source.postgres) |p| {
            allocator.free(p.host);
            allocator.free(p.user);
            allocator.free(p.database);
            if (p.password) |pw| allocator.free(pw);
            allocator.free(p.slot_name);
        }
        if (self.filters) |f| {
            if (f.include_tables) |it| {
                for (it) |t| allocator.free(t);
                allocator.free(it);
            }
            if (f.exclude_tables) |et| {
                for (et) |t| allocator.free(t);
                allocator.free(et);
            }
        }
    }
};
