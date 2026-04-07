const std = @import("std");
const http = std.http;
const core = @import("../core/interface.zig");
const types = @import("../core/types.zig");
const core_err = @import("../core/error.zig");

pub const WebhookSink = struct {
    allocator: std.mem.Allocator,
    client: http.Client,
    url: []const u8,

    pub fn init(allocator: std.mem.Allocator, url: []const u8) !*WebhookSink {
        const self = try allocator.create(WebhookSink);
        self.* = .{
            .allocator = allocator,
            .client = http.Client{ .allocator = allocator },
            .url = try allocator.dupe(u8, url),
        };
        return self;
    }

    pub fn deinit(self: *WebhookSink) void {
        self.client.deinit();
        self.allocator.free(self.url);
        self.allocator.destroy(self);
    }

    pub fn emit(ptr: *anyopaque, event: types.CdcEvent) core_err.Error!void {
        const self: *WebhookSink = @ptrCast(@alignCast(ptr));
        
        // Serialize event to JSON
        var out: std.io.Writer.Allocating = .init(self.allocator);
        defer out.deinit();
        
        std.json.Stringify.value(event, .{}, &out.writer) catch { return core_err.Error.WriteFailed; };

        // Send HTTP POST
        const uri = std.Uri.parse(self.url) catch { return core_err.Error.WriteFailed; };
        
        const resp = self.client.fetch(.{
            .method = .POST,
            .location = .{ .uri = uri },
            .payload = out.written(),
        }) catch { return core_err.Error.WriteFailed; };

        if (resp.status != .ok and resp.status != .created) {
            std.log.err("Webhook failed with status: {d}", .{@intFromEnum(resp.status)});
            return core_err.Error.WriteFailed;
        }
    }

    pub fn vtableDeinit(ptr: *anyopaque) void {
        const self: *WebhookSink = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    pub fn sink(self: *WebhookSink) core.Sink {
        return .{
            .ptr = self,
            .vtable = &.{
                .emit = emit,
                .deinit = vtableDeinit,
            },
        };
    }
};
