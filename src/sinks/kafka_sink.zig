const std = @import("std");
const core = @import("../core/interface.zig");
const types = @import("../core/types.zig");
const core_err = @import("../core/error.zig");
const net = std.net;

pub const KafkaSink = struct {
    allocator: std.mem.Allocator,
    brokers: []const u8,
    topic: []const u8,
    stream: ?net.Stream = null,
    correlation_id: i32 = 0,

    pub fn init(allocator: std.mem.Allocator, brokers: []const u8, topic: []const u8) !*KafkaSink {
        const self = try allocator.create(KafkaSink);
        self.* = .{
            .allocator = allocator,
            .brokers = try allocator.dupe(u8, brokers),
            .topic = try allocator.dupe(u8, topic),
        };
        
        // Attempt initial connection (lazy connection could be better, but let's try now)
        self.connect() catch |err| {
            std.log.warn("KafkaSink: Initial connection failed to {s}: {s}. Will retry on emit.", .{self.brokers, @errorName(err)});
        };
        
        return self;
    }

    fn connect(self: *KafkaSink) !void {
        if (self.stream) |_| return;
        
        // Parse "host:port"
        var it = std.mem.split(u8, self.brokers, ":");
        const host = it.next() orelse return error.InvalidBrokerFormat;
        const port_str = it.next() orelse "9092";
        const port = try std.fmt.parseInt(u16, port_str, 10);
        
        self.stream = try net.tcpConnectToHost(self.allocator, host, port);
        std.log.info("KafkaSink: Connected to {s}", .{self.brokers});
    }

    pub fn deinit(self: *KafkaSink) void {
        if (self.stream) |s| s.close();
        self.allocator.free(self.brokers);
        self.allocator.free(self.topic);
        self.allocator.destroy(self);
    }

    pub fn emit(ptr: *anyopaque, event: types.CdcEvent) core_err.Error!void {
        const self: *KafkaSink = @ptrCast(@alignCast(ptr));
        
        if (self.stream == null) {
            self.connect() catch return core_err.Error.InternalError;
        }

        const stream = self.stream.?;
        const writer = stream.writer();

        // 1. Serialize Event to JSON for value
        var val_buf = std.ArrayList(u8).init(self.allocator);
        defer val_buf.deinit();
        
        // Simplified JSON for Kafka value
        std.json.stringify(.{
            .op = @tagName(event.op),
            .table = event.table,
            .schema = event.schema,
            .ts = event.timestamp,
        }, .{}, val_buf.writer()) catch return core_err.Error.InternalError;

        // 2. Build Kafka Produce Request (Minimal v0)
        // [RequestSize][ApiKey][ApiVer][CorrId][ClientIdLen][ClientId][Acks][Timeout][TopicCount][TopicNameLen][TopicName][PartCount][Partition][MsgSetSize][Offset][MsgSize][CRC][Magic][Attr][KeyLen][ValLen][Val]
        
        const client_id = "zdze";
        const topic = self.topic;
        
        // Message Payload
        var msg_payload = std.ArrayList(u8).init(self.allocator);
        defer msg_payload.deinit();
        const msg_writer = msg_payload.writer();
        
        try msg_writer.writeByte(0); // Magic
        try msg_writer.writeByte(0); // Attributes
        try msg_writer.writeInt(i32, -1, .big); // Key Length (-1 = null)
        try msg_writer.writeInt(i32, @intCast(val_buf.items.len), .big); // Val Length
        try msg_writer.writeAll(val_buf.items);
        
        const crc = std.hash.Crc32.hash(msg_payload.items);
        
        // Entire Request Buffer
        var req_buf = std.ArrayList(u8).init(self.allocator);
        defer req_buf.deinit();
        const req_writer = req_buf.writer();
        
        // Header
        try req_writer.writeInt(i16, 0, .big); // API Key: Produce
        try req_writer.writeInt(i16, 0, .big); // API version
        try req_writer.writeInt(i32, self.correlation_id, .big);
        self.correlation_id += 1;
        
        try req_writer.writeInt(i16, @intCast(client_id.len), .big);
        try req_writer.writeAll(client_id);
        
        // Produce Body
        try req_writer.writeInt(i16, 1, .big); // Required Acks
        try req_writer.writeInt(i32, 1000, .big); // Timeout
        try req_writer.writeInt(i32, 1, .big); // Topic Count
        
        try req_writer.writeInt(i16, @intCast(topic.len), .big);
        try req_writer.writeAll(topic);
        
        try req_writer.writeInt(i32, 1, .big); // Partition Count
        try req_writer.writeInt(i32, 0, .big); // Partition 0
        
        // MessageSet
        const msg_set_size = 8 + 4 + 4 + msg_payload.items.len; // Offset + Size + CRC + Payload
        try req_writer.writeInt(i32, @intCast(msg_set_size), .big);
        try req_writer.writeInt(i64, 0, .big); // Offset (0 for Produce)
        try req_writer.writeInt(i32, @intCast(4 + msg_payload.items.len), .big); // Message Size
        try req_writer.writeInt(u32, crc, .big);
        try req_writer.writeAll(msg_payload.items);

        // Final Write
        try writer.writeInt(i32, @intCast(req_buf.items.len), .big);
        try writer.writeAll(req_buf.items);
        
        // Note: In production we should read response to verify Acks, 
        // but for this phase we focus on the outgoing Native Binary Protocol.
        std.log.debug("KafkaSink: [topic={s}] Sent binary ProduceRequest ({d} bytes)", .{topic, req_buf.items.len});
    }

    pub fn sink(self: *KafkaSink) core.Sink {
        return .{
            .ptr = self,
            .vtable = &.{
                .emit = emit,
                .deinit = vtableDeinit,
            },
        };
    }

    fn vtableDeinit(ptr: *anyopaque) void {
        const self: *KafkaSink = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};
