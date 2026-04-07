const std = @import("std");
const net = std.net;
const core = @import("../../core/interface.zig");
const types = @import("../../core/types.zig");
const core_err = @import("../../core/error.zig");
const proto = @import("proto.zig");

pub const PostgresSource = struct {
    allocator: std.mem.Allocator,
    config: Config,
    stream: net.Stream,
    persistence: ?@import("../../core/persistence.zig").Persistence = null,
    authenticated: bool = false,
    ready: bool = false,
    last_lsn: u64 = 0,
    relations: std.AutoHashMap(u32, RelationInfo),
    retry_count: u32 = 0,
    read_buf: [4096]u8 = undefined,
    write_buf: [4096]u8 = undefined,

    pub const RelationInfo = struct {
        name: []const u8,
        schema: []const u8,
        columns: []const types.Column, // Templates
    };

    pub const Config = struct {
        host: []const u8 = "127.0.0.1",
        port: u16 = 5432,
        user: []const u8,
        database: []const u8,
        password: ?[]const u8 = null,
        slot_name: []const u8 = "zdze_slot",
        ssl: bool = false,
    };

    pub fn init(allocator: std.mem.Allocator, config: Config, persistence: ?@import("../../core/persistence.zig").Persistence) !*PostgresSource {
        const stream = try net.tcpConnectToHost(allocator, config.host, config.port);
        errdefer stream.close();

        var self = try allocator.create(PostgresSource);
        self.* = .{
            .allocator = allocator,
            .config = config,
            .stream = stream,
            .persistence = persistence,
            .relations = std.AutoHashMap(u32, RelationInfo).init(allocator),
        };

        // Try load state
        if (persistence) |p| {
            if (p.load()) |state| {
                self.last_lsn = state.last_lsn;
            } else |_| {}
        }

        if (config.ssl) {
            try self.sslHandshake();
        }

        try self.handshake(config);
        try self.startReplication(config);
        return self;
    }

    pub fn deinit(self: *PostgresSource) void {
        const terminate_msg = [_]u8{ 'X', 0, 0, 0, 4 };
        _ = self.stream.write(&terminate_msg) catch {};
        self.stream.close();

        var it = self.relations.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.value_ptr.name);
            self.allocator.free(entry.value_ptr.schema);
            // TODO: Free columns
        }
        self.relations.deinit();

        self.allocator.destroy(self);
    }

    fn sslHandshake(self: *PostgresSource) !void {
        // Send SSLRequest
        var buf: [8]u8 = undefined;
        std.mem.writeInt(i32, buf[0..4], 8, .big);
        std.mem.writeInt(i32, buf[4..8], 80877103, .big); // SSLRequest code
        
        try self.stream.writeAll(&buf);

        var resp: [1]u8 = undefined;
        try self.stream.reader().readNoEof(&resp);

        if (resp[0] == 'S') {
            std.log.info("Postgres SSL: Handshake accepted. TLS upgrade would happen here.", .{});
            // In a full implementation, we would wrap self.stream with a TLS client here.
            // For now, we acknowledge the protocol handshake.
        } else if (resp[0] == 'N') {
            std.log.warn("Postgres SSL: SSL requested but server refused.", .{});
        } else {
            return error.UnexpectedSslResponse;
        }
    }

    fn handshake(self: *PostgresSource, config: Config) !void {
        // 1. Send StartupMessage
        var buf: [1024]u8 = undefined;
        var fbs = std.io.fixedBufferStream(&buf);
        const writer = fbs.writer();

        // Length placeholder
        try writer.writeInt(i32, 0, .big);
        try writer.writeInt(i32, proto.VERSION_3_0.pack(), .big);

        try writer.writeAll("user\x00");
        try writer.writeAll(config.user);
        try writer.writeByte(0);

        try writer.writeAll("database\x00");
        try writer.writeAll(config.database);
        try writer.writeByte(0);

        try writer.writeAll("replication\x00database\x00");
        try writer.writeByte(0);

        const end_pos = try fbs.getPos();
        std.mem.writeInt(i32, buf[0..4], @intCast(end_pos), .big);

        try self.stream.writeAll(buf[0..end_pos]);

        // 2. Process responses until ReadyForQuery
        while (!self.ready) {
            try self.processNextMessage();
        }
    }

    fn processNextMessage(self: *PostgresSource) !void {
        var r = self.stream.reader(&self.read_buf);
        const reader = (&r).interface();
        var header: [5]u8 = undefined;
        try reader.readSliceAll(&header);

        const msg_type: proto.MsgType = @enumFromInt(header[0]);
        const msg_len = std.mem.readInt(i32, header[1..5], .big);
        const payload_len: usize = @intCast(msg_len - 4);

        switch (msg_type) {
            .authentication_request => {
                var auth_type_buf: [4]u8 = undefined;
                try reader.readSliceAll(&auth_type_buf);
                const auth_type: proto.AuthType = @enumFromInt(std.mem.readInt(i32, &auth_type_buf, .big));
                
                switch (auth_type) {
                    .ok => self.authenticated = true,
                    else => return error.UnsupportedAuthMethod,
                }
            },
            .ready_for_query => {
                var status: [1]u8 = undefined;
                try reader.readSliceAll(&status);
                self.ready = true;
            },
            .error_response => {
                // For now, just skip the error details and fail
                try reader.discardAll(payload_len);
                return error.PostgresError;
            },
            else => {
                // Skip other messages like ParameterStatus, BackendKeyData
                try reader.discardAll(payload_len);
            },
        }
    }

    fn startReplication(self: *PostgresSource, config: Config) !void {
        var query_buf: [256]u8 = undefined;
        const query = try std.fmt.bufPrint(&query_buf, "START_REPLICATION SLOT {s} LOGICAL {X}/{X} (\"proto_version\" '1', \"publication_names\" 'zdze_pub')", 
            .{config.slot_name, @as(u32, @intCast(self.last_lsn >> 32)), @as(u32, @intCast(self.last_lsn & 0xFFFFFFFF)) });
        
        var header: [5]u8 = undefined;
        header[0] = @intFromEnum(proto.MsgType.query);
        std.mem.writeInt(i32, header[1..5], @intCast(query.len + 4), .big);
        
        try self.stream.writeAll(&header);
        try self.stream.writeAll(query);

        // 2. Wait for CopyBothResponse
        var resp_header: [5]u8 = undefined;
        var r_init = self.stream.reader(&self.read_buf);
        try (&r_init).interface().readSliceAll(&resp_header);
        if (resp_header[0] != 'W') {
            return error.ExpectedCopyBoth;
        }
        const resp_len = std.mem.readInt(i32, resp_header[1..5], .big);
        var r_skip = self.stream.reader(&self.read_buf);
        try (&r_skip).interface().discardAll(@intCast(resp_len - 4));
    }

    fn readInt(self: *PostgresSource, comptime T: type) !T {
        var buf: [@sizeOf(T)]u8 = undefined;
        var r = self.stream.reader(&self.read_buf);
        try (&r).interface().readSliceAll(&buf);
        return std.mem.readInt(T, &buf, .big);
    }

    fn readTuples(self: *PostgresSource, rel_info: RelationInfo) ![]types.Column {
        var r = self.stream.reader(&self.read_buf);
        const reader = (&r).interface();
        const num_cols = try self.readInt(u16);
        const rows = try self.allocator.alloc(types.Column, num_cols);

        for (0..num_cols) |i| {
            var col_type_buf: [1]u8 = undefined;
            try reader.readSliceAll(&col_type_buf);
            const col_type = col_type_buf[0];
            const col_name = try self.allocator.dupe(u8, rel_info.columns[i].name);

            if (col_type == 'n') {
                rows[i] = .{ .name = col_name, .value = .null };
            } else if (col_type == 't') {
                const col_len = try self.readInt(i32);
                const val_buf = try self.allocator.alloc(u8, @intCast(col_len));
                try reader.readSliceAll(val_buf);
                rows[i] = .{ .name = col_name, .value = .{ .string = val_buf } };
            } else {
                // Toast or other skipped for now
                try reader.discardAll(try self.readInt(u32));
                rows[i] = .{ .name = col_name, .value = .null };
            }
        }
        return rows;
    }

    fn sendStatusUpdate(self: *PostgresSource) !void {
        var buf: [39]u8 = undefined;
        buf[0] = 'd'; // CopyData
        std.mem.writeInt(i32, buf[1..5], 38, .big);
        buf[5] = 'r'; // Standby status update
        
        // Write LSNs (Last received, flushed, applied)
        std.mem.writeInt(u64, buf[6..14], self.last_lsn, .big);
        std.mem.writeInt(u64, buf[14..22], self.last_lsn, .big);
        std.mem.writeInt(u64, buf[22..30], self.last_lsn, .big);
        
        // Timestamp (Postgres epoch: Jan 1 2000)
        const pg_epoch = 946684800000; // Simplified ms
        const now = std.time.milliTimestamp();
        std.mem.writeInt(u64, buf[30..38], @intCast((now - pg_epoch) * 1000), .big);
        
        buf[38] = 0; // Reply requested: no
        
        try self.stream.writeAll(buf[0..39]);
    }

    fn readString(self: *PostgresSource, allocator: std.mem.Allocator) ![]const u8 {
        var list = std.ArrayList(u8).empty;
        errdefer list.deinit(allocator);
        var r = self.stream.reader(&self.read_buf);
        const reader = (&r).interface();
        while (true) {
            var byte_buf: [1]u8 = undefined;
            try reader.readSliceAll(&byte_buf);
            const byte = byte_buf[0];
            if (byte == 0) break;
            try list.append(allocator, byte);
        }
        return list.toOwnedSlice(allocator);
    }

    pub fn next(ptr: *anyopaque) core_err.Error!?types.CdcEvent {
        const self: *PostgresSource = @ptrCast(@alignCast(ptr));
        while (true) {
            return nextInternal(ptr) catch |err| {
                switch (err) {
                    error.OutOfMemory => return core_err.Error.OutOfMemory,
                    else => {
                        // Attempt reconnect
                        std.log.err("Postgres connection lost: {s}. Retrying...", .{@errorName(err)});
                        const wait_time: u64 = @min(@as(u64, 1) << @intCast(@min(self.retry_count, @as(u32, 8))), @as(u64, 300));
                        std.Thread.sleep(wait_time * @as(u64, std.time.ns_per_s));
                        self.retry_count += 1;
                        
                        self.reconnect() catch continue;
                        continue;
                    },
                }
            };
        }
    }

    fn reconnect(self: *PostgresSource) !void {
        self.stream.close();
        self.ready = false;
        self.stream = try net.tcpConnectToHost(self.allocator, self.config.host, self.config.port);
        try self.handshake(self.config);
        try self.startReplication(self.config);
        self.retry_count = 0;
    }

    fn nextInternal(ptr: *anyopaque) !?types.CdcEvent {
        const self: *PostgresSource = @ptrCast(@alignCast(ptr));
        var r_main = self.stream.reader(&self.read_buf);
        const reader = (&r_main).interface();
        
        while (true) {
            var header: [5]u8 = undefined;
            reader.readSliceAll(&header) catch return null; 

            const msg_type: proto.MsgType = @enumFromInt(header[0]);
            const msg_len = std.mem.readInt(i32, header[1..5], .big);
            const payload_len: usize = @intCast(msg_len - 4);

            if (msg_type == .copy_data) {
                var sub_type_buf: [1]u8 = undefined;
                try reader.readSliceAll(&sub_type_buf);
                const sub_type = sub_type_buf[0];
                
                if (sub_type == 'w') { // XLogData
                    const wal_start = try self.readInt(u64);
                    self.last_lsn = wal_start;
                    // Skip rest of WAL header (16 bytes)
                    try reader.discardAll(16);
                    
                    var logical_type_buf: [1]u8 = undefined;
                    try reader.readSliceAll(&logical_type_buf);
                    const logical_type_byte = logical_type_buf[0];
                    const l_msg_type: proto.LogicalMsgType = @enumFromInt(logical_type_byte);

                    switch (l_msg_type) {
                        .begin => {
                            _ = try self.readInt(u64); // Final LSN
                            _ = try self.readInt(u64); // Timestamp
                            _ = try self.readInt(u32); // XID
                        },
                        .commit => {
                            var flags_buf: [1]u8 = undefined;
                            try reader.readSliceAll(&flags_buf); // Flags
                            _ = try self.readInt(u64); // Commit LSN
                            const end_lsn = try self.readInt(u64); // End LSN
                            _ = try self.readInt(u64); // Timestamp
                            
                            // Checkpointing
                            if (self.persistence) |p| {
                                p.save(.{ .last_lsn = end_lsn, .updated_at = std.time.microTimestamp() }) catch {};
                            }
                        },
                        .relation => {
                            const rel_id = try self.readInt(u32);
                            const schema = try self.readString(self.allocator);
                            const name = try self.readString(self.allocator);
                            try reader.discardAll(1); // Replica identity
                            const num_cols = try self.readInt(u16);
                            
                            const columns = try self.allocator.alloc(types.Column, num_cols);
                            for (0..num_cols) |i| {
                                try reader.discardAll(1); // Flags
                                columns[i] = .{
                                    .name = try self.readString(self.allocator),
                                    .value = .null, // Template
                                };
                                try reader.discardAll(8); // Type OID + Modifier
                            }
                            
                            try self.relations.put(rel_id, .{
                                .name = name,
                                .schema = schema,
                                .columns = columns,
                            });
                        },
                        .insert => {
                            const rel_id = try self.readInt(u32);
                            const rel_info = self.relations.get(rel_id) orelse return error.UnknownRelation;
                            
                            try reader.discardAll(1); // Tuple type 'N'
                            const rows = try self.readTuples(rel_info);

                            return types.CdcEvent{
                                .op = .insert,
                                .table = try self.allocator.dupe(u8, rel_info.name),
                                .schema = try self.allocator.dupe(u8, rel_info.schema),
                                .rows = rows,
                                .timestamp = std.time.microTimestamp(),
                            };
                        },
                        .update => {
                            const rel_id = try self.readInt(u32);
                            const rel_info = self.relations.get(rel_id) orelse return error.UnknownRelation;

                            var sub_msg_type_buf: [1]u8 = undefined;
                            try reader.readSliceAll(&sub_msg_type_buf);
                            var sub_msg_type = sub_msg_type_buf[0];
                            if (sub_msg_type == 'K' or sub_msg_type == 'O') {
                                // Skip Key/Old tuples for now
                                const n = try self.readInt(u16);
                                for (0..n) |_| {
                                    var t_buf: [1]u8 = undefined;
                                    try reader.readSliceAll(&t_buf);
                                    if (t_buf[0] == 't') try reader.discardAll(try self.readInt(u32));
                                }
                                try reader.readSliceAll(&sub_msg_type_buf);
                                sub_msg_type = sub_msg_type_buf[0];
                            }

                            if (sub_msg_type != 'N') return error.ExpectedNewTuple;
                            const rows = try self.readTuples(rel_info);

                            return types.CdcEvent{
                                .op = .update,
                                .table = try self.allocator.dupe(u8, rel_info.name),
                                .schema = try self.allocator.dupe(u8, rel_info.schema),
                                .rows = rows,
                                .timestamp = std.time.microTimestamp(),
                            };
                        },
                        .delete => {
                            const rel_id = try self.readInt(u32);
                            const rel_info = self.relations.get(rel_id) orelse return error.UnknownRelation;

                            var sub_msg_type_buf: [1]u8 = undefined;
                            try reader.readSliceAll(&sub_msg_type_buf);
                            const sub_msg_type = sub_msg_type_buf[0];
                            if (sub_msg_type != 'K' and sub_msg_type != 'O') return error.ExpectedOldTuple;
                            const rows = try self.readTuples(rel_info);

                            return types.CdcEvent{
                                .op = .delete,
                                .table = try self.allocator.dupe(u8, rel_info.name),
                                .schema = try self.allocator.dupe(u8, rel_info.schema),
                                .rows = rows,
                                .timestamp = std.time.microTimestamp(),
                            };
                        },
                        else => {
                            // Skip the rest of this logical message
                            const consumed = 24 + 1 + 1; // WAL + msg_type_byte + sub_type
                            if (payload_len > consumed) {
                                try reader.discardAll(payload_len - consumed);
                            }
                        },
                    }
                } else if (sub_type == 'k') { // Keepalive
                    const wal_end = try self.readInt(u64);
                    self.last_lsn = wal_end;
                    try reader.discardAll(8); // Server time
                    var reply_req_buf: [1]u8 = undefined;
                    try reader.readSliceAll(&reply_req_buf);
                    const reply_requested = reply_req_buf[0];
                    
                    if (reply_requested == 1) {
                        try self.sendStatusUpdate();
                    }
                } else if (sub_type == 'c') { // Commit
                    // Not persisting commit LSN to avoid partial application
                    // Just skip
                    try reader.discardAll(payload_len - 1);
                } else {
                    try reader.discardAll(payload_len - 1);
                }
            } else {
                try reader.discardAll(payload_len);
            }
        }
    }

    pub fn vtableDeinit(ptr: *anyopaque) void {
        const self: *PostgresSource = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    pub fn source(self: *PostgresSource) core.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .next = next,
                .deinit = vtableDeinit,
            },
        };
    }
};
