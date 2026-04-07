const std = @import("std");
const root = @import("root.zig");
const http = @import("core/http_server.zig");
const dlq = @import("pipeline/dlq.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len > 1 and std.mem.eql(u8, args[1], "init")) {
        try handleInit();
        return;
    }

    std.log.info("Starting zdze CDC Engine...", .{});

    // 1. Load Configuration
    const config_path = "config.json";
    const cfg = root.core.config.Config.load(allocator, config_path) catch |err| {
        std.log.err("Failed to load config from {s}: {s}.", .{config_path, @errorName(err)});
        return err; 
    };
    defer cfg.deinit(allocator);

    // 2. Setup Persistence & DLQ
    var persistence = try root.core.persistence.Persistence.init(allocator, cfg.state_path);
    defer persistence.deinit();

    var dlq_ptr = try dlq.Dlq.init(allocator, cfg.dlq_path);
    defer dlq_ptr.deinit();

    // 3. Initialize Source
    var source_ptr: root.core.interface.Source = undefined;
    switch (cfg.source.type) {
        .mock => {
            const mock = try root.sources.mock.MockSource.init(allocator, cfg.source.mock_count);
            source_ptr = mock.source();
        },
        .postgres => {
            if (cfg.source.postgres) |p_cfg| {
                const pg = try root.sources.postgres.source.PostgresSource.init(allocator, p_cfg, persistence);
                source_ptr = pg.source();
            } else {
                return error.MissingPostgresConfig;
            }
        },
    }
    defer source_ptr.deinit();

    // 4. Initialize Sink
    var sink_ptr: root.core.interface.Sink = undefined;
    switch (cfg.sink.type) {
        .stdout => {
            const stdout = try root.sinks.stdout.StdoutSink.init(allocator);
            sink_ptr = stdout.sink();
        },
        .webhook => {
            if (cfg.sink.webhook_url) |url| {
                const webhook = try root.sinks.webhook.WebhookSink.init(allocator, url);
                sink_ptr = webhook.sink();
            } else {
                return error.MissingWebhookConfig;
            }
        },
        .kafka => {
            if (cfg.sink.kafka_brokers) |kb| {
                if (cfg.sink.kafka_topic) |kt| {
                    const kafka = try root.sinks.kafka_sink.KafkaSink.init(allocator, kb, kt);
                    sink_ptr = kafka.sink();
                } else {
                    return error.MissingKafkaTopic;
                }
            } else {
                return error.MissingKafkaBrokers;
            }
        },
    }
    // Note: Sink lifecycle is managed by Engine (via Dispatcher) or manually if engine fails/stops early

    // 5. Setup Engine
    const engine = try root.pipeline.engine.Engine.init(
        allocator,
        source_ptr,
        sink_ptr,
        dlq_ptr,
        persistence,
        cfg,
    );
    defer engine.deinit();

    engine.metrics.start_time = std.time.timestamp();
    
    // 6. Setup HTTP Dashboard
    var server = try http.HttpServer.init(allocator, engine, 8080);
    defer server.deinit();
    try server.start();

    // 7. Signal Handling
    _engine_ptr = engine;
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handleSigInt },
        .mask = 0,
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &act, null);
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);

    std.log.info("Engine components initialized. Starting replication loop...", .{});
    try engine.run();
}

fn handleInit() !void {
    std.log.info("Auto-discovering Postgres configuration...", .{});
    // Simplified stub for auto-discovery
    const default_config = 
        \\{
        \\  "source": {
        \\    "type": "postgres",
        \\    "postgres": {
        \\      "host": "localhost",
        \\      "port": 5432,
        \\      "user": "postgres",
        \\      "database": "zdze_db",
        \\      "ssl": false
        \\    }
        \\  },
        \\  "sink": {
        \\    "type": "stdout"
        \\  }
        \\}
    ;
    const file = try std.fs.cwd().createFile("config.json", .{});
    defer file.close();
    try file.writeAll(default_config);
    std.log.info("Created default config.json. Edit it and run 'zdze' to start.", .{});
}

var _engine_ptr: ?*root.pipeline.engine.Engine = null;

fn handleSigInt(sig: i32) callconv(.c) void {
    _ = sig;
    if (_engine_ptr) |e| {
        std.debug.print("\nSignal received, stopping engine...\n", .{});
        e.stop();
    }
}
