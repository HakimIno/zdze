const std = @import("std");
const root = @import("root.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Starting zdze CDC Engine...", .{});

    // 1. Load Configuration
    const config_path = "config.json";
    const cfg = root.core.config.Config.load(allocator, config_path) catch |err| {
        std.log.err("Failed to load config from {s}: {s}. Using internal defaults...", .{config_path, @errorName(err)});
        // Mock default for demonstration if file missing
        return err; 
    };
    defer cfg.deinit(allocator);

    // 2. Setup Persistence
    var persistence = try root.core.persistence.Persistence.init(allocator, cfg.state_path);
    defer persistence.deinit();

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
    }
    defer sink_ptr.deinit();

    // 5. Setup Engine & Run
    const engine = try root.pipeline.engine.Engine.init(
        allocator,
        source_ptr,
        sink_ptr,
        cfg.filters,
    );
    defer engine.deinit();

    engine.metrics.start_time = std.time.timestamp();
    
    // 6. Signal Handling for Graceful Shutdown
    _engine_ptr = engine;
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handleSigInt },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &act, null) catch {};
    std.posix.sigaction(std.posix.SIG.TERM, &act, null) catch {};

    std.log.info("Engine components initialized. Starting replication loop...", .{});
    try engine.run();
}

var _engine_ptr: ?*root.pipeline.engine.Engine = null;

fn handleSigInt(sig: i32) callconv(.c) void {
    _ = sig;
    if (_engine_ptr) |e| {
        std.debug.print("\nSignal received, stopping engine...\n", .{});
        e.stop();
    }
}
