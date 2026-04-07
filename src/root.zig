pub const core = struct {
    pub const types = @import("core/types.zig");
    pub const interface = @import("core/interface.zig");
    pub const err = @import("core/error.zig");
    pub const persistence = @import("core/persistence.zig");
    pub const config = @import("core/config.zig");
};

pub const sources = struct {
    pub const mock = @import("sources/mock.zig");
    pub const postgres = @import("sources/postgres.zig");
};

pub const sinks = struct {
    pub const stdout = @import("sinks/stdout_sink.zig");
    pub const webhook = @import("sinks/webhook_sink.zig");
};

pub const pipeline = struct {
    pub const engine = @import("pipeline/engine.zig");
    pub const filter = @import("pipeline/filter.zig");
};
