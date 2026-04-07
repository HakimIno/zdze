const std = @import("std");

pub const Error = error{
    OutOfMemory,
    NotImplemented,
    ConnectionFailed,
    ProtocolError,
    EndOfStream,
    InvalidData,
    WriteFailed,
    GenericError,
};
