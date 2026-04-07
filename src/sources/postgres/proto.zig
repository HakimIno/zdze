const std = @import("std");

pub const ProtocolVersion = struct {
    major: u16,
    minor: u16,

    pub fn pack(self: ProtocolVersion) i32 {
        return (@as(i32, self.major) << 16) | @as(i32, self.minor);
    }
};

pub const VERSION_3_0 = ProtocolVersion{ .major = 3, .minor = 0 };

pub const MsgType = enum(u8) {
    authentication_request = 'R',
    backend_key_data = 'K',
    bind_complete = '2',
    close_complete = '3',
    command_complete = 'C',
    copy_data = 'd',
    copy_done = 'c',
    copy_in_response = 'G',
    copy_out_response = 'H',
    copy_both_response = 'W',
    data_row = 'D',
    empty_query_response = 'I',
    error_response = 'E',
    function_call_response = 'V',
    no_data = 'n',
    notice_response = 'N',
    notification_response = 'A',
    parameter_description = 't',
    parameter_status = 'S',
    parse_complete = '1',
    portal_suspended = 's',
    query = 'Q',
    ready_for_query = 'Z',
    row_description = 'T',
    terminate = 'X',
    password_message = 'p',
    _,
};

pub const AuthType = enum(i32) {
    ok = 0,
    kerberos_v5 = 2,
    cleartext_password = 3,
    md5_password = 5,
    scm_credential = 6,
    gss = 7,
    sspi = 9,
    gss_continue = 8,
    sasl = 10,
    sasl_continue = 11,
    sasl_final = 12,
};

pub const ErrorField = enum(u8) {
    severity = 'S',
    severity_non_localized = 'V',
    code = 'C',
    message = 'M',
    detail = 'D',
    hint = 'H',
    position = 'P',
    internal_position = 'p',
    internal_query = 'q',
    where = 'W',
    schema_name = 's',
    table_name = 't',
    column_name = 'c',
    data_type_name = 'd',
    constraint_name = 'n',
    file = 'F',
    line = 'L',
    routine = 'R',
    _,
};

pub const LogicalMsgType = enum(u8) {
    begin = 'B',
    commit = 'C',
    origin = 'O',
    relation = 'R',
    insert = 'I',
    update = 'U',
    delete = 'D',
    truncate = 'T',
    type = 'Y',
    message = 'M',
    _,
};

pub const TupleType = enum(u8) {
    new = 'N',
    key = 'K',
    old = 'O',
    _,
};
