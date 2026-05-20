const std = @import("std");

// Minimal TOML loader for adapter schema files.
// Handles: [section], [[array_section]], key = "string_value"

pub const FieldMapping = struct {
    json_path: []const u8,
    im_field: []const u8,
    transform: ?[]const u8,
};

pub const Endpoints = struct {
    transfer: []const u8 = "",
    callback: []const u8 = "",
    status: []const u8 = "",
};

pub const AdapterSchema = struct {
    id: []const u8,
    version: []const u8,
    auth_type: []const u8,
    mti_debit: [4]u8,
    mti_credit: [4]u8,
    endpoints: Endpoints,
    field_mappings: []FieldMapping,
    alloc: std.mem.Allocator,

    pub fn deinit(self: *AdapterSchema) void {
        self.alloc.free(self.field_mappings);
    }
};

pub const ParseError = error{
    InvalidFormat,
    MissingField,
    OutOfMemory,
};

pub fn parseAdapterSchema(src: []const u8, alloc: std.mem.Allocator) ParseError!AdapterSchema {
    var schema = AdapterSchema{
        .id = "",
        .version = "",
        .auth_type = "",
        .mti_debit = "0200".*,
        .mti_credit = "0200".*,
        .endpoints = .{},
        .field_mappings = &.{},
        .alloc = alloc,
    };

    var mappings: std.ArrayList(FieldMapping) = .empty;
    errdefer mappings.deinit(alloc);

    var current_mapping: ?FieldMapping = null;
    var section: []const u8 = "";
    var in_mapping_array = false;

    var lines = std.mem.splitScalar(u8, src, '\n');
    while (lines.next()) |raw_line| {
        const line = std.mem.trim(u8, raw_line, " \t\r");
        if (line.len == 0 or std.mem.startsWith(u8, line, "#")) continue;

        // [[field_mappings]]
        if (std.mem.eql(u8, line, "[[field_mappings]]")) {
            if (current_mapping) |m| try mappings.append(alloc, m);
            current_mapping = FieldMapping{
                .json_path = "",
                .im_field = "",
                .transform = null,
            };
            in_mapping_array = true;
            section = "field_mappings";
            continue;
        }

        // [section]
        if (std.mem.startsWith(u8, line, "[") and std.mem.endsWith(u8, line, "]")) {
            if (current_mapping) |m| {
                try mappings.append(alloc, m);
                current_mapping = null;
            }
            section = line[1 .. line.len - 1];
            in_mapping_array = false;
            continue;
        }

        // key = "value"
        const eq_idx = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        const key = std.mem.trim(u8, line[0..eq_idx], " \t");
        const raw_val = std.mem.trim(u8, line[eq_idx + 1 ..], " \t");
        const value = unquote(raw_val);

        if (in_mapping_array) {
            if (current_mapping) |*m| {
                if (std.mem.eql(u8, key, "json_path")) m.json_path = value;
                if (std.mem.eql(u8, key, "im_field")) m.im_field = value;
                if (std.mem.eql(u8, key, "transform")) m.transform = value;
            }
        } else if (std.mem.eql(u8, section, "meta")) {
            if (std.mem.eql(u8, key, "id")) schema.id = value;
            if (std.mem.eql(u8, key, "version")) schema.version = value;
            if (std.mem.eql(u8, key, "auth_type")) schema.auth_type = value;
            if (std.mem.eql(u8, key, "mti_debit")) {
                if (value.len >= 4) @memcpy(&schema.mti_debit, value[0..4]);
            }
            if (std.mem.eql(u8, key, "mti_credit")) {
                if (value.len >= 4) @memcpy(&schema.mti_credit, value[0..4]);
            }
        } else if (std.mem.eql(u8, section, "endpoints")) {
            if (std.mem.eql(u8, key, "transfer")) schema.endpoints.transfer = value;
            if (std.mem.eql(u8, key, "callback")) schema.endpoints.callback = value;
            if (std.mem.eql(u8, key, "status")) schema.endpoints.status = value;
        }
    }

    if (current_mapping) |m| try mappings.append(alloc, m);

    schema.field_mappings = try mappings.toOwnedSlice(alloc);
    if (schema.id.len == 0) return error.MissingField;
    return schema;
}

fn unquote(s: []const u8) []const u8 {
    if (s.len >= 2 and s[0] == '"' and s[s.len - 1] == '"')
        return s[1 .. s.len - 1];
    return s;
}

test "parseAdapterSchema: orange_money minimal" {
    const alloc = std.testing.allocator;
    const src =
        \\[meta]
        \\id = "orange_money"
        \\version = "2.1"
        \\auth_type = "oauth2_hmac"
        \\mti_debit = "0200"
        \\mti_credit = "0200"
        \\[endpoints]
        \\transfer = "POST /v2/payment"
        \\callback = "POST /webhooks/orange"
        \\[[field_mappings]]
        \\json_path = "msisdn"
        \\im_field = "pan"
        \\transform = "msisdn_to_pan"
        \\[[field_mappings]]
        \\json_path = "amount.value"
        \\im_field = "amount"
        \\transform = "to_centimes"
    ;

    var schema = try parseAdapterSchema(src, alloc);
    defer schema.deinit();

    try std.testing.expectEqualStrings("orange_money", schema.id);
    try std.testing.expectEqualStrings("oauth2_hmac", schema.auth_type);
    try std.testing.expectEqual(@as(usize, 2), schema.field_mappings.len);
    try std.testing.expectEqualStrings("msisdn", schema.field_mappings[0].json_path);
    try std.testing.expectEqualStrings("pan", schema.field_mappings[0].im_field);
}
