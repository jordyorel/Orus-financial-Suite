const std = @import("std");

// TOML loader for MoMo adapter schema files.
// Handles: [section], [[array_section]], key = "value", key = ["a","b"]

pub const FieldMapping = struct {
    json_path: []const u8,
    im_field:  []const u8,
    transform: ?[]const u8,
};

pub const Endpoints = struct {
    transfer: []const u8 = "",
    callback: []const u8 = "",
    status:   []const u8 = "",
};

pub const RoutingConfig = struct {
    msisdn_prefixes: []const []const u8 = &.{},
};

pub const HttpConfig = struct {
    payment_path:  []const u8 = "",
    callback_path: []const u8 = "",
};

// Names of env vars holding runtime secrets — never actual values.
pub const EnvConfig = struct {
    bearer_token:   []const u8 = "",
    callback_token: []const u8 = "",
    api_host:       []const u8 = "",
    api_port:       []const u8 = "",
};

pub const CallbackConfig = struct {
    auth_header:    []const u8 = "",
    ref_field:      []const u8 = "referenceId",
    status_field:   []const u8 = "status",
    success_value:  []const u8 = "SUCCESSFUL",
    tx_id_response: []const u8 = "referenceId",
};

pub const RequestConfig = struct {
    msisdn_field:   []const u8 = "msisdn",
    amount_field:   []const u8 = "amount",
    currency_field: []const u8 = "currency",
    ref_field:      []const u8 = "",
};

pub const AdapterSchema = struct {
    id:            []const u8,
    version:       []const u8,
    auth_type:     []const u8,
    mti_debit:     [4]u8,
    mti_credit:    [4]u8,
    endpoints:     Endpoints,
    field_mappings: []FieldMapping,
    routing:       RoutingConfig,
    http:          HttpConfig,
    env:           EnvConfig,
    callback:      CallbackConfig,
    request:       RequestConfig,
    outbound:      RequestConfig,
    alloc:         std.mem.Allocator,

    pub fn deinit(self: *AdapterSchema) void {
        self.alloc.free(self.field_mappings);
        self.alloc.free(self.routing.msisdn_prefixes);
    }
};

pub const ParseError = error{ InvalidFormat, MissingField, OutOfMemory };

pub fn parseAdapterSchema(src: []const u8, alloc: std.mem.Allocator) ParseError!AdapterSchema {
    var schema = AdapterSchema{
        .id            = "",
        .version       = "",
        .auth_type     = "",
        .mti_debit     = "0200".*,
        .mti_credit    = "0200".*,
        .endpoints     = .{},
        .field_mappings = &.{},
        .routing       = .{},
        .http          = .{},
        .env           = .{},
        .callback      = .{},
        .request       = .{},
        .outbound      = .{},
        .alloc         = alloc,
    };

    var mappings:  std.ArrayList(FieldMapping)    = .empty;
    var prefixes:  std.ArrayList([]const u8)      = .empty;
    errdefer mappings.deinit(alloc);
    errdefer prefixes.deinit(alloc);

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
            current_mapping = .{ .json_path = "", .im_field = "", .transform = null };
            in_mapping_array = true;
            section = "field_mappings";
            continue;
        }

        // [section]
        if (std.mem.startsWith(u8, line, "[") and std.mem.endsWith(u8, line, "]") and
            !std.mem.startsWith(u8, line, "[["))
        {
            if (current_mapping) |m| { try mappings.append(alloc, m); current_mapping = null; }
            section = line[1 .. line.len - 1];
            in_mapping_array = false;
            continue;
        }

        const eq_idx = std.mem.indexOfScalar(u8, line, '=') orelse continue;
        const key     = std.mem.trim(u8, line[0..eq_idx], " \t");
        const raw_val = std.mem.trim(u8, line[eq_idx + 1 ..], " \t");

        // Array value: key = ["a", "b"]
        if (std.mem.startsWith(u8, raw_val, "[")) {
            if (std.mem.eql(u8, section, "routing") and std.mem.eql(u8, key, "msisdn_prefixes")) {
                try parseStringArray(raw_val, alloc, &prefixes);
            }
            continue;
        }

        const value = unquote(raw_val);

        if (in_mapping_array) {
            if (current_mapping) |*m| {
                if (std.mem.eql(u8, key, "json_path")) m.json_path = value;
                if (std.mem.eql(u8, key, "im_field"))  m.im_field  = value;
                if (std.mem.eql(u8, key, "transform"))  m.transform = value;
            }
        } else if (std.mem.eql(u8, section, "meta")) {
            if (std.mem.eql(u8, key, "id"))         schema.id         = value;
            if (std.mem.eql(u8, key, "version"))    schema.version    = value;
            if (std.mem.eql(u8, key, "auth_type"))  schema.auth_type  = value;
            if (std.mem.eql(u8, key, "mti_debit")  and value.len >= 4) @memcpy(&schema.mti_debit,  value[0..4]);
            if (std.mem.eql(u8, key, "mti_credit") and value.len >= 4) @memcpy(&schema.mti_credit, value[0..4]);
        } else if (std.mem.eql(u8, section, "endpoints")) {
            if (std.mem.eql(u8, key, "transfer")) schema.endpoints.transfer = value;
            if (std.mem.eql(u8, key, "callback")) schema.endpoints.callback = value;
            if (std.mem.eql(u8, key, "status"))   schema.endpoints.status   = value;
        } else if (std.mem.eql(u8, section, "http")) {
            if (std.mem.eql(u8, key, "payment_path"))  schema.http.payment_path  = value;
            if (std.mem.eql(u8, key, "callback_path")) schema.http.callback_path = value;
        } else if (std.mem.eql(u8, section, "env")) {
            if (std.mem.eql(u8, key, "bearer_token"))   schema.env.bearer_token   = value;
            if (std.mem.eql(u8, key, "callback_token")) schema.env.callback_token = value;
            if (std.mem.eql(u8, key, "api_host"))       schema.env.api_host       = value;
            if (std.mem.eql(u8, key, "api_port"))       schema.env.api_port       = value;
        } else if (std.mem.eql(u8, section, "callback")) {
            if (std.mem.eql(u8, key, "auth_header"))    schema.callback.auth_header    = value;
            if (std.mem.eql(u8, key, "ref_field"))      schema.callback.ref_field      = value;
            if (std.mem.eql(u8, key, "status_field"))   schema.callback.status_field   = value;
            if (std.mem.eql(u8, key, "success_value"))  schema.callback.success_value  = value;
            if (std.mem.eql(u8, key, "tx_id_response")) schema.callback.tx_id_response = value;
        } else if (std.mem.eql(u8, section, "request")) {
            if (std.mem.eql(u8, key, "msisdn_field"))   schema.request.msisdn_field   = value;
            if (std.mem.eql(u8, key, "amount_field"))   schema.request.amount_field   = value;
            if (std.mem.eql(u8, key, "currency_field")) schema.request.currency_field = value;
            if (std.mem.eql(u8, key, "ref_field"))      schema.request.ref_field      = value;
        } else if (std.mem.eql(u8, section, "outbound")) {
            if (std.mem.eql(u8, key, "msisdn_field"))   schema.outbound.msisdn_field   = value;
            if (std.mem.eql(u8, key, "amount_field"))   schema.outbound.amount_field   = value;
            if (std.mem.eql(u8, key, "currency_field")) schema.outbound.currency_field = value;
            if (std.mem.eql(u8, key, "ref_field"))      schema.outbound.ref_field      = value;
        }
    }

    if (current_mapping) |m| try mappings.append(alloc, m);

    schema.field_mappings     = try mappings.toOwnedSlice(alloc);
    schema.routing.msisdn_prefixes = try prefixes.toOwnedSlice(alloc);

    if (schema.id.len == 0) return error.MissingField;
    return schema;
}

fn parseStringArray(
    s:     []const u8,
    alloc: std.mem.Allocator,
    out:   *std.ArrayList([]const u8),
) ParseError!void {
    const trimmed = std.mem.trim(u8, s, " \t");
    if (trimmed.len < 2 or trimmed[0] != '[' or trimmed[trimmed.len - 1] != ']') return;
    const inner = trimmed[1 .. trimmed.len - 1];
    var iter = std.mem.splitScalar(u8, inner, ',');
    while (iter.next()) |item| {
        const t = std.mem.trim(u8, item, " \t");
        if (t.len == 0) continue;
        try out.append(alloc, unquote(t));
    }
}

fn unquote(s: []const u8) []const u8 {
    if (s.len >= 2 and s[0] == '"' and s[s.len - 1] == '"') return s[1 .. s.len - 1];
    return s;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "parseAdapterSchema: full mtn_momo schema" {
    const alloc = std.testing.allocator;
    const src =
        \\[meta]
        \\id = "mtn_momo"
        \\version = "1.0"
        \\mti_debit = "0200"
        \\mti_credit = "0200"
        \\[routing]
        \\msisdn_prefixes = ["24206"]
        \\[http]
        \\payment_path = "/collection/v1_0/requesttopay"
        \\callback_path = "/webhooks/mtn"
        \\[env]
        \\bearer_token = "MTN_BEARER_TOKEN"
        \\callback_token = "MTN_CALLBACK_TOKEN"
        \\api_host = "MTN_API_HOST"
        \\api_port = "MTN_API_PORT"
        \\[callback]
        \\auth_header = "x-callback-token"
        \\ref_field = "referenceId"
        \\status_field = "status"
        \\success_value = "SUCCESSFUL"
        \\tx_id_response = "referenceId"
        \\[request]
        \\msisdn_field = "msisdn"
        \\amount_field = "amount"
        \\currency_field = "currency"
        \\ref_field = "externalId"
        \\[[field_mappings]]
        \\json_path = "msisdn"
        \\im_field = "pan"
        \\transform = "msisdn_to_pan"
    ;

    var schema = try parseAdapterSchema(src, alloc);
    defer schema.deinit();

    try std.testing.expectEqualStrings("mtn_momo", schema.id);
    try std.testing.expectEqual(@as(usize, 1), schema.routing.msisdn_prefixes.len);
    try std.testing.expectEqualStrings("24206", schema.routing.msisdn_prefixes[0]);
    try std.testing.expectEqualStrings("/collection/v1_0/requesttopay", schema.http.payment_path);
    try std.testing.expectEqualStrings("MTN_BEARER_TOKEN", schema.env.bearer_token);
    try std.testing.expectEqualStrings("x-callback-token", schema.callback.auth_header);
    try std.testing.expectEqualStrings("SUCCESSFUL", schema.callback.success_value);
    try std.testing.expectEqualStrings("externalId", schema.request.ref_field);
    try std.testing.expectEqual(@as(usize, 1), schema.field_mappings.len);
}

test "parseAdapterSchema: multi-prefix routing" {
    const alloc = std.testing.allocator;
    const src =
        \\[meta]
        \\id = "orange_money"
        \\version = "1.0"
        \\[routing]
        \\msisdn_prefixes = ["24269", "24268", "24267"]
        \\[http]
        \\payment_path = "/v1/payment"
        \\callback_path = "/webhooks/orange"
        \\[env]
        \\bearer_token = "ORANGE_BEARER_TOKEN"
        \\callback_token = "ORANGE_BEARER_TOKEN"
        \\api_host = "ORANGE_API_HOST"
        \\api_port = "ORANGE_API_PORT"
        \\[callback]
        \\ref_field = "transactionId"
        \\status_field = "status"
        \\success_value = "SUCCESS"
        \\tx_id_response = "transactionId"
        \\[request]
        \\msisdn_field = "phone"
        \\amount_field = "amount"
        \\currency_field = "currency"
    ;

    var schema = try parseAdapterSchema(src, alloc);
    defer schema.deinit();

    try std.testing.expectEqualStrings("orange_money", schema.id);
    try std.testing.expectEqual(@as(usize, 3), schema.routing.msisdn_prefixes.len);
    try std.testing.expectEqualStrings("24267", schema.routing.msisdn_prefixes[2]);
    try std.testing.expectEqualStrings("phone", schema.request.msisdn_field);
}

test "parseAdapterSchema: backward compat — orange_money minimal" {
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
    try std.testing.expectEqual(@as(usize, 0), schema.routing.msisdn_prefixes.len);
}
