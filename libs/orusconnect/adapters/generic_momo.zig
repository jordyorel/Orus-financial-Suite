const std = @import("std");
const orusshare = @import("orusshare");
const http = @import("../http_server.zig");
const toml = @import("../toml.zig");
const pending = @import("../state/pending_tx.zig");
const broker_mod = @import("../broker_client.zig");
const api_client = @import("../outbound/api_client.zig");

// Generic MoMo adapter — driven entirely by AdapterSchema loaded from TOML.
// Supports any operator with:
//   - OAuth2 Bearer inbound auth
//   - Bearer or custom-header callback auth
//   - Standard JSON body (msisdn / amount / currency / optional ref)
//   - Async payment: POST → 202, webhook confirms
//
// Adding a new operator = drop a TOML file + set env vars. Zero code change.

pub const GenericMoMoAdapter = struct {
    schema:        toml.AdapterSchema,
    bearer_token:  []const u8,
    callback_token: []const u8,
    pending_tx:    *pending.PendingTxStore,
    broker:        *const broker_mod.BrokerClient,
    api:           api_client.ApiClient,
    pan_hash_seed: u64,

    pub fn init(
        schema:        toml.AdapterSchema,
        bearer_token:  []const u8,
        callback_token: []const u8,
        pending_tx:    *pending.PendingTxStore,
        broker:        *const broker_mod.BrokerClient,
        api:           api_client.ApiClient,
        pan_hash_seed: u64,
    ) GenericMoMoAdapter {
        return .{
            .schema         = schema,
            .bearer_token   = bearer_token,
            .callback_token = callback_token,
            .pending_tx     = pending_tx,
            .broker         = broker,
            .api            = api,
            .pan_hash_seed  = pan_hash_seed,
        };
    }

    // Inbound payment request handler.
    // Validates Bearer token, parses JSON using schema.request field names,
    // stores serialized InternalMessage in pending_tx, returns 202.
    pub fn handlePayment(
        self:  *const GenericMoMoAdapter,
        req:   *http.HttpRequest,
        alloc: std.mem.Allocator,
    ) http.HttpResponse {
        if (!self.validateBearer(req)) return http.HttpResponse.unauthorized();

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, req.body, .{}) catch
            return http.HttpResponse.badRequest("{\"error\":\"Malformed JSON\"}");
        defer parsed.deinit();
        const obj = parsed.value.object;

        const cfg = self.schema.request;
        const msisdn = strField(obj, cfg.msisdn_field) orelse
            return http.HttpResponse.badRequest("{\"error\":\"Missing msisdn\"}");
        const amount = intField(obj, cfg.amount_field) orelse
            return http.HttpResponse.badRequest("{\"error\":\"Missing amount\"}");
        if (amount <= 0)
            return http.HttpResponse.badRequest("{\"error\":\"Invalid amount\"}");
        const currency = strField(obj, cfg.currency_field) orelse
            return http.HttpResponse.badRequest("{\"error\":\"Missing currency\"}");
        if (currency.len != 3)
            return http.HttpResponse.badRequest("{\"error\":\"Invalid currency\"}");
        const ext_ref = if (cfg.ref_field.len > 0) strField(obj, cfg.ref_field) else null;

        const msg = self.buildMessage(msisdn, amount, currency, ext_ref);

        var payload_list: std.ArrayList(u8) = .empty;
        payload_list.ensureTotalCapacity(alloc, 512) catch
            return http.HttpResponse.serviceUnavailable();
        var pw = std.Io.Writer.fromArrayList(&payload_list);
        orusshare.serialize.serialize(&msg, &pw) catch {
            var leftover = std.Io.Writer.toArrayList(&pw);
            leftover.deinit(alloc);
            return http.HttpResponse.serviceUnavailable();
        };
        var payload_al = std.Io.Writer.toArrayList(&pw);
        defer payload_al.deinit(alloc);

        var tx_hex = std.fmt.bytesToHex(&msg.msg_id, .lower);
        const tx_id: []const u8 = &tx_hex;
        const now_ns = msg.received_at;

        self.pending_tx.put(.{
            .msg_id       = msg.msg_id,
            .reference_id = tx_id,
            .state        = .PENDING,
            .created_at   = now_ns,
            .expires_at   = now_ns + 5 * 60 * std.time.ns_per_s,
            .msg_bytes    = payload_al.items,
        }) catch return http.HttpResponse.serviceUnavailable();

        const body = std.fmt.allocPrint(
            alloc,
            "{{\"{s}\":\"{s}\",\"status\":\"PENDING\"}}",
            .{ self.schema.callback.tx_id_response, tx_id },
        ) catch return http.HttpResponse.serviceUnavailable();
        return http.HttpResponse.accepted(body);
    }

    // Callback webhook handler.
    // Validates auth (Bearer or custom header), checks status,
    // on success: recovers stored message and publishes to OrusBroker.
    pub fn handleCallback(
        self:  *const GenericMoMoAdapter,
        req:   *http.HttpRequest,
        alloc: std.mem.Allocator,
    ) http.HttpResponse {
        if (!self.validateCallback(req)) return http.HttpResponse.unauthorized();

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, req.body, .{}) catch
            return http.HttpResponse.badRequest("{\"error\":\"Malformed JSON\"}");
        defer parsed.deinit();
        const obj = parsed.value.object;

        const cb = self.schema.callback;
        const tx_id = strField(obj, cb.ref_field) orelse
            return http.HttpResponse.badRequest("{\"error\":\"Missing transaction id\"}");
        const status = strField(obj, cb.status_field) orelse
            return http.HttpResponse.badRequest("{\"error\":\"Missing status\"}");

        if (std.mem.eql(u8, status, cb.success_value)) {
            const msg_bytes = self.pending_tx.takeBytes(tx_id, alloc) orelse
                return http.HttpResponse.notFound();
            defer alloc.free(msg_bytes);

            var fixed_r = std.Io.Reader.fixed(msg_bytes);
            const msg = orusshare.serialize.deserialize(&fixed_r, alloc) catch
                return http.HttpResponse.serviceUnavailable();
            defer orusshare.serialize.free(&msg, alloc);

            self.broker.publish(&msg) catch return http.HttpResponse.serviceUnavailable();
        } else {
            _ = self.pending_tx.remove(tx_id);
        }

        return http.HttpResponse.ok("{}");
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn validateBearer(self: *const GenericMoMoAdapter, req: *http.HttpRequest) bool {
        const hdr = req.header("authorization") orelse return false;
        const prefix = "Bearer ";
        if (!std.mem.startsWith(u8, hdr, prefix)) return false;
        const token = hdr[prefix.len..];
        if (token.len != self.bearer_token.len) return false;
        var diff: u8 = 0;
        for (token, self.bearer_token) |a, b| diff |= a ^ b;
        return diff == 0;
    }

    fn validateCallback(self: *const GenericMoMoAdapter, req: *http.HttpRequest) bool {
        const cb_hdr = self.schema.callback.auth_header;
        if (cb_hdr.len == 0 or std.mem.eql(u8, cb_hdr, "authorization")) {
            // Use same Bearer token validation
            return self.validateCallbackBearer(req);
        }
        // Custom header — constant-time comparison
        const hdr = req.header(cb_hdr) orelse return false;
        if (hdr.len != self.callback_token.len) return false;
        var diff: u8 = 0;
        for (hdr, self.callback_token) |a, b| diff |= a ^ b;
        return diff == 0;
    }

    fn validateCallbackBearer(self: *const GenericMoMoAdapter, req: *http.HttpRequest) bool {
        const hdr = req.header("authorization") orelse return false;
        const prefix = "Bearer ";
        if (!std.mem.startsWith(u8, hdr, prefix)) return false;
        const token = hdr[prefix.len..];
        if (token.len != self.callback_token.len) return false;
        var diff: u8 = 0;
        for (token, self.callback_token) |a, b| diff |= a ^ b;
        return diff == 0;
    }

    fn buildMessage(
        self:     *const GenericMoMoAdapter,
        msisdn:   []const u8,
        amount:   i64,
        currency: []const u8,
        ext_ref:  ?[]const u8,
    ) orusshare.InternalMessage {
        var msg_id: [16]u8 = undefined;
        std.c.arc4random_buf(&msg_id, msg_id.len);
        const now_ns: i64 = @intCast(std.Io.Clock.real.now(self.broker.io).nanoseconds);
        return .{
            .msg_id      = msg_id,
            .schema_id   = self.schema.id,
            .topic       = "transactions.inbound",
            .origin      = .rest_json,
            .provider    = .custom,
            .mti         = self.schema.mti_debit,
            .fields      = &.{},
            .stan        = randomStan(),
            .pan_hash    = orusshare.hash.hashPan(msisdn, self.pan_hash_seed),
            .amount      = amount,
            .currency    = currency[0..3].*,
            .received_at = now_ns,
            .source_ip   = [_]u8{0} ** 16,
            .hop_count   = 1,
            .external_id = ext_ref,
        };
    }
};

// Direction 2 — Banque → MoMo dispatch.
pub const DispatchError = error{ MissingMsisdn, ApiCallFailed, OutOfMemory };

pub fn dispatch(
    adapter: *const GenericMoMoAdapter,
    msg:     *const orusshare.InternalMessage,
    alloc:   std.mem.Allocator,
) DispatchError!orusshare.InternalMessage {
    const msisdn = findField(msg.fields, 2) orelse return error.MissingMsisdn;

    const out = adapter.schema.outbound;
    var body_buf: [512]u8 = undefined;
    const ref: []const u8 = if (msg.external_id) |e| e else &msg.stan;
    const body = std.fmt.bufPrint(&body_buf,
        "{{\"{s}\":\"{s}\",\"{s}\":{d},\"{s}\":\"{s}\",\"{s}\":\"{s}\"}}",
        .{
            out.msisdn_field,   msisdn,
            out.amount_field,   msg.amount,
            out.currency_field, msg.currency,
            if (out.ref_field.len > 0) out.ref_field else "reference", ref,
        },
    ) catch return error.OutOfMemory;

    var bearer_buf: [280]u8 = undefined;
    const bearer = std.fmt.bufPrint(&bearer_buf, "Bearer {s}", .{adapter.bearer_token}) catch
        return error.OutOfMemory;
    const hdrs = [_]api_client.Header{
        .{ .name = "Content-Type", .value = "application/json" },
        .{ .name = "Authorization", .value = bearer },
    };

    var api = adapter.api;
    var resp = api.post(adapter.schema.outbound.ref_field, &hdrs, body, alloc) catch
        return error.ApiCallFailed;
    defer resp.deinit();

    const rc = responseCode(resp.status, resp.body, adapter.schema.callback.success_value);

    var fields: std.ArrayList(orusshare.InternalMessage.FieldEntry) = .empty;
    errdefer {
        for (fields.items) |f| alloc.free(f.value);
        fields.deinit(alloc);
    }
    const rc_val = alloc.dupe(u8, rc) catch return error.OutOfMemory;
    fields.append(alloc, .{ .id = 39, .value = rc_val }) catch return error.OutOfMemory;

    var result = msg.*;
    result.mti        = adapter.schema.mti_credit;
    result.origin     = .rest_json;
    result.hop_count  +|= 1;
    result.fields     = fields.toOwnedSlice(alloc) catch return error.OutOfMemory;
    return result;
}

fn responseCode(status: u16, body: []const u8, success_value: []const u8) []const u8 {
    if (status == 200 or status == 202) {
        if (std.mem.indexOf(u8, body, success_value) != null) return "00";
        if (std.mem.indexOf(u8, body, "FAILED") != null or
            std.mem.indexOf(u8, body, "FAIL")   != null) return "96";
        return "79";
    }
    if (status == 409) return "94";
    if (status == 404) return "15";
    return "96";
}

fn findField(fields: []const orusshare.InternalMessage.FieldEntry, id: u8) ?[]const u8 {
    for (fields) |f| if (f.id == id) return f.value;
    return null;
}

fn randomStan() [6]u8 {
    var raw: [3]u8 = undefined;
    std.c.arc4random_buf(&raw, raw.len);
    const n = (@as(u32, raw[0]) << 16 | @as(u32, raw[1]) << 8 | raw[2]) % 1_000_000;
    return .{
        '0' + @as(u8, @intCast(n / 100_000)),
        '0' + @as(u8, @intCast((n / 10_000) % 10)),
        '0' + @as(u8, @intCast((n / 1_000) % 10)),
        '0' + @as(u8, @intCast((n / 100) % 10)),
        '0' + @as(u8, @intCast((n / 10) % 10)),
        '0' + @as(u8, @intCast(n % 10)),
    };
}

fn strField(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    return switch (obj.get(key) orelse return null) {
        .string => |s| s,
        else    => null,
    };
}

fn intField(obj: std.json.ObjectMap, key: []const u8) ?i64 {
    return switch (obj.get(key) orelse return null) {
        .integer => |i| i,
        .float   => |f| @intFromFloat(f),
        else     => null,
    };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "GenericMoMoAdapter: unauthorized without token" {
    const alloc = std.testing.allocator;

    var store = pending.PendingTxStore.init(alloc);
    defer store.deinit();

    var schema = toml.AdapterSchema{
        .id = "test_op", .version = "1.0", .auth_type = "",
        .mti_debit = "0200".*, .mti_credit = "0200".*,
        .endpoints = .{}, .field_mappings = &.{},
        .routing = .{}, .http = .{}, .env = .{},
        .callback = .{ .auth_header = "", .ref_field = "txId", .status_field = "status", .success_value = "SUCCESS", .tx_id_response = "txId" },
        .request = .{ .msisdn_field = "msisdn", .amount_field = "amount", .currency_field = "currency", .ref_field = "" },
        .outbound = .{ .msisdn_field = "msisdn", .amount_field = "amount", .currency_field = "currency", .ref_field = "" },
        .alloc = alloc,
    };
    defer schema.deinit();

    const dummy_broker = broker_mod.BrokerClient.init(.{ .host = "127.0.0.1", .port = 1 }, undefined);
    const dummy_api = api_client.ApiClient.init("127.0.0.1", 1, undefined);
    const adapter = GenericMoMoAdapter.init(schema, "secret", "secret", &store, &dummy_broker, dummy_api, 0);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var req = http.HttpRequest{
        .arena = arena, .method = .POST,
        .path = "/test/payment", .headers = .{}, .body = "{}",
    };

    const resp = adapter.handlePayment(&req, alloc);
    try std.testing.expectEqual(@as(u16, 401), resp.status);
}

test "GenericMoMoAdapter: callback FAILED removes entry" {
    const alloc = std.testing.allocator;

    var store = pending.PendingTxStore.init(alloc);
    defer store.deinit();
    try store.put(.{
        .msg_id = [_]u8{0xAA} ** 16, .reference_id = "tx_abc",
        .state = .PENDING, .created_at = 0, .expires_at = 9_999_999_999, .msg_bytes = &.{},
    });

    var schema = toml.AdapterSchema{
        .id = "test_op", .version = "1.0", .auth_type = "",
        .mti_debit = "0200".*, .mti_credit = "0200".*,
        .endpoints = .{}, .field_mappings = &.{},
        .routing = .{}, .http = .{}, .env = .{},
        .callback = .{ .auth_header = "x-token", .ref_field = "txId", .status_field = "status", .success_value = "SUCCESS", .tx_id_response = "txId" },
        .request = .{ .msisdn_field = "msisdn", .amount_field = "amount", .currency_field = "currency", .ref_field = "" },
        .outbound = .{ .msisdn_field = "msisdn", .amount_field = "amount", .currency_field = "currency", .ref_field = "" },
        .alloc = alloc,
    };
    defer schema.deinit();

    const dummy_broker = broker_mod.BrokerClient.init(.{ .host = "127.0.0.1", .port = 1 }, undefined);
    const dummy_api = api_client.ApiClient.init("127.0.0.1", 1, undefined);
    const adapter = GenericMoMoAdapter.init(schema, "tok", "cbtok", &store, &dummy_broker, dummy_api, 0);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var headers = std.StringHashMapUnmanaged([]const u8){};
    try headers.put(arena.allocator(), "x-token", "cbtok");

    var req = http.HttpRequest{
        .arena = arena, .method = .POST, .path = "/webhooks/test",
        .headers = headers, .body = "{\"txId\":\"tx_abc\",\"status\":\"FAILED\"}",
    };

    const resp = adapter.handleCallback(&req, alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(store.get("tx_abc") == null);
}
