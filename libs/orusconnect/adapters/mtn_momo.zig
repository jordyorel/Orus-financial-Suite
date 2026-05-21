const std = @import("std");
const orusshare = @import("orusshare");
const oauth2 = @import("../auth/oauth2.zig");
const http = @import("../http_server.zig");
const toml = @import("../toml.zig");
const pending = @import("../state/pending_tx.zig");
const broker_mod = @import("../broker_client.zig");
const api_client = @import("../outbound/api_client.zig");

// MTN MoMo Collection API — async: POST returns 202, webhook confirms.
// Request body: {"msisdn":"237XXXXXXX","amount":5000,"currency":"XAF","externalId":"..."}
// Callback body: {"referenceId":"...","status":"SUCCESSFUL"|"FAILED"}
pub const MtnMomoAdapter = struct {
    schema: toml.AdapterSchema,
    token_validator: oauth2.TokenValidator,
    callback_token: []const u8,
    pending_tx: *pending.PendingTxStore,
    broker: *const broker_mod.BrokerClient,
    pan_hash_seed: u64,

    pub fn init(
        schema: toml.AdapterSchema,
        bearer_token: []const u8,
        callback_token: []const u8,
        pending_tx: *pending.PendingTxStore,
        broker: *const broker_mod.BrokerClient,
        pan_hash_seed: u64,
    ) MtnMomoAdapter {
        return .{
            .schema = schema,
            .token_validator = .{ .token = bearer_token },
            .callback_token = callback_token,
            .pending_tx = pending_tx,
            .broker = broker,
            .pan_hash_seed = pan_hash_seed,
        };
    }

    // POST /collection/v1_0/requesttopay
    // CDC §5.5 : stocker dans pending_tx uniquement — PAS de publish broker.
    // Le broker est alimenté par handleCallback quand MTN confirme le PIN USSD.
    pub fn handleRequestToPay(
        self: *const MtnMomoAdapter,
        req: *http.HttpRequest,
        alloc: std.mem.Allocator,
    ) http.HttpResponse {
        const auth_hdr = req.header("authorization") orelse
            return http.HttpResponse.unauthorized();
        if (!self.token_validator.validate(auth_hdr))
            return http.HttpResponse.unauthorized();

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, req.body, .{}) catch
            return badRequest("C1001", "Malformed JSON");
        defer parsed.deinit();
        const obj = parsed.value.object;

        const msisdn = strField(obj, "msisdn") orelse
            return badRequest("C1002", "Missing msisdn");
        const amount = intField(obj, "amount") orelse
            return badRequest("C1002", "Missing amount");
        if (amount <= 0) return badRequest("C2001", "Invalid amount");
        const currency = strField(obj, "currency") orelse
            return badRequest("C1002", "Missing currency");
        if (currency.len != 3) return badRequest("C2002", "Invalid currency");
        const external_id = strField(obj, "externalId");

        const msg = buildMessage(self, msisdn, amount, currency, external_id);

        // Serialize message — stored until callback confirms.
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

        var ref_hex = std.fmt.bytesToHex(&msg.msg_id, .lower);
        const ref_id: []const u8 = &ref_hex;

        const now_ns = msg.received_at;
        self.pending_tx.put(.{
            .msg_id = msg.msg_id,
            .reference_id = ref_id,
            .state = .PENDING,
            .created_at = now_ns,
            .expires_at = now_ns + 5 * 60 * std.time.ns_per_s,
            .msg_bytes = payload_al.items,
        }) catch return http.HttpResponse.serviceUnavailable();

        const body = std.fmt.allocPrint(alloc, "{{\"referenceId\":\"{s}\"}}", .{ref_id}) catch
            return http.HttpResponse.serviceUnavailable();
        return http.HttpResponse.accepted(body);
    }

    // POST /webhooks/mtn — MTN confirms with X-Callback-Token.
    // CDC §5.5 : c'est ici que l'InternalMessage est publié dans OrusBroker,
    // après confirmation du PIN USSD par l'utilisateur.
    pub fn handleCallback(
        self: *const MtnMomoAdapter,
        req: *http.HttpRequest,
        alloc: std.mem.Allocator,
    ) http.HttpResponse {
        // Constant-time comparison — CDC §10, protection timing attack.
        const cb_token = req.header("x-callback-token") orelse
            return http.HttpResponse.unauthorized();
        if (cb_token.len != self.callback_token.len)
            return http.HttpResponse.unauthorized();
        var diff: u8 = 0;
        for (cb_token, self.callback_token) |a, b| diff |= a ^ b;
        if (diff != 0) return http.HttpResponse.unauthorized();

        var parsed = std.json.parseFromSlice(std.json.Value, alloc, req.body, .{}) catch
            return badRequest("C1001", "Malformed JSON");
        defer parsed.deinit();
        const obj = parsed.value.object;

        const ref_id = strField(obj, "referenceId") orelse
            return badRequest("C1002", "Missing referenceId");
        const status = strField(obj, "status") orelse
            return badRequest("C1002", "Missing status");

        if (std.mem.eql(u8, status, "SUCCESSFUL")) {
            // Atomically remove pending entry and recover the stored message bytes.
            const msg_bytes = self.pending_tx.takeBytes(ref_id, alloc) orelse
                return http.HttpResponse.notFound();
            defer alloc.free(msg_bytes);

            var fixed_r = std.Io.Reader.fixed(msg_bytes);
            const msg = orusshare.serialize.deserialize(&fixed_r, alloc) catch
                return http.HttpResponse.serviceUnavailable();
            defer orusshare.serialize.free(&msg, alloc);

            self.broker.publish(&msg) catch return http.HttpResponse.serviceUnavailable();
        } else {
            // FAILED ou autre — supprimer l'entrée sans publier.
            _ = self.pending_tx.remove(ref_id);
        }

        return http.HttpResponse.ok("{}");
    }

    fn buildMessage(
        self: *const MtnMomoAdapter,
        msisdn: []const u8,
        amount: i64,
        currency: []const u8,
        external_id: ?[]const u8,
    ) orusshare.InternalMessage {
        var msg_id: [16]u8 = undefined;
        std.c.arc4random_buf(&msg_id, msg_id.len);

        const now_ns: i64 = @intCast(std.Io.Clock.real.now(self.broker.io).nanoseconds);

        return .{
            .msg_id = msg_id,
            .schema_id = self.schema.id,
            .topic = "transactions.inbound",
            .origin = .rest_json,
            .provider = .mtn_momo,
            .mti = self.schema.mti_debit,
            .fields = @constCast(MTN_FIELDS[0..]),
            .stan = randomStan(),
            .pan_hash = orusshare.hash.hashPan(msisdn, self.pan_hash_seed),
            .amount = amount,
            .currency = currency[0..3].*,
            .received_at = now_ns,
            .source_ip = [_]u8{0} ** 16,
            .hop_count = 1,
            .external_id = external_id,
        };
    }
};

pub const DispatchError = error{
    MissingMsisdn,
    ApiCallFailed,
    OutOfMemory,
};

// Direction 2 — Banque → MoMo.
// Receives an InternalMessage routed by OrusBroker, calls the MTN MoMo
// disbursement API, and returns the response as an InternalMessage.
// Caller frees result.fields via: for (result.fields) |f| alloc.free(f.value);
//                                  alloc.free(result.fields);
pub fn dispatch(
    self: *const MtnMomoAdapter,
    msg: *const orusshare.InternalMessage,
    client: *api_client.ApiClient,
    alloc: std.mem.Allocator,
) DispatchError!orusshare.InternalMessage {
    // Field 2 carries the destination MSISDN when the bank initiates a transfer.
    const msisdn = findField(msg.fields, 2) orelse return error.MissingMsisdn;

    // Build JSON body for MTN Disbursements API.
    var body_buf: [512]u8 = undefined;
    const body = std.fmt.bufPrint(&body_buf,
        \\{{"msisdn":"{s}","amount":{d},"currency":"{s}","externalId":"{s}"}}
    , .{
        msisdn,
        msg.amount,
        msg.currency,
        if (msg.external_id) |e| e else &msg.stan,
    }) catch return error.OutOfMemory;

    var bearer_buf: [280]u8 = undefined;
    const bearer = std.fmt.bufPrint(&bearer_buf, "Bearer {s}", .{self.token_validator.token}) catch return error.OutOfMemory;
    const hdrs = [_]api_client.Header{
        .{ .name = "Content-Type", .value = "application/json" },
        .{ .name = "Authorization", .value = bearer },
    };

    var resp = client.post(
        self.schema.endpoints.transfer,
        &hdrs,
        body,
        alloc,
    ) catch return error.ApiCallFailed;
    defer resp.deinit();

    const rc = mtnResponseCode(resp.status, resp.body);

    var fields: std.ArrayList(orusshare.InternalMessage.FieldEntry) = .empty;
    errdefer {
        for (fields.items) |f| alloc.free(f.value);
        fields.deinit(alloc);
    }
    const rc_val = alloc.dupe(u8, rc) catch return error.OutOfMemory;
    fields.append(alloc, .{ .id = 39, .value = rc_val }) catch return error.OutOfMemory;

    var result = msg.*;
    result.mti = self.schema.mti_credit;
    result.origin = .rest_json;
    result.hop_count +|= 1;
    result.fields = fields.toOwnedSlice(alloc) catch return error.OutOfMemory;
    return result;
}

// Fixed ISO 8583 fields added by every MTN request.
// field 2 is populated from the MSISDN at the gateway layer via pan_hash routing;
// field 3 = processing code 30 (MoMo purchase/debit).
const MTN_FIELDS = [_]orusshare.InternalMessage.FieldEntry{
    .{ .id = 3, .value = "300000" },
};

fn findField(fields: []const orusshare.InternalMessage.FieldEntry, id: u8) ?[]const u8 {
    for (fields) |f| if (f.id == id) return f.value;
    return null;
}

// Map MTN API HTTP status + body to ISO 8583 field-39 response code.
fn mtnResponseCode(status: u16, body: []const u8) []const u8 {
    if (status == 202 or status == 200) {
        if (std.mem.indexOf(u8, body, "SUCCESSFUL") != null) return "00";
        if (std.mem.indexOf(u8, body, "FAILED") != null) return "96";
        return "79"; // PENDING
    }
    if (status == 409) return "94"; // duplicate
    if (status == 404) return "15"; // account not found
    return "96"; // generic error
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
        else => null,
    };
}

fn intField(obj: std.json.ObjectMap, key: []const u8) ?i64 {
    return switch (obj.get(key) orelse return null) {
        .integer => |i| i,
        .float => |f| @intFromFloat(f),
        else => null,
    };
}

fn badRequest(code: []const u8, msg: []const u8) http.HttpResponse {
    _ = code;
    var buf: [256]u8 = undefined;
    const body = std.fmt.bufPrint(&buf, "{{\"error\":\"{s}\"}}", .{msg}) catch
        "{\"error\":\"Bad request\"}";
    return http.HttpResponse.badRequest(body);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "MtnMomoAdapter: unauthorized without token" {
    const alloc = std.testing.allocator;
    var store = pending.PendingTxStore.init(alloc);
    defer store.deinit();

    var schema = toml.AdapterSchema{
        .id = "mtn_momo",
        .version = "1.0",
        .auth_type = "oauth2",
        .mti_debit = "0200".*,
        .mti_credit = "0200".*,
        .endpoints = .{},
        .field_mappings = &.{},
        .alloc = alloc,
    };
    defer schema.deinit();

    // Use a dummy broker — request will fail at auth before broker is called
    const dummy_broker = broker_mod.BrokerClient.init(.{
        .host = "127.0.0.1",
        .port = 1,
    }, undefined);

    const adapter = MtnMomoAdapter.init(schema, "secret", "cb_token", &store, &dummy_broker, 0);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var req = http.HttpRequest{
        .arena = arena,
        .method = .POST,
        .path = "/collection/v1_0/requesttopay",
        .headers = .{},
        .body = "{}",
    };

    const resp = adapter.handleRequestToPay(&req, alloc);
    try std.testing.expectEqual(@as(u16, 401), resp.status);
}

test "MtnMomoAdapter: bad request on missing msisdn" {
    const alloc = std.testing.allocator;
    var store = pending.PendingTxStore.init(alloc);
    defer store.deinit();

    var schema = toml.AdapterSchema{
        .id = "mtn_momo",
        .version = "1.0",
        .auth_type = "oauth2",
        .mti_debit = "0200".*,
        .mti_credit = "0200".*,
        .endpoints = .{},
        .field_mappings = &.{},
        .alloc = alloc,
    };
    defer schema.deinit();

    const dummy_broker = broker_mod.BrokerClient.init(.{
        .host = "127.0.0.1",
        .port = 1,
    }, undefined);

    const adapter = MtnMomoAdapter.init(schema, "tok", "cb", &store, &dummy_broker, 0);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var headers = std.StringHashMapUnmanaged([]const u8){};
    try headers.put(arena.allocator(), "authorization", "Bearer tok");

    var req = http.HttpRequest{
        .arena = arena,
        .method = .POST,
        .path = "/collection/v1_0/requesttopay",
        .headers = headers,
        .body = "{\"amount\":5000,\"currency\":\"XAF\"}",
    };

    const resp = adapter.handleRequestToPay(&req, alloc);
    try std.testing.expectEqual(@as(u16, 400), resp.status);
}

test "MtnMomoAdapter: callback updates state" {
    const alloc = std.testing.allocator;
    var store = pending.PendingTxStore.init(alloc);
    defer store.deinit();

    // Pre-populate a pending transaction
    const ref = "abcdef1234567890abcdef1234567890";
    try store.put(.{
        .msg_id = [_]u8{0xAB} ** 16,
        .reference_id = ref,
        .state = .PENDING,
        .created_at = 0,
        .expires_at = 9_999_999_999,
        .msg_bytes = &.{},
    });

    var schema = toml.AdapterSchema{
        .id = "mtn_momo", .version = "1.0", .auth_type = "oauth2",
        .mti_debit = "0200".*, .mti_credit = "0200".*,
        .endpoints = .{}, .field_mappings = &.{}, .alloc = alloc,
    };
    defer schema.deinit();

    const dummy_broker = broker_mod.BrokerClient.init(.{ .host = "127.0.0.1", .port = 1 }, undefined);
    const adapter = MtnMomoAdapter.init(schema, "tok", "cb_secret", &store, &dummy_broker, 0);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var headers = std.StringHashMapUnmanaged([]const u8){};
    try headers.put(arena.allocator(), "x-callback-token", "cb_secret");

    // FAILED path: entry removed, no broker interaction.
    var req = http.HttpRequest{
        .arena = arena,
        .method = .POST,
        .path = "/webhooks/mtn",
        .headers = headers,
        .body = try std.fmt.allocPrint(arena.allocator(),
            "{{\"referenceId\":\"{s}\",\"status\":\"FAILED\"}}", .{ref}),
    };

    const resp = adapter.handleCallback(&req, alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(store.get(ref) == null);
}
