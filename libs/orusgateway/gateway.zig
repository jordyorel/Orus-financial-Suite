const std = @import("std");
const orusshare = @import("orusshare");
const translator = @import("translator.zig");
const bank_client_mod = @import("bank_client.zig");
const reconciliation_mod = @import("reconciliation.zig");
const network_mgmt = @import("network_mgmt.zig");
const mac_mod = @import("mac.zig");
const parser = @import("iso8583/parser.zig");

pub const InternalMessage = orusshare.InternalMessage;
pub const BankClient = bank_client_mod.BankClient;
pub const BankClientError = bank_client_mod.BankClientError;
pub const ReconciliationState = reconciliation_mod.ReconciliationState;
pub const MacEngine = mac_mod.MacEngine;

pub const GatewayError = translator.TranslateError || BankClientError || error{MacMismatch};
pub const NetworkError = BankClientError || error{ SignOnRejected, HeartbeatFailed };

pub const Gateway = struct {
    bank:           BankClient,
    pan_hash_seed:  u64,
    reconciliation: ?*ReconciliationState = null,
    mac_engine:     ?*MacEngine           = null,

    pub fn init(bank: BankClient, pan_hash_seed: u64) Gateway {
        return .{ .bank = bank, .pan_hash_seed = pan_hash_seed };
    }

    // Full pipeline: InternalMessage → ISO 8583 → bank → ISO 8583 → InternalMessage.
    // If mac_engine is active, appends MAC (F64) to the outbound message and
    // verifies MAC on the response (returns MacMismatch on failure).
    pub fn process(
        self:  *Gateway,
        req:   *const InternalMessage,
        alloc: std.mem.Allocator,
    ) GatewayError!InternalMessage {
        var iso_req = try translator.fromInternal(req, alloc);
        defer iso_req.deinit();

        // Append MAC to outbound request, then send pre-serialized bytes to
        // avoid a third serialization (scope bytes + wire bytes + bank.send bytes).
        var iso_resp = mac_blk: {
            if (self.mac_engine) |mac| {
                if (mac.active) {
                    // 1st serialization: scope without F64
                    const scope = parser.serialize(&iso_req, alloc) catch return error.OutOfMemory;
                    defer alloc.free(scope);
                    const mac_bytes = mac.compute(scope);
                    iso_req.set(64, &mac_bytes) catch return error.OutOfMemory;
                    // 2nd serialization: wire bytes with F64 set — sent directly
                    const wire = parser.serialize(&iso_req, alloc) catch return error.OutOfMemory;
                    defer alloc.free(wire);
                    break :mac_blk try self.bank.sendBytes(wire, alloc);
                }
            }
            break :mac_blk try self.bank.send(&iso_req, alloc);
        };
        defer iso_resp.deinit();

        // Verify MAC on bank response — reject the transaction on mismatch.
        if (self.mac_engine) |mac| if (mac.active) {
            const mac_ok: bool = blk: {
                const received_mac = iso_resp.get(64) orelse break :blk false;
                var clone = mac_mod.cloneWithoutMac(&iso_resp, alloc) catch break :blk false;
                defer clone.deinit();
                const rd = parser.serialize(&clone, alloc) catch break :blk false;
                defer alloc.free(rd);
                break :blk mac.verify(rd, received_mac);
            };
            if (!mac_ok) return error.MacMismatch;
        };

        var base = req.*;
        base.origin    = .iso8583;
        base.hop_count +|= 1;
        base.received_at = @intCast(std.Io.Clock.real.now(self.bank.io).nanoseconds);

        const result = try translator.toInternal(&iso_resp, base, alloc, self.pan_hash_seed);
        if (self.reconciliation) |r| r.record(&result);
        return result;
    }

    // Send a reversal for an approved payment (0420/0421 → 0430).
    pub fn processReversal(
        self:     *Gateway,
        original: *const InternalMessage,
        alloc:    std.mem.Allocator,
    ) GatewayError!InternalMessage {
        var rev = try translator.buildReversal(original, alloc);
        defer {
            for (rev.fields) |f| alloc.free(f.value);
            alloc.free(rev.fields);
        }
        return self.process(&rev, alloc);
    }

    // Send sign-on (0800/NMI=301). Stores session key from F96 into mac_engine.
    pub fn signOn(self: *Gateway, stan: *u32, alloc: std.mem.Allocator) NetworkError!void {
        var req = network_mgmt.buildRequest(network_mgmt.NMI.SIGNON, stan, alloc)
            catch return error.BankUnreachable;
        defer req.deinit();
        var resp = try self.bank.send(&req, alloc);
        defer resp.deinit();
        const rc = resp.get(39) orelse return error.SignOnRejected;
        if (!std.mem.eql(u8, rc, "00")) return error.SignOnRejected;
        if (self.mac_engine) |mac| {
            if (resp.get(96)) |key| mac.setKey(key);
        }
    }

    // Send sign-off (0800/NMI=302). Best-effort.
    pub fn signOff(self: *Gateway, stan: *u32, alloc: std.mem.Allocator) void {
        var req = network_mgmt.buildRequest(network_mgmt.NMI.SIGNOFF, stan, alloc) catch return;
        defer req.deinit();
        var resp = self.bank.send(&req, alloc) catch return;
        defer resp.deinit();
        const rc = resp.get(39) orelse "";
        if (!std.mem.eql(u8, rc, "00"))
            std.log.warn("gateway: sign-off rejected RC={s}", .{rc});
    }

    // Send heartbeat (0800/NMI=801). Call every ~30s to keep TCP session alive.
    pub fn sendHeartbeat(self: *Gateway, stan: *u32, alloc: std.mem.Allocator) NetworkError!void {
        var req = network_mgmt.buildRequest(network_mgmt.NMI.ECHO, stan, alloc)
            catch return error.BankUnreachable;
        defer req.deinit();
        var resp = try self.bank.send(&req, alloc);
        defer resp.deinit();
        const rc = resp.get(39) orelse return error.HeartbeatFailed;
        if (!std.mem.eql(u8, rc, "00")) return error.HeartbeatFailed;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

const TEST_PORT: u16 = 19233;

fn buildApproval(alloc: std.mem.Allocator, stan: []const u8) ![]u8 {
    var resp = parser.IsoMessage.init(alloc, "0210".*);
    defer resp.deinit();
    try resp.set(39, "00");
    try resp.set(11, stan[0..6]);
    try resp.set(49, "XAF");
    return parser.serialize(&resp, alloc);
}

fn buildDecline(alloc: std.mem.Allocator, stan: []const u8) ![]u8 {
    var resp = parser.IsoMessage.init(alloc, "0210".*);
    defer resp.deinit();
    try resp.set(39, "51");
    try resp.set(11, stan[0..6]);
    try resp.set(49, "XAF");
    return parser.serialize(&resp, alloc);
}

fn serveBankOnce(
    server: *std.Io.net.Server,
    io: std.Io,
    response_bytes: []const u8,
    alloc: std.mem.Allocator,
) void {
    var stream = server.accept(io) catch return;
    defer stream.close(io);

    var rbuf: [8192]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    var r = &sr.interface;
    var len_buf: [2]u8 = undefined;
    r.readSliceAll(&len_buf) catch return;
    const req_len = std.mem.readInt(u16, &len_buf, .big);
    const tmp = alloc.alloc(u8, req_len) catch return;
    defer alloc.free(tmp);
    r.readSliceAll(tmp) catch return;

    var wbuf: [8192]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    var w = &sw.interface;
    w.writeInt(u16, @intCast(response_bytes.len), .big) catch return;
    w.writeAll(response_bytes) catch return;
    w.flush() catch return;
}

test "Gateway.process: approved 0210 response" {
    const alloc = testing.allocator;

    const approval = try buildApproval(alloc, "001234");
    defer alloc.free(approval);

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", TEST_PORT);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    const thread = try std.Thread.spawn(.{}, serveBankOnce, .{ &server, io, approval, alloc });
    defer thread.join();

    var gw = Gateway.init(BankClient.init(.{
        .host = "127.0.0.1",
        .port = TEST_PORT,
    }, io), 0);

    var adapter_fields = [_]InternalMessage.FieldEntry{
        .{ .id = 3, .value = "300000" },
    };
    const req = InternalMessage{
        .msg_id = [_]u8{0xAA} ** 16,
        .schema_id = "gimac",
        .topic = "transactions.financial",
        .origin = .rest_json,
        .provider = .orange_money,
        .mti = "0200".*,
        .fields = &adapter_fields,
        .stan = "001234".*,
        .pan_hash = 0,
        .amount = 10000,
        .currency = "XAF".*,
        .received_at = 0,
        .source_ip = [_]u8{0} ** 16,
        .hop_count = 1,
        .external_id = null,
    };

    const resp = try gw.process(&req, alloc);
    defer {
        for (resp.fields) |f| alloc.free(f.value);
        alloc.free(resp.fields);
    }

    try testing.expectEqualSlices(u8, "0210", &resp.mti);
    var rc: ?[]const u8 = null;
    for (resp.fields) |f| {
        if (f.id == 39) rc = f.value;
    }
    try testing.expectEqualStrings("00", rc.?);
    try testing.expectEqual(orusshare.MessageOrigin.iso8583, resp.origin);
    try testing.expectEqual(@as(u8, 2), resp.hop_count);
    try testing.expect(resp.received_at != 0);
    try testing.expectEqualSlices(u8, &req.msg_id, &resp.msg_id);
    try testing.expectEqualStrings(req.topic, resp.topic);
}

test "Gateway.process: declined 0210 response" {
    const alloc = testing.allocator;

    const decline = try buildDecline(alloc, "001235");
    defer alloc.free(decline);

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", TEST_PORT + 1);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    const thread = try std.Thread.spawn(.{}, serveBankOnce, .{ &server, io, decline, alloc });
    defer thread.join();

    var gw = Gateway.init(BankClient.init(.{
        .host = "127.0.0.1",
        .port = TEST_PORT + 1,
    }, io), 0);

    const req = InternalMessage{
        .msg_id = [_]u8{0xBB} ** 16,
        .schema_id = "gimac",
        .topic = "transactions.financial",
        .origin = .rest_json,
        .provider = .mtn_momo,
        .mti = "0200".*,
        .fields = &.{},
        .stan = "001235".*,
        .pan_hash = 0,
        .amount = 50000,
        .currency = "XAF".*,
        .received_at = 0,
        .source_ip = [_]u8{0} ** 16,
        .hop_count = 0,
        .external_id = null,
    };

    const resp = try gw.process(&req, alloc);
    defer {
        for (resp.fields) |f| alloc.free(f.value);
        alloc.free(resp.fields);
    }

    try testing.expectEqualSlices(u8, "0210", &resp.mti);
    var rc: ?[]const u8 = null;
    for (resp.fields) |f| {
        if (f.id == 39) rc = f.value;
    }
    try testing.expectEqualStrings("51", rc.?);
    try testing.expectEqual(@as(u8, 1), resp.hop_count);
}
