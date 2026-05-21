const std = @import("std");
const orusshare = @import("orusshare");
const translator = @import("translator.zig");
const bank_client_mod = @import("bank_client.zig");
const parser = @import("iso8583/parser.zig");

pub const InternalMessage = orusshare.InternalMessage;
pub const BankClient = bank_client_mod.BankClient;
pub const BankClientError = bank_client_mod.BankClientError;

pub const GatewayError = translator.TranslateError || BankClientError;

pub const Gateway = struct {
    bank: BankClient,
    pan_hash_seed: u64,

    pub fn init(bank: BankClient, pan_hash_seed: u64) Gateway {
        return .{ .bank = bank, .pan_hash_seed = pan_hash_seed };
    }

    // Full pipeline: InternalMessage → ISO 8583 request → bank → ISO 8583 response → InternalMessage.
    //
    // Ownership: result.fields is allocated with alloc. Caller frees with:
    //   for (result.fields) |f| alloc.free(f.value);
    //   alloc.free(result.fields);
    pub fn process(
        self: *const Gateway,
        req: *const InternalMessage,
        alloc: std.mem.Allocator,
    ) GatewayError!InternalMessage {
        var iso_req = try translator.fromInternal(req, alloc);
        defer iso_req.deinit();

        var iso_resp = try self.bank.send(&iso_req, alloc);
        defer iso_resp.deinit();

        // The response inherits identity from the request.
        // origin and hop_count are updated to reflect the bank leg.
        var base = req.*;
        base.origin = .iso8583;
        base.hop_count +|= 1; // saturating: never wraps past 255
        base.received_at = @intCast(std.Io.Clock.real.now(self.bank.io).nanoseconds);

        return translator.toInternal(&iso_resp, base, alloc, self.pan_hash_seed);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;

const TEST_PORT: u16 = 19233;

fn buildApproval(alloc: std.mem.Allocator, stan: []const u8) ![]u8 {
    var resp = parser.IsoMessage.init(alloc, "0210".*);
    defer resp.deinit();
    try resp.set(39, "00"); // approved
    try resp.set(11, stan[0..6]);
    try resp.set(49, "XAF");
    return parser.serialize(&resp, alloc);
}

fn buildDecline(alloc: std.mem.Allocator, stan: []const u8) ![]u8 {
    var resp = parser.IsoMessage.init(alloc, "0210".*);
    defer resp.deinit();
    try resp.set(39, "51"); // insufficient funds
    try resp.set(11, stan[0..6]);
    try resp.set(49, "XAF");
    return parser.serialize(&resp, alloc);
}

// Fake bank: serves one framed ISO request and replies with `response_bytes`.
fn serveBankOnce(
    server: *std.Io.net.Server,
    io: std.Io,
    response_bytes: []const u8,
    alloc: std.mem.Allocator,
) void {
    var stream = server.accept(io) catch return;
    defer stream.close(io);

    // Drain the inbound 2-byte-prefixed request
    var rbuf: [8192]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    var r = &sr.interface;
    var len_buf: [2]u8 = undefined;
    r.readSliceAll(&len_buf) catch return;
    const req_len = std.mem.readInt(u16, &len_buf, .big);
    const tmp = alloc.alloc(u8, req_len) catch return;
    defer alloc.free(tmp);
    r.readSliceAll(tmp) catch return;

    // Send back 2-byte-prefixed response
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

    const gw = Gateway.init(BankClient.init(.{
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

    // MTI and response code
    try testing.expectEqualSlices(u8, "0210", &resp.mti);
    var rc: ?[]const u8 = null;
    for (resp.fields) |f| {
        if (f.id == 39) rc = f.value;
    }
    try testing.expectEqualStrings("00", rc.?);

    // Gateway metadata updated
    try testing.expectEqual(orusshare.MessageOrigin.iso8583, resp.origin);
    try testing.expectEqual(@as(u8, 2), resp.hop_count); // was 1, incremented to 2
    try testing.expect(resp.received_at != 0);

    // Identity preserved
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

    const gw = Gateway.init(BankClient.init(.{
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
