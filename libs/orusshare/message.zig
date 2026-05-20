const std = @import("std");

pub const MessageOrigin = enum {
    iso8583,
    rest_json,
    internal,
};

pub const ServiceProvider = enum {
    none,
    orange_money,
    mtn_momo,
    wave,
    airtel_money,
    custom,
};

pub const InternalMessage = struct {
    // Identity
    msg_id: [16]u8,
    schema_id: []const u8,
    topic: []const u8,
    origin: MessageOrigin,
    provider: ServiceProvider,

    // Parsed ISO 8583 content
    mti: [4]u8,
    fields: []FieldEntry,

    // Routing metadata
    stan: [6]u8,
    pan_hash: u64,
    amount: i64,
    currency: [3]u8,
    received_at: i64,

    // Traceability
    source_ip: [16]u8,
    hop_count: u8,
    external_id: ?[]const u8,

    pub const FieldEntry = struct {
        id: u8,
        value: []const u8,
    };

    pub fn computeHash(self: *const InternalMessage) [32]u8 {
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        h.update(&self.msg_id);
        h.update(&self.mti);
        h.update(&self.stan);
        var amt_buf: [8]u8 = undefined;
        std.mem.writeInt(i64, &amt_buf, self.amount, .big);
        h.update(&amt_buf);
        return h.finalResult();
    }
};

test "computeHash: same message same hash" {
    const msg = InternalMessage{
        .msg_id = [_]u8{1} ** 16,
        .schema_id = "gimac",
        .topic = "transactions.financial",
        .origin = .rest_json,
        .provider = .orange_money,
        .mti = "0200".*,
        .fields = &.{},
        .stan = "123456".*,
        .pan_hash = 0xdeadbeef,
        .amount = 5000000,
        .currency = "XAF".*,
        .received_at = 1_700_000_000_000_000_000,
        .source_ip = [_]u8{0} ** 16,
        .hop_count = 1,
        .external_id = null,
    };
    const h1 = msg.computeHash();
    const h2 = msg.computeHash();
    try std.testing.expectEqual(h1, h2);
}

test "computeHash: modified message different hash" {
    var msg = InternalMessage{
        .msg_id = [_]u8{1} ** 16,
        .schema_id = "gimac",
        .topic = "transactions.financial",
        .origin = .rest_json,
        .provider = .orange_money,
        .mti = "0200".*,
        .fields = &.{},
        .stan = "123456".*,
        .pan_hash = 0xdeadbeef,
        .amount = 5000000,
        .currency = "XAF".*,
        .received_at = 1_700_000_000_000_000_000,
        .source_ip = [_]u8{0} ** 16,
        .hop_count = 1,
        .external_id = null,
    };
    const h1 = msg.computeHash();
    msg.amount = 9999999;
    const h2 = msg.computeHash();
    try std.testing.expect(!std.mem.eql(u8, &h1, &h2));
}

test "all MessageOrigin values defined" {
    const origins = [_]MessageOrigin{ .iso8583, .rest_json, .internal };
    for (origins) |o| {
        _ = @intFromEnum(o);
    }
}

test "all ServiceProvider values defined" {
    const providers = [_]ServiceProvider{
        .none, .orange_money, .mtn_momo, .wave, .airtel_money, .custom,
    };
    for (providers) |p| {
        _ = @intFromEnum(p);
    }
}
