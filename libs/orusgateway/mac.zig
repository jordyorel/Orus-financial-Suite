// ISO 8583 Message Authentication Code — Field 64 (primary) / Field 128 (secondary).
//
// Algorithm: HMAC-SHA256 truncated to 8 bytes (ISO 16609 profile).
// The session key is provided at sign-on (Field 96 of the 0810 response).
//
// MAC scope: all serialized message bytes with F64/F128 absent.
// Workflow:
//   send  → serialize without F64 → compute → set F64 → re-serialize
//   recv  → extract F64 → clone without F64 → serialize → compare

const std    = @import("std");
const parser = @import("iso8583/parser.zig");

const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

pub const MAC_LEN: usize = 8;
pub const KEY_LEN: usize = 32;

pub const MacEngine = struct {
    key:    [KEY_LEN]u8,
    active: bool,

    pub fn init() MacEngine {
        return .{ .key = [_]u8{0} ** KEY_LEN, .active = false };
    }

    // Load key from sign-on F96. Truncates or zero-pads to KEY_LEN.
    pub fn setKey(self: *MacEngine, material: []const u8) void {
        const n = @min(material.len, KEY_LEN);
        @memcpy(self.key[0..n], material[0..n]);
        if (n < KEY_LEN) @memset(self.key[n..], 0);
        self.active = true;
    }

    // Compute 8-byte MAC = first 8 bytes of HMAC-SHA256(key, data).
    pub fn compute(self: *const MacEngine, data: []const u8) [MAC_LEN]u8 {
        var full: [HmacSha256.mac_length]u8 = undefined;
        HmacSha256.create(&full, data, &self.key);
        return full[0..MAC_LEN].*;
    }

    // Returns false on mismatch → caller must reject the message.
    // Constant-time comparison prevents timing oracle attacks on the MAC value.
    pub fn verify(self: *const MacEngine, data: []const u8, received: []const u8) bool {
        if (received.len != MAC_LEN) return false;
        const expected = self.compute(data);
        return timingSafeEql(&expected, received[0..MAC_LEN]);
    }
};

// XOR-accumulation constant-time comparison — no branch on secret bytes.
fn timingSafeEql(a: *const [MAC_LEN]u8, b: *const [MAC_LEN]u8) bool {
    var diff: u8 = 0;
    for (a, b) |x, y| diff |= x ^ y;
    return diff == 0;
}

// Build a copy of `msg` without F64 and F128 (for MAC computation scope).
pub fn cloneWithoutMac(msg: *const parser.IsoMessage, alloc: std.mem.Allocator) !parser.IsoMessage {
    var copy = parser.IsoMessage.init(alloc, msg.mti);
    errdefer copy.deinit();
    for (2..128) |n| {                 // 2..127 inclusive (skip F64 and F128)
        if (n == 64) continue;
        if (msg.get(@intCast(n))) |val| try copy.set(@intCast(n), val);
    }
    return copy;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "MacEngine: compute is deterministic" {
    var engine = MacEngine.init();
    engine.setKey("test_key_32bytes_padding_xxxxxxx");
    const data = "hello iso 8583";
    const m1 = engine.compute(data);
    const m2 = engine.compute(data);
    try std.testing.expectEqualSlices(u8, &m1, &m2);
    try std.testing.expectEqual(@as(usize, MAC_LEN), m1.len);
}

test "MacEngine: verify round-trip" {
    var engine = MacEngine.init();
    engine.setKey("test_key_32bytes_padding_xxxxxxx");
    const data = "0200 gimac transaction";
    const mac_bytes = engine.compute(data);
    try std.testing.expect(engine.verify(data, &mac_bytes));
}

test "MacEngine: verify rejects tampered data" {
    var engine = MacEngine.init();
    engine.setKey("test_key_32bytes_padding_xxxxxxx");
    const mac_bytes = engine.compute("original data");
    try std.testing.expect(!engine.verify("tampered data", &mac_bytes));
}

test "MacEngine: verify rejects wrong length" {
    var engine = MacEngine.init();
    engine.setKey("key");
    const mac_bytes = engine.compute("data");
    try std.testing.expect(!engine.verify("data", mac_bytes[0..4]));
}

test "cloneWithoutMac: F64 excluded, other fields preserved" {
    const alloc = std.testing.allocator;
    var msg = parser.IsoMessage.init(alloc, "0200".*);
    defer msg.deinit();
    try msg.set(4,  "000000010000");
    try msg.set(11, "000042");
    try msg.set(64, &[_]u8{0xAA} ** 8);

    var clone = try cloneWithoutMac(&msg, alloc);
    defer clone.deinit();

    try std.testing.expect(clone.get(64) == null);
    try std.testing.expectEqualStrings("000000010000", clone.get(4).?);
    try std.testing.expectEqualStrings("000042", clone.get(11).?);
}
