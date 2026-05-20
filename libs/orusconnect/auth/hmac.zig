const std = @import("std");

const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;
const mac_len = Hmac.mac_length;

const hex_chars = "0123456789abcdef";

fn bytesToHex(bytes: []const u8, out: []u8) void {
    for (bytes, 0..) |b, i| {
        out[i * 2] = hex_chars[b >> 4];
        out[i * 2 + 1] = hex_chars[b & 0xf];
    }
}

// Validates a hex-encoded HMAC-SHA256 signature against payload + secret.
// Constant-time comparison prevents timing attacks.
pub fn validateHmacSha256(
    secret: []const u8,
    payload: []const u8,
    signature_hex: []const u8,
) bool {
    if (signature_hex.len != mac_len * 2) return false;

    var mac_bytes: [mac_len]u8 = undefined;
    var h = Hmac.init(secret);
    h.update(payload);
    h.final(&mac_bytes);

    var computed_hex: [mac_len * 2]u8 = undefined;
    bytesToHex(&mac_bytes, &computed_hex);

    return std.crypto.timing_safe.eql([mac_len * 2]u8, computed_hex, signature_hex[0 .. mac_len * 2].*);
}

// Builds the HMAC-SHA256 signature for outgoing requests (used in tests).
pub fn sign(secret: []const u8, payload: []const u8) [mac_len * 2]u8 {
    var mac_bytes: [mac_len]u8 = undefined;
    var h = Hmac.init(secret);
    h.update(payload);
    h.final(&mac_bytes);

    var hex: [mac_len * 2]u8 = undefined;
    bytesToHex(&mac_bytes, &hex);
    return hex;
}

test "validateHmacSha256: valid signature" {
    const secret = "test_secret_key";
    const payload = "{\"amount\":5000}";
    const sig = sign(secret, payload);
    try std.testing.expect(validateHmacSha256(secret, payload, &sig));
}

test "validateHmacSha256: wrong secret" {
    const payload = "{\"amount\":5000}";
    const sig = sign("correct_secret", payload);
    try std.testing.expect(!validateHmacSha256("wrong_secret", payload, &sig));
}

test "validateHmacSha256: tampered payload" {
    const secret = "secret";
    const sig = sign(secret, "{\"amount\":5000}");
    try std.testing.expect(!validateHmacSha256(secret, "{\"amount\":9999}", &sig));
}

test "validateHmacSha256: wrong length" {
    try std.testing.expect(!validateHmacSha256("s", "p", "tooshort"));
}
