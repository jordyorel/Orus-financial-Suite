const std = @import("std");

// V1: static bearer token validation (constant-time).
// V2: JWT signature verification + expiry check.
pub const TokenValidator = struct {
    token: []const u8,

    pub fn validate(self: *const TokenValidator, auth_header: []const u8) bool {
        const prefix = "Bearer ";
        if (!std.mem.startsWith(u8, auth_header, prefix)) return false;
        const provided = auth_header[prefix.len..];
        const n = @min(provided.len, self.token.len);
        var diff: u8 = @intFromBool(provided.len != self.token.len);
        for (provided[0..n], self.token[0..n]) |a, b| diff |= a ^ b;
        return diff == 0;
    }
};

// Extract raw bearer token from Authorization header.
pub fn extractBearer(auth_header: []const u8) ?[]const u8 {
    const prefix = "Bearer ";
    if (!std.mem.startsWith(u8, auth_header, prefix)) return null;
    const token = auth_header[prefix.len..];
    if (token.len == 0) return null;
    return token;
}

test "TokenValidator: valid token" {
    const v = TokenValidator{ .token = "my_secret_token" };
    try std.testing.expect(v.validate("Bearer my_secret_token"));
}

test "TokenValidator: wrong token" {
    const v = TokenValidator{ .token = "correct" };
    try std.testing.expect(!v.validate("Bearer wrong"));
}

test "TokenValidator: missing Bearer prefix" {
    const v = TokenValidator{ .token = "token" };
    try std.testing.expect(!v.validate("token"));
}

test "extractBearer: valid" {
    try std.testing.expectEqualStrings("abc123", extractBearer("Bearer abc123").?);
}

test "extractBearer: no prefix" {
    try std.testing.expect(extractBearer("abc123") == null);
}
