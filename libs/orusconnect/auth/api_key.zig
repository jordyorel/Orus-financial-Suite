const std = @import("std");

// V1: static API key validation (constant-time).
pub const ApiKeyValidator = struct {
    key: []const u8,
    header_name: []const u8 = "x-api-key",

    pub fn validate(self: *const ApiKeyValidator, provided: []const u8) bool {
        if (provided.len != self.key.len) return false;
        var diff: u8 = 0;
        for (provided, self.key) |a, b| diff |= a ^ b;
        return diff == 0;
    }
};

test "ApiKeyValidator: valid key" {
    const v = ApiKeyValidator{ .key = "my_api_key_123" };
    try std.testing.expect(v.validate("my_api_key_123"));
}

test "ApiKeyValidator: invalid key" {
    const v = ApiKeyValidator{ .key = "correct_key" };
    try std.testing.expect(!v.validate("wrong_key"));
}

test "ApiKeyValidator: empty key rejected" {
    const v = ApiKeyValidator{ .key = "key" };
    try std.testing.expect(!v.validate(""));
}
