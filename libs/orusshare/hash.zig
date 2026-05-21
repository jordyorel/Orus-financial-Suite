const std = @import("std");
const message = @import("message.zig");

// Keyed u64 hash of PAN. seed must be a secret loaded at startup (PCI-DSS Req 3.5).
// Using seed=0 is acceptable only in tests or for pure routing (no WAL persistence).
pub fn hashPan(pan: []const u8, seed: u64) u64 {
    return std.hash.Wyhash.hash(seed, pan);
}

// PCI-DSS Req 3.4 — mask a PAN for safe display in logs and audit trails.
// Shows first 6 and last 4 digits; middle replaced by '*'.
// For PANs ≤ 10 digits (malformed), masks everything.
// buf must be at least pan.len bytes; returns a slice of buf.
//
// Example: maskPan("4762001234567890", &buf) → "476200******7890"
pub fn maskPan(pan: []const u8, buf: []u8) []const u8 {
    const n = pan.len;
    if (buf.len < n) return buf[0..0];
    if (n <= 10) {
        @memset(buf[0..n], '*');
        return buf[0..n];
    }
    @memcpy(buf[0..6], pan[0..6]);
    @memset(buf[6 .. n - 4], '*');
    @memcpy(buf[n - 4 .. n], pan[n - 4 ..]);
    return buf[0..n];
}

// Partition index from a pan_hash given a partition count.
pub fn partitionIndex(pan_hash: u64, partition_count: u32) u32 {
    return @intCast(pan_hash % partition_count);
}

test "maskPan: standard 16-digit PAN" {
    var buf: [16]u8 = undefined;
    const masked = maskPan("4762001234567890", &buf);
    try std.testing.expectEqualStrings("476200******7890", masked);
}

test "maskPan: 19-digit PAN" {
    var buf: [19]u8 = undefined;
    const masked = maskPan("4762001234567890123", &buf);
    try std.testing.expectEqualStrings("476200*********0123", masked);
}

test "maskPan: short PAN masked entirely" {
    var buf: [8]u8 = undefined;
    const masked = maskPan("12345678", &buf);
    for (masked) |c| try std.testing.expectEqual('*', c);
}

test "maskPan: does not contain raw middle digits" {
    var buf: [16]u8 = undefined;
    const masked = maskPan("4762001234567890", &buf);
    try std.testing.expect(std.mem.indexOf(u8, masked, "1234") == null);
}

test "hashPan: same PAN same hash" {
    const h1 = hashPan("4762000000001234", 0);
    const h2 = hashPan("4762000000001234", 0);
    try std.testing.expectEqual(h1, h2);
}

test "hashPan: different PAN different hash" {
    const h1 = hashPan("4762000000001234", 0);
    const h2 = hashPan("4762000000005678", 0);
    try std.testing.expect(h1 != h2);
}

test "partitionIndex: same pan_hash same partition" {
    const idx1 = partitionIndex(0xdeadbeef, 8);
    const idx2 = partitionIndex(0xdeadbeef, 8);
    try std.testing.expectEqual(idx1, idx2);
}

test "partitionIndex: result bounded by partition_count" {
    for (0..100) |i| {
        const idx = partitionIndex(@intCast(i * 7919), 16);
        try std.testing.expect(idx < 16);
    }
}
