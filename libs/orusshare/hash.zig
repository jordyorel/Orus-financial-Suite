const std = @import("std");
const message = @import("message.zig");

// Fast u64 hash of PAN for broker partition routing.
// Uses Wyhash — non-cryptographic, collision-resistant enough for partitioning.
pub fn hashPan(pan: []const u8) u64 {
    return std.hash.Wyhash.hash(0, pan);
}

// Partition index from a pan_hash given a partition count.
pub fn partitionIndex(pan_hash: u64, partition_count: u32) u32 {
    return @intCast(pan_hash % partition_count);
}

test "hashPan: same PAN same hash" {
    const h1 = hashPan("4762000000001234");
    const h2 = hashPan("4762000000001234");
    try std.testing.expectEqual(h1, h2);
}

test "hashPan: different PAN different hash" {
    const h1 = hashPan("4762000000001234");
    const h2 = hashPan("4762000000005678");
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
