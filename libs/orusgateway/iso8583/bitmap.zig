const std = @import("std");

// ISO 8583 bitmap — primary (64-bit) + optional secondary (64-bit).
// Bit N is set if field N is present in the message.
// Bit layout: bit 1 = MSB of byte 0, bit 8 = LSB of byte 0, etc.
pub const Bitmap = struct {
    bits: [16]u8 = [_]u8{0} ** 16,

    pub fn isSet(self: *const Bitmap, field: u8) bool {
        if (field == 0) return false;
        const idx: usize = field - 1;
        const byte_idx = idx / 8;
        const bit_shift: u3 = @intCast(7 - (idx % 8));
        return (self.bits[byte_idx] >> bit_shift) & 1 == 1;
    }

    pub fn set(self: *Bitmap, field: u8) void {
        if (field == 0) return;
        const idx: usize = field - 1;
        const byte_idx = idx / 8;
        const bit_shift: u3 = @intCast(7 - (idx % 8));
        self.bits[byte_idx] |= @as(u8, 1) << bit_shift;
    }

    // Number of bytes to write on the wire (8 or 16).
    pub fn byteLen(self: *const Bitmap) usize {
        return if (self.isSet(1)) 16 else 8;
    }
};

test "Bitmap: set and isSet basic" {
    var bm = Bitmap{};
    bm.set(2);
    bm.set(11);
    bm.set(64);
    try std.testing.expect(bm.isSet(2));
    try std.testing.expect(bm.isSet(11));
    try std.testing.expect(bm.isSet(64));
    try std.testing.expect(!bm.isSet(1));
    try std.testing.expect(!bm.isSet(63));
    try std.testing.expectEqual(@as(usize, 8), bm.byteLen());
}

test "Bitmap: secondary bitmap via bit 1" {
    var bm = Bitmap{};
    bm.set(1);
    bm.set(65);
    bm.set(128);
    try std.testing.expect(bm.isSet(1));
    try std.testing.expect(bm.isSet(65));
    try std.testing.expect(bm.isSet(128));
    try std.testing.expectEqual(@as(usize, 16), bm.byteLen());
}

test "Bitmap: wire bytes" {
    // Fields 2, 3, 4 set → byte 0 = 0x70
    var bm = Bitmap{};
    bm.set(2); // bit 2: 0x40
    bm.set(3); // bit 3: 0x20
    bm.set(4); // bit 4: 0x10
    try std.testing.expectEqual(@as(u8, 0x70), bm.bits[0]);
    try std.testing.expectEqual(@as(u8, 0x00), bm.bits[1]);
}
