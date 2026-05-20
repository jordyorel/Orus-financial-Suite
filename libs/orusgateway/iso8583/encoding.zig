const std = @import("std");

pub const BcdError = error{ InvalidDigit, BufferTooSmall };

// Encode an even-length ASCII decimal string to packed BCD (high nibble first).
// digits.len must be even. Returns the number of BCD bytes written (digits.len / 2).
pub fn bcdEncode(digits: []const u8, out: []u8) BcdError!usize {
    std.debug.assert(digits.len % 2 == 0);
    const n = digits.len / 2;
    if (out.len < n) return error.BufferTooSmall;
    for (0..n) |i| {
        const hi = digits[i * 2];
        const lo = digits[i * 2 + 1];
        if (hi < '0' or hi > '9' or lo < '0' or lo > '9') return error.InvalidDigit;
        out[i] = ((hi - '0') << 4) | (lo - '0');
    }
    return n;
}

// Decode N packed BCD bytes into 2*N ASCII decimal characters.
pub fn bcdDecode(bcd: []const u8, out: []u8) BcdError!usize {
    const n = bcd.len * 2;
    if (out.len < n) return error.BufferTooSmall;
    for (bcd, 0..) |byte, i| {
        out[i * 2]     = '0' + (byte >> 4);
        out[i * 2 + 1] = '0' + (byte & 0x0F);
    }
    return n;
}

// Left-pad `digits` with '0' if length is odd, writing into `out`.
// Returns a slice of `out` (always even length).
pub fn padToEven(digits: []const u8, out: []u8) []const u8 {
    if (digits.len % 2 == 0) {
        @memcpy(out[0..digits.len], digits);
        return out[0..digits.len];
    }
    out[0] = '0';
    @memcpy(out[1..][0..digits.len], digits);
    return out[0 .. digits.len + 1];
}

// EBCDIC (IBM CP037) ↔ ASCII conversion tables, computed at compile time.
pub const EBCDIC_TO_ASCII: [256]u8 = blk: {
    var t = [_]u8{0} ** 256;
    t[0x40] = ' ';
    t[0x4B] = '.';  t[0x4C] = '<';  t[0x4D] = '(';  t[0x4E] = '+';  t[0x4F] = '|';
    t[0x50] = '&';  t[0x5A] = '!';  t[0x5B] = '$';  t[0x5C] = '*';  t[0x5D] = ')';
    t[0x5E] = ';';  t[0x60] = '-';  t[0x61] = '/';  t[0x6B] = ',';  t[0x6C] = '%';
    t[0x6D] = '_';  t[0x6E] = '>';  t[0x6F] = '?';  t[0x7A] = ':';  t[0x7B] = '#';
    t[0x7C] = '@';  t[0x7D] = '\''; t[0x7E] = '=';  t[0x7F] = '"';
    // a-i
    t[0x81] = 'a'; t[0x82] = 'b'; t[0x83] = 'c'; t[0x84] = 'd'; t[0x85] = 'e';
    t[0x86] = 'f'; t[0x87] = 'g'; t[0x88] = 'h'; t[0x89] = 'i';
    // j-r
    t[0x91] = 'j'; t[0x92] = 'k'; t[0x93] = 'l'; t[0x94] = 'm'; t[0x95] = 'n';
    t[0x96] = 'o'; t[0x97] = 'p'; t[0x98] = 'q'; t[0x99] = 'r';
    // s-z
    t[0xA2] = 's'; t[0xA3] = 't'; t[0xA4] = 'u'; t[0xA5] = 'v'; t[0xA6] = 'w';
    t[0xA7] = 'x'; t[0xA8] = 'y'; t[0xA9] = 'z';
    // A-I
    t[0xC1] = 'A'; t[0xC2] = 'B'; t[0xC3] = 'C'; t[0xC4] = 'D'; t[0xC5] = 'E';
    t[0xC6] = 'F'; t[0xC7] = 'G'; t[0xC8] = 'H'; t[0xC9] = 'I';
    // J-R
    t[0xD1] = 'J'; t[0xD2] = 'K'; t[0xD3] = 'L'; t[0xD4] = 'M'; t[0xD5] = 'N';
    t[0xD6] = 'O'; t[0xD7] = 'P'; t[0xD8] = 'Q'; t[0xD9] = 'R';
    // S-Z
    t[0xE2] = 'S'; t[0xE3] = 'T'; t[0xE4] = 'U'; t[0xE5] = 'V'; t[0xE6] = 'W';
    t[0xE7] = 'X'; t[0xE8] = 'Y'; t[0xE9] = 'Z';
    // 0-9
    t[0xF0] = '0'; t[0xF1] = '1'; t[0xF2] = '2'; t[0xF3] = '3'; t[0xF4] = '4';
    t[0xF5] = '5'; t[0xF6] = '6'; t[0xF7] = '7'; t[0xF8] = '8'; t[0xF9] = '9';
    break :blk t;
};

pub const ASCII_TO_EBCDIC: [256]u8 = blk: {
    var t = [_]u8{0x40} ** 256; // default: EBCDIC space
    var i: usize = 0;
    while (i < 256) : (i += 1) {
        const a = EBCDIC_TO_ASCII[i];
        if (a != 0) t[a] = @intCast(i);
    }
    t[0] = 0;
    break :blk t;
};

pub fn decodeEbcdic(src: []const u8, dst: []u8) void {
    for (src, 0..) |b, i| dst[i] = EBCDIC_TO_ASCII[b];
}

pub fn encodeEbcdic(src: []const u8, dst: []u8) void {
    for (src, 0..) |b, i| dst[i] = ASCII_TO_EBCDIC[b];
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "BCD encode/decode round-trip" {
    const digits = "000000010000";
    var bcd: [6]u8 = undefined;
    const n = try bcdEncode(digits, &bcd);
    try std.testing.expectEqual(@as(usize, 6), n);
    try std.testing.expectEqual(@as(u8, 0x00), bcd[0]);
    try std.testing.expectEqual(@as(u8, 0x01), bcd[3]);
    try std.testing.expectEqual(@as(u8, 0x00), bcd[4]);

    var ascii: [12]u8 = undefined;
    const m = try bcdDecode(bcd[0..n], &ascii);
    try std.testing.expectEqual(@as(usize, 12), m);
    try std.testing.expectEqualStrings(digits, ascii[0..m]);
}

test "BCD encode PAN" {
    const digits = "4111111111111111"; // 16 digits
    var bcd: [8]u8 = undefined;
    const n = try bcdEncode(digits, &bcd);
    try std.testing.expectEqual(@as(usize, 8), n);
    try std.testing.expectEqual(@as(u8, 0x41), bcd[0]);
    try std.testing.expectEqual(@as(u8, 0x11), bcd[1]);
}

test "padToEven: odd length" {
    var buf: [20]u8 = undefined;
    const result = padToEven("123", &buf);
    try std.testing.expectEqualStrings("0123", result);
}

test "padToEven: even length unchanged" {
    var buf: [20]u8 = undefined;
    const result = padToEven("1234", &buf);
    try std.testing.expectEqualStrings("1234", result);
}

test "EBCDIC: decode digits 0-9" {
    var ebcdic: [10]u8 = undefined;
    for (0..10) |i| ebcdic[i] = 0xF0 + @as(u8, @intCast(i));
    var ascii: [10]u8 = undefined;
    decodeEbcdic(&ebcdic, &ascii);
    try std.testing.expectEqualStrings("0123456789", &ascii);
}

test "EBCDIC: round-trip ASCII letters" {
    const src = "Hello";
    var ebcdic: [5]u8 = undefined;
    encodeEbcdic(src, &ebcdic);
    var back: [5]u8 = undefined;
    decodeEbcdic(&ebcdic, &back);
    try std.testing.expectEqualStrings(src, &back);
}
