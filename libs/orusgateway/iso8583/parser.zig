const std = @import("std");
const bitmap_mod = @import("bitmap.zig");
const fields_mod = @import("fields.zig");
const schema_mod = @import("schema.zig");
const enc = @import("encoding.zig");

pub const Bitmap = bitmap_mod.Bitmap;
pub const FieldKind = fields_mod.FieldKind;
pub const FieldSpec = fields_mod.FieldSpec;
pub const SPECS = fields_mod.SPECS;
pub const IsoSchema = schema_mod.IsoSchema;
pub const FieldDef = schema_mod.FieldDef;
pub const Encoding = schema_mod.Encoding;
pub const FieldType = schema_mod.FieldType;

pub const ParseError = error{
    TooShort,
    InvalidMti,
    UnknownField,
    InvalidLength,
    FieldTooLong,
    OutOfMemory,
};

// An ISO 8583 message. All field slices are owned by the arena allocator.
// Call deinit() when done.
pub const IsoMessage = struct {
    arena: std.heap.ArenaAllocator,
    mti: [4]u8,
    bitmap: Bitmap,
    // fields[n] holds the raw data for field n (1-indexed, index 0 unused).
    fields: [129]?[]const u8,

    pub fn init(alloc: std.mem.Allocator, mti: [4]u8) IsoMessage {
        return .{
            .arena = std.heap.ArenaAllocator.init(alloc),
            .mti = mti,
            .bitmap = .{},
            .fields = [_]?[]const u8{null} ** 129,
        };
    }

    pub fn deinit(self: *IsoMessage) void {
        self.arena.deinit();
    }

    pub fn get(self: *const IsoMessage, n: u8) ?[]const u8 {
        if (n == 0 or n > 128) return null;
        return self.fields[n];
    }

    // Set field n to a copy of val (duped into the arena).
    // Automatically enables the secondary bitmap when n > 64.
    pub fn set(self: *IsoMessage, n: u8, val: []const u8) !void {
        if (n == 0 or n > 128) return;
        self.fields[n] = try self.arena.allocator().dupe(u8, val);
        self.bitmap.set(n);
        if (n > 64) self.bitmap.set(1);
    }
};

// Parse an ISO 8583 message from a raw byte slice.
// The slice should contain exactly one message (MTI + bitmap + fields).
pub fn parse(data: []const u8, alloc: std.mem.Allocator) ParseError!IsoMessage {
    if (data.len < 4) return error.TooShort;

    for (data[0..4]) |b| {
        if (b < '0' or b > '9') return error.InvalidMti;
    }
    var mti: [4]u8 = undefined;
    @memcpy(&mti, data[0..4]);

    var pos: usize = 4;

    if (pos + 8 > data.len) return error.TooShort;
    var bm = Bitmap{};
    @memcpy(bm.bits[0..8], data[pos..][0..8]);
    pos += 8;

    if (bm.isSet(1)) {
        if (pos + 8 > data.len) return error.TooShort;
        @memcpy(bm.bits[8..16], data[pos..][0..8]);
        pos += 8;
    }

    var msg = IsoMessage{
        .arena = std.heap.ArenaAllocator.init(alloc),
        .mti = mti,
        .bitmap = bm,
        .fields = [_]?[]const u8{null} ** 129,
    };
    errdefer msg.arena.deinit();
    const a = msg.arena.allocator();

    for (2..129) |n| {
        if (!bm.isSet(@intCast(n))) continue;

        const spec = SPECS[n] orelse return error.UnknownField;

        const field_len: usize = switch (spec.kind) {
            .fixed => spec.len,
            .llvar => blk: {
                if (pos + 2 > data.len) return error.TooShort;
                const len = try asciiLen(data[pos..][0..2]);
                if (len > spec.len) return error.FieldTooLong;
                pos += 2;
                break :blk len;
            },
            .lllvar => blk: {
                if (pos + 3 > data.len) return error.TooShort;
                const len = try asciiLen(data[pos..][0..3]);
                if (len > spec.len) return error.FieldTooLong;
                pos += 3;
                break :blk len;
            },
        };

        if (pos + field_len > data.len) return error.TooShort;
        msg.fields[n] = try a.dupe(u8, data[pos..][0..field_len]);
        pos += field_len;
    }

    return msg;
}

// Serialize an IsoMessage to a byte slice owned by the caller.
pub fn serialize(msg: *const IsoMessage, alloc: std.mem.Allocator) ![]u8 {
    var buf = std.ArrayList(u8).empty;
    errdefer buf.deinit(alloc);

    try buf.appendSlice(alloc, &msg.mti);

    const bm_len = msg.bitmap.byteLen();
    try buf.appendSlice(alloc, msg.bitmap.bits[0..bm_len]);

    for (2..129) |n| {
        if (!msg.bitmap.isSet(@intCast(n))) continue;
        const val = msg.fields[n] orelse continue;
        const spec = SPECS[n] orelse continue;

        switch (spec.kind) {
            .fixed => {
                try buf.appendSlice(alloc, val);
            },
            .llvar => {
                var lb: [2]u8 = undefined;
                writeLen2(&lb, val.len);
                try buf.appendSlice(alloc, &lb);
                try buf.appendSlice(alloc, val);
            },
            .lllvar => {
                var lb: [3]u8 = undefined;
                writeLen3(&lb, val.len);
                try buf.appendSlice(alloc, &lb);
                try buf.appendSlice(alloc, val);
            },
        }
    }

    return buf.toOwnedSlice(alloc);
}

// ── Schema-aware parse / serialize ───────────────────────────────────────────
//
// parseWithSchema decodes wire encoding (BCD, EBCDIC, BINARY) per FieldDef.
// IsoMessage.fields always holds the decoded/canonical representation:
//   ASCII/EBCDIC → UTF-8 string   BCD → ASCII digit string   BINARY → raw bytes
//
// serializeWithSchema encodes canonical values back to wire format.

pub fn parseWithSchema(
    data:   []const u8,
    schema: *const IsoSchema,
    alloc:  std.mem.Allocator,
) ParseError!IsoMessage {
    var pos: usize = 0;

    // Skip length prefix header if present.
    switch (schema.header_type) {
        .none => {},
        .length_2 => {
            if (data.len < 2) return error.TooShort;
            pos += 2;
        },
        .length_4 => {
            if (data.len < 4) return error.TooShort;
            pos += 4;
        },
    }

    // MTI (4 ASCII digits, or 2 BCD bytes decoded to 4 digits)
    const mti_wire: usize = if (schema.mti_encoding == .BCD) 2 else 4;
    if (pos + mti_wire > data.len) return error.TooShort;

    var mti: [4]u8 = undefined;
    if (schema.mti_encoding == .BCD) {
        var tmp: [4]u8 = undefined;
        _ = enc.bcdDecode(data[pos..][0..2], &tmp) catch return error.InvalidMti;
        @memcpy(&mti, &tmp);
    } else {
        @memcpy(&mti, data[pos..][0..4]);
    }
    for (mti) |b| if (b < '0' or b > '9') return error.InvalidMti;
    pos += mti_wire;

    // Bitmap (always binary on wire, 8 or 16 bytes)
    if (pos + 8 > data.len) return error.TooShort;
    var bm = Bitmap{};
    @memcpy(bm.bits[0..8], data[pos..][0..8]);
    pos += 8;
    if (bm.isSet(1)) {
        if (pos + 8 > data.len) return error.TooShort;
        @memcpy(bm.bits[8..16], data[pos..][0..8]);
        pos += 8;
    }

    var msg = IsoMessage{
        .arena  = std.heap.ArenaAllocator.init(alloc),
        .mti    = mti,
        .bitmap = bm,
        .fields = [_]?[]const u8{null} ** 129,
    };
    errdefer msg.arena.deinit();
    const a = msg.arena.allocator();

    for (2..129) |n| {
        if (!bm.isSet(@intCast(n))) continue;
        const fdef = schema.getField(@intCast(n)) orelse return error.UnknownField;
        msg.fields[n] = try schemaReadField(data, &pos, fdef, a);
    }
    return msg;
}

pub fn serializeWithSchema(
    msg:    *const IsoMessage,
    schema: *const IsoSchema,
    alloc:  std.mem.Allocator,
) ![]u8 {
    var body: std.ArrayList(u8) = .empty;
    errdefer body.deinit(alloc);

    // MTI
    if (schema.mti_encoding == .BCD) {
        var bcd: [2]u8 = undefined;
        _ = enc.bcdEncode(&msg.mti, &bcd) catch return error.OutOfMemory;
        try body.appendSlice(alloc, &bcd);
    } else {
        try body.appendSlice(alloc, &msg.mti);
    }

    // Bitmap
    const bm_len = msg.bitmap.byteLen();
    try body.appendSlice(alloc, msg.bitmap.bits[0..bm_len]);

    // Fields
    for (2..129) |n| {
        if (!msg.bitmap.isSet(@intCast(n))) continue;
        const val = msg.fields[n] orelse continue;
        const fdef = schema.getField(@intCast(n)) orelse continue;
        try schemaWriteField(&body, val, fdef, alloc);
    }

    // Length prefix header
    switch (schema.header_type) {
        .none => return body.toOwnedSlice(alloc),
        .length_2 => {
            var out: std.ArrayList(u8) = .empty;
            errdefer out.deinit(alloc);
            var hdr: [2]u8 = undefined;
            std.mem.writeInt(u16, &hdr, @intCast(body.items.len), .big);
            try out.appendSlice(alloc, &hdr);
            try out.appendSlice(alloc, body.items);
            body.deinit(alloc);
            return out.toOwnedSlice(alloc);
        },
        .length_4 => {
            var out: std.ArrayList(u8) = .empty;
            errdefer out.deinit(alloc);
            var hdr: [4]u8 = undefined;
            std.mem.writeInt(u32, &hdr, @intCast(body.items.len), .big);
            try out.appendSlice(alloc, &hdr);
            try out.appendSlice(alloc, body.items);
            body.deinit(alloc);
            return out.toOwnedSlice(alloc);
        },
    }
}

// Wire length for a FIXED field given its encoding.
fn fixedWireLen(fdef: *const FieldDef) usize {
    return switch (fdef.encoding) {
        .BCD => (fdef.length + 1) / 2, // ceil(digits / 2)
        else => fdef.length,
    };
}

// Wire data length for a variable-length field given declared digit count.
fn varWireLen(enc_type: Encoding, digit_count: usize) usize {
    return switch (enc_type) {
        .BCD => (digit_count + 1) / 2,
        else => digit_count,
    };
}

fn schemaReadField(
    data:  []const u8,
    pos:   *usize,
    fdef:  *const FieldDef,
    a:     std.mem.Allocator,
) ParseError![]const u8 {
    const wire_len: usize = switch (fdef.field_type) {
        .FIXED => fixedWireLen(fdef),
        .LLVAR => blk: {
            if (pos.* + 2 > data.len) return error.TooShort;
            const dc = asciiLen(data[pos.*..][0..2]) catch return error.InvalidLength;
            if (dc > fdef.length) return error.FieldTooLong;
            pos.* += 2;
            break :blk varWireLen(fdef.encoding, dc);
        },
        .LLLVAR, .TLV => blk: {
            if (pos.* + 3 > data.len) return error.TooShort;
            const dc = asciiLen(data[pos.*..][0..3]) catch return error.InvalidLength;
            if (dc > fdef.length) return error.FieldTooLong;
            pos.* += 3;
            break :blk varWireLen(fdef.encoding, dc);
        },
    };

    if (pos.* + wire_len > data.len) return error.TooShort;
    const raw = data[pos.*..][0..wire_len];
    pos.* += wire_len;

    return switch (fdef.encoding) {
        .ASCII  => a.dupe(u8, raw) catch return error.OutOfMemory,
        .BINARY => a.dupe(u8, raw) catch return error.OutOfMemory,
        .BCD => blk: {
            const out = a.alloc(u8, raw.len * 2) catch return error.OutOfMemory;
            _ = enc.bcdDecode(raw, out) catch return error.OutOfMemory;
            break :blk out;
        },
        .EBCDIC => blk: {
            const out = a.alloc(u8, raw.len) catch return error.OutOfMemory;
            enc.decodeEbcdic(raw, out);
            break :blk out;
        },
    };
}

fn schemaWriteField(
    buf:   *std.ArrayList(u8),
    val:   []const u8,
    fdef:  *const FieldDef,
    alloc: std.mem.Allocator,
) !void {
    switch (fdef.field_type) {
        .FIXED => try schemaWriteEncoded(buf, val, fdef.encoding, alloc),
        .LLVAR => {
            var lb: [2]u8 = undefined;
            writeLen2(&lb, val.len); // logical digit/byte count
            try buf.appendSlice(alloc, &lb);
            try schemaWriteEncoded(buf, val, fdef.encoding, alloc);
        },
        .LLLVAR, .TLV => {
            var lb: [3]u8 = undefined;
            writeLen3(&lb, val.len);
            try buf.appendSlice(alloc, &lb);
            try schemaWriteEncoded(buf, val, fdef.encoding, alloc);
        },
    }
}

fn schemaWriteEncoded(
    buf:      *std.ArrayList(u8),
    val:      []const u8,
    enc_type: Encoding,
    alloc:    std.mem.Allocator,
) !void {
    switch (enc_type) {
        .ASCII, .BINARY => try buf.appendSlice(alloc, val),
        .BCD => {
            var pad_buf: [42]u8 = undefined;
            const padded = enc.padToEven(val, &pad_buf);
            const bcd_buf = try alloc.alloc(u8, padded.len / 2);
            defer alloc.free(bcd_buf);
            _ = enc.bcdEncode(padded, bcd_buf) catch return error.OutOfMemory;
            try buf.appendSlice(alloc, bcd_buf);
        },
        .EBCDIC => {
            const out = try alloc.alloc(u8, val.len);
            defer alloc.free(out);
            enc.encodeEbcdic(val, out);
            try buf.appendSlice(alloc, out);
        },
    }
}

fn asciiLen(bytes: []const u8) ParseError!usize {
    var len: usize = 0;
    for (bytes) |b| {
        if (b < '0' or b > '9') return error.InvalidLength;
        len = len * 10 + (b - '0');
    }
    return len;
}

fn writeLen2(buf: *[2]u8, n: usize) void {
    buf[0] = '0' + @as(u8, @intCast((n / 10) % 10));
    buf[1] = '0' + @as(u8, @intCast(n % 10));
}

fn writeLen3(buf: *[3]u8, n: usize) void {
    buf[0] = '0' + @as(u8, @intCast((n / 100) % 10));
    buf[1] = '0' + @as(u8, @intCast((n / 10) % 10));
    buf[2] = '0' + @as(u8, @intCast(n % 10));
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "parse: 0200 debit request" {
    const alloc = std.testing.allocator;

    // Build raw message bytes manually:
    // MTI "0200" + bitmap (fields 2,3,4,11,49) + fields
    //
    // Bitmap: F2→byte0 bit6(0x40), F3→0x20, F4→0x10, F11→byte1 bit5(0x20), F49→byte6 bit7(0x80)
    // byte0=0x70, byte1=0x20, bytes2-5=0x00, byte6=0x80, byte7=0x00
    const raw = "0200" ++
        "\x70\x20\x00\x00\x00\x00\x80\x00" ++ // bitmap
        "164111111111111111" ++ // F2: LLVAR PAN (len=16)
        "000000" ++ // F3: processing code
        "000000010000" ++ // F4: amount
        "000001" ++ // F11: STAN
        "952"; // F49: currency XOF

    var msg = try parse(raw, alloc);
    defer msg.deinit();

    try std.testing.expectEqualSlices(u8, "0200", &msg.mti);
    try std.testing.expectEqualStrings("4111111111111111", msg.get(2).?);
    try std.testing.expectEqualStrings("000000", msg.get(3).?);
    try std.testing.expectEqualStrings("000000010000", msg.get(4).?);
    try std.testing.expectEqualStrings("000001", msg.get(11).?);
    try std.testing.expectEqualStrings("952", msg.get(49).?);
    try std.testing.expect(msg.get(39) == null);
}

test "serialize/parse round-trip" {
    const alloc = std.testing.allocator;

    var msg = IsoMessage.init(alloc, "0210".*);
    defer msg.deinit();

    try msg.set(39, "00");
    try msg.set(11, "000042");
    try msg.set(49, "840");

    const bytes = try serialize(&msg, alloc);
    defer alloc.free(bytes);

    var parsed = try parse(bytes, alloc);
    defer parsed.deinit();

    try std.testing.expectEqualSlices(u8, "0210", &parsed.mti);
    try std.testing.expectEqualStrings("00", parsed.get(39).?);
    try std.testing.expectEqualStrings("000042", parsed.get(11).?);
    try std.testing.expectEqualStrings("840", parsed.get(49).?);
}

test "parse: secondary bitmap with field 102" {
    const alloc = std.testing.allocator;

    // Build a message with field 102 (secondary bitmap required)
    // Bitmap primary: bit 1 set (0x80) → secondary present
    // Bitmap secondary: field 102 → byte 4 (0-indexed in secondary), bit 1
    // Field 102: (102-1)=101, 101/8=12, 101%8=5, bit_shift=7-5=2 → byte 12, bit 2 = 0x04
    // byte12 of the full 16-byte bitmap = secondary byte 4
    var primary: [8]u8 = [_]u8{0} ** 8;
    primary[0] = 0x80; // bit 1 set
    var secondary: [8]u8 = [_]u8{0} ** 8;
    secondary[4] = 0x04; // field 102

    var raw_buf: [4 + 16 + 3 + 10]u8 = undefined;
    @memcpy(raw_buf[0..4], "0200");
    @memcpy(raw_buf[4..12], &primary);
    @memcpy(raw_buf[12..20], &secondary);
    // F102: LLVAR, val = "1234567890" (len 10)
    raw_buf[20] = '1';
    raw_buf[21] = '0';
    @memcpy(raw_buf[22..32], "1234567890");

    var msg = try parse(&raw_buf, alloc);
    defer msg.deinit();

    try std.testing.expectEqualStrings("1234567890", msg.get(102).?);
}

test "parseWithSchema: BCD amount decoded to ASCII digits" {
    const alloc = std.testing.allocator;

    // Schema: field 4 FIXED BCD length=12 (6 BCD bytes on wire)
    const schema = IsoSchema{
        .id = "test", .version = "1.0",
        .header_type = .none,
        .default_encoding = .ASCII, .mti_encoding = .ASCII,
        .fields = &.{
            .{ .id = 4, .name = "Amount", .field_type = .FIXED, .encoding = .BCD, .length = 12 },
            .{ .id = 11, .name = "STAN",  .field_type = .FIXED, .encoding = .ASCII, .length = 6 },
        },
        .alloc = undefined,
    };

    // MTI "0200" + bitmap (fields 4 and 11) + BCD amount 000000010000 + ASCII STAN
    // bitmap: field4=bit4→byte0 0x10, field11=bit11→byte1 0x20 → [0x10,0x20,0,0,0,0,0,0]
    const bcd_amount = [_]u8{ 0x00, 0x00, 0x00, 0x01, 0x00, 0x00 }; // 000000010000
    const stan = "000001";
    var raw: [4 + 8 + 6 + 6]u8 = undefined;
    @memcpy(raw[0..4], "0200");
    @memset(raw[4..12], 0);
    raw[4] = 0x10; // field 4
    raw[5] = 0x20; // field 11
    @memcpy(raw[12..18], &bcd_amount);
    @memcpy(raw[18..24], stan);

    var msg = try parseWithSchema(&raw, &schema, alloc);
    defer msg.deinit();

    try std.testing.expectEqualStrings("000000010000", msg.get(4).?);
    try std.testing.expectEqualStrings("000001", msg.get(11).?);
}

test "serializeWithSchema / parseWithSchema round-trip BCD" {
    const alloc = std.testing.allocator;

    const schema = IsoSchema{
        .id = "test", .version = "1.0",
        .header_type = .none,
        .default_encoding = .ASCII, .mti_encoding = .ASCII,
        .fields = &.{
            .{ .id = 4,  .name = "Amount",   .field_type = .FIXED, .encoding = .BCD,   .length = 12 },
            .{ .id = 11, .name = "STAN",     .field_type = .FIXED, .encoding = .ASCII,  .length = 6  },
            .{ .id = 49, .name = "Currency", .field_type = .FIXED, .encoding = .ASCII,  .length = 3  },
        },
        .alloc = undefined,
    };

    var msg = IsoMessage.init(alloc, "0200".*);
    defer msg.deinit();
    try msg.set(4, "000000025000");
    try msg.set(11, "000099");
    try msg.set(49, "XAF");

    const wire = try serializeWithSchema(&msg, &schema, alloc);
    defer alloc.free(wire);

    var parsed = try parseWithSchema(wire, &schema, alloc);
    defer parsed.deinit();

    try std.testing.expectEqualSlices(u8, "0200", &parsed.mti);
    try std.testing.expectEqualStrings("000000025000", parsed.get(4).?);
    try std.testing.expectEqualStrings("000099", parsed.get(11).?);
    try std.testing.expectEqualStrings("XAF", parsed.get(49).?);
}
