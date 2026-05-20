const std = @import("std");

pub const TlvError = error{
    BufferTooShort,
    TlvMaxDepthExceeded,
    InvalidLength,
    OutOfMemory,
};

pub const DEFAULT_MAX_DEPTH: u8 = 4;

pub const TlvField = struct {
    tag:         u32,
    value:       []const u8, // raw value bytes (owned by alloc)
    constructed: bool,
    children:    []TlvField, // non-empty iff constructed (owned by alloc)
};

// Parse BER-TLV data from `buf`.
// Caller passes alloc; if alloc is an arena, cleanup on error is automatic.
// On success, caller owns the returned slice and must call freeTlvFields().
pub fn parseTlv(
    buf:       []const u8,
    depth:     u8,
    max_depth: u8,
    alloc:     std.mem.Allocator,
) TlvError![]TlvField {
    if (depth > max_depth) return error.TlvMaxDepthExceeded;

    var list: std.ArrayList(TlvField) = .empty;
    errdefer {
        for (list.items) |*f| freeTlvField(f, alloc);
        list.deinit(alloc);
    }

    var pos: usize = 0;
    while (pos < buf.len) {
        // ── Tag ────────────────────────────────────────────────────────────────
        if (pos >= buf.len) break;
        const first = buf[pos];
        pos += 1;
        const constructed = (first & 0x20) != 0;

        var tag: u32 = first;
        if ((first & 0x1F) == 0x1F) {
            // Multi-byte tag: keep reading while bit 7 is set.
            while (pos < buf.len) {
                const b = buf[pos];
                pos += 1;
                tag = (tag << 8) | b;
                if ((b & 0x80) == 0) break;
            }
        }

        // ── Length ─────────────────────────────────────────────────────────────
        if (pos >= buf.len) return error.BufferTooShort;
        const len_byte = buf[pos];
        pos += 1;

        const value_len: usize = if (len_byte < 0x80) blk: {
            break :blk len_byte;
        } else blk: {
            const num = len_byte & 0x7F;
            if (num == 0 or num > 4) return error.InvalidLength;
            if (pos + num > buf.len) return error.BufferTooShort;
            var l: usize = 0;
            for (0..num) |_| {
                l = (l << 8) | buf[pos];
                pos += 1;
            }
            break :blk l;
        };

        if (pos + value_len > buf.len) return error.BufferTooShort;
        const raw_value = buf[pos .. pos + value_len];
        pos += value_len;

        // ── Recurse if constructed ─────────────────────────────────────────────
        var children: []TlvField = &.{};
        if (constructed) {
            children = parseTlv(raw_value, depth + 1, max_depth, alloc) catch |err| switch (err) {
                error.TlvMaxDepthExceeded => return error.TlvMaxDepthExceeded,
                else => return err,
            };
        }

        const value_copy = alloc.dupe(u8, raw_value) catch {
            for (children) |*c| freeTlvField(c, alloc);
            alloc.free(children);
            return error.OutOfMemory;
        };

        list.append(alloc, .{
            .tag = tag,
            .value = value_copy,
            .constructed = constructed,
            .children = children,
        }) catch {
            alloc.free(value_copy);
            for (children) |*c| freeTlvField(c, alloc);
            alloc.free(children);
            return error.OutOfMemory;
        };
    }

    return list.toOwnedSlice(alloc);
}

pub fn freeTlvField(field: *TlvField, alloc: std.mem.Allocator) void {
    alloc.free(field.value);
    for (field.children) |*child| freeTlvField(child, alloc);
    alloc.free(field.children);
}

pub fn freeTlvFields(fields: []TlvField, alloc: std.mem.Allocator) void {
    for (fields) |*f| freeTlvField(f, alloc);
    alloc.free(fields);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "TLV: primitive tag 1-byte" {
    const alloc = std.testing.allocator;
    // Tag 0x9A (LocalDate), len 3, value 3 bytes
    const buf = [_]u8{ 0x9A, 0x03, 0x24, 0x01, 0x15 };
    const fields = try parseTlv(&buf, 0, DEFAULT_MAX_DEPTH, alloc);
    defer freeTlvFields(fields, alloc);

    try std.testing.expectEqual(@as(usize, 1), fields.len);
    try std.testing.expectEqual(@as(u32, 0x9A), fields[0].tag);
    try std.testing.expectEqual(@as(usize, 3), fields[0].value.len);
    try std.testing.expect(!fields[0].constructed);
}

test "TLV: multi-byte tag 9F26" {
    const alloc = std.testing.allocator;
    // Tag 0x9F26 (Application Cryptogram), len 8
    const buf = [_]u8{ 0x9F, 0x26, 0x08 } ++ [_]u8{0xAB} ** 8;
    const fields = try parseTlv(&buf, 0, DEFAULT_MAX_DEPTH, alloc);
    defer freeTlvFields(fields, alloc);

    try std.testing.expectEqual(@as(usize, 1), fields.len);
    try std.testing.expectEqual(@as(u32, 0x9F26), fields[0].tag);
    try std.testing.expectEqual(@as(usize, 8), fields[0].value.len);
}

test "TLV: constructed tag with nested primitive" {
    const alloc = std.testing.allocator;
    // 0x70 (constructed) containing 0x9F02 (amount), len 6, value 6 bytes
    const inner = [_]u8{ 0x9F, 0x02, 0x06, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00 };
    var buf: [2 + inner.len]u8 = undefined;
    buf[0] = 0x70; // constructed (bit 5 set)
    buf[1] = @intCast(inner.len);
    @memcpy(buf[2..], &inner);

    const fields = try parseTlv(&buf, 0, DEFAULT_MAX_DEPTH, alloc);
    defer freeTlvFields(fields, alloc);

    try std.testing.expectEqual(@as(usize, 1), fields.len);
    try std.testing.expect(fields[0].constructed);
    try std.testing.expectEqual(@as(usize, 1), fields[0].children.len);
    try std.testing.expectEqual(@as(u32, 0x9F02), fields[0].children[0].tag);
}

test "TLV: max depth exceeded" {
    const alloc = std.testing.allocator;
    // constructed inside constructed — max_depth = 0 means no recursion allowed
    const inner = [_]u8{ 0x9A, 0x01, 0xFF };
    var buf: [2 + 2 + inner.len]u8 = undefined;
    buf[0] = 0x70; // outer constructed
    buf[1] = @intCast(2 + inner.len);
    buf[2] = 0x70; // inner constructed
    buf[3] = @intCast(inner.len);
    @memcpy(buf[4..], &inner);

    const result = parseTlv(&buf, 0, 0, alloc);
    try std.testing.expectError(error.TlvMaxDepthExceeded, result);
}

test "TLV: multiple primitives in sequence" {
    const alloc = std.testing.allocator;
    // 0x9A len=1 val=0x01 | 0x9F02 len=2 val=0x00 0xFF
    const buf = [_]u8{ 0x9A, 0x01, 0x01, 0x9F, 0x02, 0x02, 0x00, 0xFF };
    const fields = try parseTlv(&buf, 0, DEFAULT_MAX_DEPTH, alloc);
    defer freeTlvFields(fields, alloc);

    try std.testing.expectEqual(@as(usize, 2), fields.len);
    try std.testing.expectEqual(@as(u32, 0x9A), fields[0].tag);
    try std.testing.expectEqual(@as(u32, 0x9F02), fields[1].tag);
}
