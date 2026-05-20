const std = @import("std");
const orusshare = @import("orusshare");
const parser = @import("iso8583/parser.zig");

pub const IsoMessage = parser.IsoMessage;
pub const InternalMessage = orusshare.InternalMessage;

pub const TranslateError = error{
    MissingAmount,
    InvalidAmount,
    MissingCurrency,
    OutOfMemory,
};

// InternalMessage → IsoMessage
// Populates well-known ISO fields from top-level InternalMessage fields,
// then copies every FieldEntry verbatim.
pub fn fromInternal(
    msg: *const InternalMessage,
    alloc: std.mem.Allocator,
) TranslateError!IsoMessage {
    var iso = IsoMessage.init(alloc, msg.mti);
    errdefer iso.deinit();

    // Field 4 — amount (12-digit ASCII, smallest currency unit, always positive)
    var amt_buf: [12]u8 = undefined;
    _ = std.fmt.bufPrint(&amt_buf, "{d:0>12}", .{@as(u64, @intCast(msg.amount))}) catch
        return error.InvalidAmount;
    iso.set(4, &amt_buf) catch return error.OutOfMemory;

    // Field 11 — STAN
    iso.set(11, &msg.stan) catch return error.OutOfMemory;

    // Field 49 — currency code
    iso.set(49, &msg.currency) catch return error.OutOfMemory;

    // Remaining fields from the adapter (PAN, processing code, etc.)
    for (msg.fields) |entry| {
        // Skip fields already written above; adapters may also include them.
        if (entry.id == 4 or entry.id == 11 or entry.id == 49) continue;
        iso.set(entry.id, entry.value) catch return error.OutOfMemory;
    }

    return iso;
}

// IsoMessage → InternalMessage
// Extracts well-known fields into InternalMessage top-level slots.
// `base` provides identity/routing fields that are not in ISO 8583
// (msg_id, topic, origin, provider, schema_id, source_ip, external_id).
pub fn toInternal(
    iso: *const IsoMessage,
    base: InternalMessage,
    alloc: std.mem.Allocator,
) TranslateError!InternalMessage {
    var msg = base;
    msg.mti = iso.mti;

    // Field 4 → amount
    if (iso.get(4)) |amt_str| {
        msg.amount = std.fmt.parseInt(i64, amt_str, 10) catch
            return error.InvalidAmount;
    }

    // Field 11 → STAN
    if (iso.get(11)) |stan| {
        if (stan.len == 6) @memcpy(&msg.stan, stan);
    }

    // Field 49 → currency
    if (iso.get(49)) |cur| {
        if (cur.len == 3) @memcpy(&msg.currency, cur);
    }

    // Copy all present ISO fields into InternalMessage.fields
    var entries: std.ArrayList(InternalMessage.FieldEntry) = .empty;
    errdefer entries.deinit(alloc);
    for (2..129) |n| {
        const val = iso.get(@intCast(n)) orelse continue;
        if (n == 2) {
            // Field 2 (PAN) — hash into pan_hash, never store raw (CDC §6, PCI-DSS Req 3.4).
            msg.pan_hash = orusshare.hash.hashPan(val);
            continue;
        }
        const duped = try alloc.dupe(u8, val);
        entries.append(alloc, .{ .id = @intCast(n), .value = duped }) catch
            return error.OutOfMemory;
    }
    msg.fields = try entries.toOwnedSlice(alloc);

    return msg;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "fromInternal: top-level fields map to ISO fields" {
    const alloc = std.testing.allocator;

    const msg = InternalMessage{
        .msg_id = [_]u8{0xAB} ** 16,
        .schema_id = "gimac",
        .topic = "transactions.financial",
        .origin = .rest_json,
        .provider = .orange_money,
        .mti = "0200".*,
        .fields = &.{},
        .stan = "000042".*,
        .pan_hash = 0,
        .amount = 10000,
        .currency = "XAF".*,
        .received_at = 0,
        .source_ip = [_]u8{0} ** 16,
        .hop_count = 1,
        .external_id = null,
    };

    var iso = try fromInternal(&msg, alloc);
    defer iso.deinit();

    try std.testing.expectEqualSlices(u8, "0200", &iso.mti);
    try std.testing.expectEqualStrings("000000010000", iso.get(4).?);
    try std.testing.expectEqualStrings("000042", iso.get(11).?);
    try std.testing.expectEqualStrings("XAF", iso.get(49).?);
}

test "fromInternal: adapter fields are forwarded" {
    const alloc = std.testing.allocator;

    var fields = [_]InternalMessage.FieldEntry{
        .{ .id = 2, .value = "4111111111111111" },
        .{ .id = 3, .value = "000000" },
    };

    const msg = InternalMessage{
        .msg_id = [_]u8{1} ** 16,
        .schema_id = "gimac",
        .topic = "transactions.financial",
        .origin = .rest_json,
        .provider = .mtn_momo,
        .mti = "0200".*,
        .fields = &fields,
        .stan = "000001".*,
        .pan_hash = 0,
        .amount = 5000,
        .currency = "XOF".*,
        .received_at = 0,
        .source_ip = [_]u8{0} ** 16,
        .hop_count = 1,
        .external_id = null,
    };

    var iso = try fromInternal(&msg, alloc);
    defer iso.deinit();

    try std.testing.expectEqualStrings("4111111111111111", iso.get(2).?);
    try std.testing.expectEqualStrings("000000", iso.get(3).?);
}

test "toInternal: ISO fields populate InternalMessage" {
    const alloc = std.testing.allocator;

    var iso = IsoMessage.init(alloc, "0210".*);
    defer iso.deinit();
    try iso.set(4, "000000005000");
    try iso.set(11, "000007");
    try iso.set(49, "XOF");
    try iso.set(39, "00");

    const base = InternalMessage{
        .msg_id = [_]u8{2} ** 16,
        .schema_id = "gimac",
        .topic = "transactions.financial",
        .origin = .iso8583,
        .provider = .none,
        .mti = "0000".*,
        .fields = &.{},
        .stan = [_]u8{0} ** 6,
        .pan_hash = 0,
        .amount = 0,
        .currency = [_]u8{0} ** 3,
        .received_at = 0,
        .source_ip = [_]u8{0} ** 16,
        .hop_count = 0,
        .external_id = null,
    };

    const result = try toInternal(&iso, base, alloc);
    defer {
        for (result.fields) |f| alloc.free(f.value);
        alloc.free(result.fields);
    }

    try std.testing.expectEqualSlices(u8, "0210", &result.mti);
    try std.testing.expectEqual(@as(i64, 5000), result.amount);
    try std.testing.expectEqualSlices(u8, "000007", &result.stan);
    try std.testing.expectEqualSlices(u8, "XOF", &result.currency);
    try std.testing.expectEqual(@as(usize, 4), result.fields.len); // fields 4,11,49,39
}

test "round-trip: fromInternal then toInternal" {
    const alloc = std.testing.allocator;

    var adapter_fields = [_]InternalMessage.FieldEntry{
        .{ .id = 3, .value = "300000" },
    };

    const original = InternalMessage{
        .msg_id = [_]u8{3} ** 16,
        .schema_id = "gimac",
        .topic = "transactions.financial",
        .origin = .rest_json,
        .provider = .wave,
        .mti = "0200".*,
        .fields = &adapter_fields,
        .stan = "001234".*,
        .pan_hash = 0xdeadbeef,
        .amount = 25000,
        .currency = "XAF".*,
        .received_at = 1_700_000_000,
        .source_ip = [_]u8{0} ** 16,
        .hop_count = 1,
        .external_id = null,
    };

    var iso = try fromInternal(&original, alloc);
    defer iso.deinit();

    const restored = try toInternal(&iso, original, alloc);
    defer {
        for (restored.fields) |f| alloc.free(f.value);
        alloc.free(restored.fields);
    }

    try std.testing.expectEqualSlices(u8, "0200", &restored.mti);
    try std.testing.expectEqual(@as(i64, 25000), restored.amount);
    try std.testing.expectEqualSlices(u8, "001234", &restored.stan);
    try std.testing.expectEqualSlices(u8, "XAF", &restored.currency);
}
