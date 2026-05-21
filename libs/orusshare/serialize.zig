const std = @import("std");
const message = @import("message.zig");
const InternalMessage = message.InternalMessage;
const MessageOrigin = message.MessageOrigin;
const ServiceProvider = message.ServiceProvider;

// Wire format (big-endian):
//   [16]    msg_id
//   [1+N]   schema_id  (1 byte len, N bytes)
//   [2+N]   topic      (2 bytes len, N bytes)
//   [1]     origin     (enum u8)
//   [1]     provider   (enum u8)
//   [4]     mti
//   [6]     stan
//   [8]     pan_hash   (u64)
//   [8]     amount     (i64)
//   [3]     currency
//   [8]     received_at (i64 nanoseconds)
//   [16]    source_ip
//   [1]     hop_count
//   [2+N]   external_id (2 bytes len; 0x0000 if absent)
//   [2]     field_count
//   per field: [1] id  [2] value_len  [value_len] value

pub const SerializeError = error{
    SchemaIdTooLong,
    TopicTooLong,
    ExternalIdTooLong,
    TooManyFields,
    FieldValueTooLong,
};

pub const DeserializeError = error{
    InvalidOrigin,
    InvalidProvider,
};

pub fn serialize(msg: *const InternalMessage, w: *std.Io.Writer) (std.Io.Writer.Error || SerializeError)!void {
    if (msg.schema_id.len > 255) return error.SchemaIdTooLong;
    if (msg.topic.len > 65535) return error.TopicTooLong;
    if (msg.fields.len > 65535) return error.TooManyFields;
    if (msg.external_id) |eid| if (eid.len > 65535) return error.ExternalIdTooLong;

    try w.writeAll(&msg.msg_id);

    try w.writeByte(@intCast(msg.schema_id.len));
    try w.writeAll(msg.schema_id);

    try w.writeInt(u16, @intCast(msg.topic.len), .big);
    try w.writeAll(msg.topic);

    try w.writeByte(@intFromEnum(msg.origin));
    try w.writeByte(@intFromEnum(msg.provider));

    try w.writeAll(&msg.mti);
    try w.writeAll(&msg.stan);

    try w.writeInt(u64, msg.pan_hash, .big);
    try w.writeInt(i64, msg.amount, .big);
    try w.writeAll(&msg.currency);
    try w.writeInt(i64, msg.received_at, .big);
    try w.writeAll(&msg.source_ip);
    try w.writeByte(msg.hop_count);

    if (msg.external_id) |eid| {
        try w.writeInt(u16, @intCast(eid.len), .big);
        try w.writeAll(eid);
    } else {
        try w.writeInt(u16, 0, .big);
    }

    try w.writeInt(u16, @intCast(msg.fields.len), .big);
    for (msg.fields) |field| {
        if (field.value.len > 65535) return error.FieldValueTooLong;
        try w.writeByte(field.id);
        try w.writeInt(u16, @intCast(field.value.len), .big);
        try w.writeAll(field.value);
    }
}

pub fn deserialize(r: *std.Io.Reader, alloc: std.mem.Allocator) (std.Io.Reader.Error || DeserializeError || error{OutOfMemory})!InternalMessage {
    var msg: InternalMessage = undefined;

    try r.readSliceAll(&msg.msg_id);

    const schema_id_len = try r.takeByte();
    const schema_id = try alloc.alloc(u8, schema_id_len);
    errdefer alloc.free(schema_id);
    try r.readSliceAll(schema_id);
    msg.schema_id = schema_id;

    const topic_len = try r.takeInt(u16, .big);
    const topic = try alloc.alloc(u8, topic_len);
    errdefer alloc.free(topic);
    try r.readSliceAll(topic);
    msg.topic = topic;

    msg.origin = switch (try r.takeByte()) {
        0 => MessageOrigin.iso8583,
        1 => MessageOrigin.rest_json,
        2 => MessageOrigin.internal,
        else => return error.InvalidOrigin,
    };
    msg.provider = switch (try r.takeByte()) {
        0 => ServiceProvider.none,
        1 => ServiceProvider.orange_money,
        2 => ServiceProvider.mtn_momo,
        3 => ServiceProvider.wave,
        4 => ServiceProvider.airtel_money,
        5 => ServiceProvider.custom,
        else => return error.InvalidProvider,
    };

    try r.readSliceAll(&msg.mti);
    try r.readSliceAll(&msg.stan);

    msg.pan_hash = try r.takeInt(u64, .big);
    msg.amount = try r.takeInt(i64, .big);
    try r.readSliceAll(&msg.currency);
    msg.received_at = try r.takeInt(i64, .big);
    try r.readSliceAll(&msg.source_ip);
    msg.hop_count = try r.takeByte();

    const eid_len = try r.takeInt(u16, .big);
    if (eid_len > 0) {
        const eid = try alloc.alloc(u8, eid_len);
        errdefer alloc.free(eid);
        try r.readSliceAll(eid);
        msg.external_id = eid;
    } else {
        msg.external_id = null;
    }

    const field_count = try r.takeInt(u16, .big);
    const fields = try alloc.alloc(InternalMessage.FieldEntry, field_count);
    errdefer alloc.free(fields);
    var fields_done: usize = 0;
    errdefer for (fields[0..fields_done]) |f| alloc.free(f.value);
    for (fields) |*field| {
        field.id = try r.takeByte();
        const val_len = try r.takeInt(u16, .big);
        const val = try alloc.alloc(u8, val_len);
        errdefer alloc.free(val);
        try r.readSliceAll(val);
        field.value = val;
        fields_done += 1;
    }
    msg.fields = fields;

    return msg;
}

// Free all memory allocated by deserialize.
pub fn free(msg: *const InternalMessage, alloc: std.mem.Allocator) void {
    alloc.free(msg.schema_id);
    alloc.free(msg.topic);
    if (msg.external_id) |eid| alloc.free(@constCast(eid));
    for (msg.fields) |field| alloc.free(@constCast(field.value));
    alloc.free(msg.fields);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

fn makeTestMsg(fields: []InternalMessage.FieldEntry) InternalMessage {
    return .{
        .msg_id = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 },
        .schema_id = "gimac",
        .topic = "transactions.financial",
        .origin = .rest_json,
        .provider = .orange_money,
        .mti = "0200".*,
        .fields = fields,
        .stan = "123456".*,
        .pan_hash = 0xdeadbeefcafe1234,
        .amount = 5_000_000,
        .currency = "XAF".*,
        .received_at = 1_716_000_000_000_000_000,
        .source_ip = [_]u8{ 196, 217, 0, 1 } ++ [_]u8{0} ** 12,
        .hop_count = 1,
        .external_id = null,
    };
}

fn roundTrip(original: *const InternalMessage, alloc: std.mem.Allocator) !InternalMessage {
    var list: std.ArrayList(u8) = .empty;
    try list.ensureTotalCapacity(alloc, 1024);
    var w = std.Io.Writer.fromArrayList(&list);
    try serialize(original, &w);
    var written = std.Io.Writer.toArrayList(&w);
    defer written.deinit(alloc);
    var r = std.Io.Reader.fixed(written.items);
    return deserialize(&r, alloc);
}

test "round-trip: no fields, no external_id" {
    const alloc = std.testing.allocator;
    const original = makeTestMsg(&.{});
    const restored = try roundTrip(&original, alloc);
    defer free(&restored, alloc);

    try std.testing.expectEqual(original.msg_id, restored.msg_id);
    try std.testing.expectEqualStrings(original.schema_id, restored.schema_id);
    try std.testing.expectEqualStrings(original.topic, restored.topic);
    try std.testing.expectEqual(original.origin, restored.origin);
    try std.testing.expectEqual(original.provider, restored.provider);
    try std.testing.expectEqual(original.mti, restored.mti);
    try std.testing.expectEqual(original.stan, restored.stan);
    try std.testing.expectEqual(original.pan_hash, restored.pan_hash);
    try std.testing.expectEqual(original.amount, restored.amount);
    try std.testing.expectEqual(original.currency, restored.currency);
    try std.testing.expectEqual(original.received_at, restored.received_at);
    try std.testing.expectEqual(original.source_ip, restored.source_ip);
    try std.testing.expectEqual(original.hop_count, restored.hop_count);
    try std.testing.expectEqual(@as(usize, 0), restored.fields.len);
    try std.testing.expect(restored.external_id == null);
}

test "round-trip: with external_id" {
    const alloc = std.testing.allocator;
    var original = makeTestMsg(&.{});
    original.external_id = "ext-ref-abc123";
    const restored = try roundTrip(&original, alloc);
    defer free(&restored, alloc);
    try std.testing.expectEqualStrings(original.external_id.?, restored.external_id.?);
}

test "round-trip: with fields" {
    const alloc = std.testing.allocator;
    var fields = [_]InternalMessage.FieldEntry{
        .{ .id = 2, .value = "4762000000001234" },
        .{ .id = 4, .value = "000000050000" },
        .{ .id = 11, .value = "123456" },
    };
    const original = makeTestMsg(&fields);
    const restored = try roundTrip(&original, alloc);
    defer free(&restored, alloc);

    try std.testing.expectEqual(@as(usize, 3), restored.fields.len);
    try std.testing.expectEqual(@as(u8, 2), restored.fields[0].id);
    try std.testing.expectEqualStrings("4762000000001234", restored.fields[0].value);
    try std.testing.expectEqual(@as(u8, 4), restored.fields[1].id);
    try std.testing.expectEqualStrings("000000050000", restored.fields[1].value);
}

test "round-trip: all ServiceProvider variants" {
    const alloc = std.testing.allocator;
    const providers = [_]ServiceProvider{
        .none, .orange_money, .mtn_momo, .wave, .airtel_money, .custom,
    };
    for (providers) |p| {
        var original = makeTestMsg(&.{});
        original.provider = p;
        const restored = try roundTrip(&original, alloc);
        defer free(&restored, alloc);
        try std.testing.expectEqual(p, restored.provider);
    }
}

test "round-trip: all MessageOrigin variants" {
    const alloc = std.testing.allocator;
    const origins = [_]MessageOrigin{ .iso8583, .rest_json, .internal };
    for (origins) |o| {
        var original = makeTestMsg(&.{});
        original.origin = o;
        const restored = try roundTrip(&original, alloc);
        defer free(&restored, alloc);
        try std.testing.expectEqual(o, restored.origin);
    }
}

test "serialize: schema_id too long returns error" {
    const alloc = std.testing.allocator;
    var original = makeTestMsg(&.{});
    original.schema_id = "x" ** 256;
    var list: std.ArrayList(u8) = .empty;
    try list.ensureTotalCapacity(alloc, 512);
    var w = std.Io.Writer.fromArrayList(&list);
    const err = serialize(&original, &w);
    var leftover = std.Io.Writer.toArrayList(&w);
    leftover.deinit(alloc);
    try std.testing.expectError(error.SchemaIdTooLong, err);
}

test "round-trip: field with empty value" {
    const alloc = std.testing.allocator;
    var fields = [_]InternalMessage.FieldEntry{
        .{ .id = 3, .value = "" }, // valeur vide — cas limite
    };
    const original = makeTestMsg(&fields);
    const restored = try roundTrip(&original, alloc);
    defer free(&restored, alloc);
    try std.testing.expectEqual(@as(usize, 1), restored.fields.len);
    try std.testing.expectEqual(@as(u8, 3), restored.fields[0].id);
    try std.testing.expectEqual(@as(usize, 0), restored.fields[0].value.len);
}
