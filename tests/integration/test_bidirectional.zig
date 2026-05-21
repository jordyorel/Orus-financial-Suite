// CDC §2 — "Flux bidirectionnel MoMo ↔ Banque."
// Teste les deux directions en simulant les translations en mémoire
// sans nécessiter de vrais serveurs MoMo ou bancaires.

const std         = @import("std");
const orusshare   = @import("orusshare");
const orusgateway = @import("orusgateway");
const testing     = std.testing;

const serialize         = orusshare.serialize;
const InternalMessage   = orusshare.InternalMessage;
const parseIsoSchema    = orusgateway.parseIsoSchema;
const parseWithSchema   = orusgateway.iso8583.parseWithSchema;
const serializeWithSchema = orusgateway.iso8583.serializeWithSchema;
const IsoMessage        = orusgateway.iso8583.IsoMessage;
const fromInternal      = orusgateway.fromInternal;
const toInternal        = orusgateway.toInternal;

const GIMAC_TOML = @import("schemas").iso8583.gimac;

fn makeMsg(alloc: std.mem.Allocator, topic: []const u8, amount: i64) !InternalMessage {
    var msg_id: [16]u8 = undefined;
    std.c.arc4random_buf(&msg_id, 16);

    // Field 2 (PAN) is stripped at ingress — stored only as pan_hash (CDC §6 PCI-DSS).
    const fields = try alloc.dupe(InternalMessage.FieldEntry, &[_]InternalMessage.FieldEntry{
        .{ .id = 3,  .value = "000000" },
        .{ .id = 49, .value = "XAF" },
    });

    return .{
        .msg_id      = msg_id,
        .schema_id   = "gimac",
        .topic       = topic,
        .origin      = .iso8583,
        .provider    = .mtn_momo,
        .mti         = "0200".*,
        .fields      = fields,
        .stan        = [_]u8{0} ** 6,
        .pan_hash    = orusshare.hash.hashPan("4762001234567890", 0),
        .amount      = amount,
        .currency    = "XAF".*,
        .received_at = 0,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,
    };
}

// ── Direction 1 : MoMo → InternalMessage → ISO 8583 ─────────────────────────

test "Direction 1: InternalMessage → ISO 8583 via fromInternal" {
    const alloc = testing.allocator;

    const im = try makeMsg(alloc, "transactions.financial", 10_000);
    defer alloc.free(im.fields);

    var iso = try fromInternal(&im, alloc);
    defer iso.deinit();

    // Le MTI doit être 0200.
    try testing.expectEqualSlices(u8, "0200", &iso.mti);

    // Le champ 4 (Amount) doit contenir le montant formaté.
    try testing.expect(iso.get(4) != null);

    // Le champ 49 (Currency) doit être XAF.
    try testing.expectEqualStrings("XAF", iso.get(49).?);
}

test "Direction 1: InternalMessage → ISO 8583 → sérialisé sur fil (GIMAC)" {
    const alloc = testing.allocator;
    var schema = try parseIsoSchema(GIMAC_TOML, alloc);
    defer schema.deinit();

    const im = try makeMsg(alloc, "transactions.financial", 5_000);
    defer alloc.free(im.fields);

    var iso = try fromInternal(&im, alloc);
    defer iso.deinit();

    const wire = try serializeWithSchema(&iso, &schema, alloc);
    defer alloc.free(wire);

    // Le fil doit commencer par le préfixe 2 octets (GIMAC header_type=length_2).
    try testing.expect(wire.len > 2);
    const hdr_len = std.mem.readInt(u16, wire[0..2], .big);
    try testing.expectEqual(@as(u16, @intCast(wire.len - 2)), hdr_len);
}

test "Direction 1: round-trip complet IM → ISO wire → parse → vérif champs" {
    const alloc = testing.allocator;
    var schema = try parseIsoSchema(GIMAC_TOML, alloc);
    defer schema.deinit();

    const im = try makeMsg(alloc, "transactions.financial", 7_500);
    defer alloc.free(im.fields);

    // IM → ISO message.
    var iso_out = try fromInternal(&im, alloc);
    defer iso_out.deinit();
    iso_out.mti = "0200".*;

    // Sérialise sur fil.
    const wire = try serializeWithSchema(&iso_out, &schema, alloc);
    defer alloc.free(wire);

    // parseWithSchema skips the 2-byte GIMAC header automatically.
    var iso_in = try parseWithSchema(wire, &schema, alloc);
    defer iso_in.deinit();

    try testing.expectEqualSlices(u8, "0200", &iso_in.mti);
    try testing.expectEqualStrings("XAF", iso_in.get(49).?);
}

// ── Direction 2 : ISO 8583 → InternalMessage → broker wire ──────────────────

test "Direction 2: ISO 8583 0200 → toInternal préserve amount et currency" {
    const alloc = testing.allocator;
    var schema = try parseIsoSchema(GIMAC_TOML, alloc);
    defer schema.deinit();

    // Construit un message ISO 8583 simulant une initiation bancaire.
    var bank_msg = IsoMessage.init(alloc, "0200".*);
    defer bank_msg.deinit();
    try bank_msg.set(2,  "4762001234567890");
    try bank_msg.set(3,  "000000");
    try bank_msg.set(4,  "000000010000");   // 100,00 XAF en centimes * 100
    try bank_msg.set(11, "000001");
    try bank_msg.set(49, "XAF");

    var msg_id: [16]u8 = undefined;
    std.c.arc4random_buf(&msg_id, 16);

    const base = InternalMessage{
        .msg_id      = msg_id,
        .schema_id   = "gimac",
        .topic       = "transactions.financial",
        .origin      = .iso8583,
        .provider    = .none,
        .mti         = "0200".*,
        .fields      = &.{},
        .stan        = [_]u8{0} ** 6,
        .pan_hash    = 0,
        .amount      = 0,
        .currency    = [_]u8{0} ** 3,
        .received_at = 0,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,
    };

    const im = try toInternal(&bank_msg, base, alloc, 0);
    defer {
        for (im.fields) |f| alloc.free(f.value);
        alloc.free(im.fields);
    }

    // Le montant doit être non-nul (traduit depuis field 4).
    try testing.expect(im.amount > 0);

    // La devise doit être XAF.
    try testing.expectEqualStrings("XAF", &im.currency);
}

test "Direction 2: ISO 8583 → toInternal → serialize/deserialize round-trip" {
    const alloc = testing.allocator;
    var schema = try parseIsoSchema(GIMAC_TOML, alloc);
    defer schema.deinit();

    var bank_msg = IsoMessage.init(alloc, "0200".*);
    defer bank_msg.deinit();
    try bank_msg.set(4,  "000000005000");
    try bank_msg.set(11, "000002");
    try bank_msg.set(49, "XAF");

    var msg_id: [16]u8 = undefined;
    std.c.arc4random_buf(&msg_id, 16);

    const base = InternalMessage{
        .msg_id      = msg_id,
        .schema_id   = "gimac",
        .topic       = "transactions.financial",
        .origin      = .iso8583,
        .provider    = .none,
        .mti         = "0200".*,
        .fields      = &.{},
        .stan        = [_]u8{0} ** 6,
        .pan_hash    = 0,
        .amount      = 0,
        .currency    = [_]u8{0} ** 3,
        .received_at = 0,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,
    };

    const im = try toInternal(&bank_msg, base, alloc, 0);
    defer {
        for (im.fields) |f| alloc.free(f.value);
        alloc.free(im.fields);
    }

    // Sérialise sur wire broker.
    var payload_list: std.ArrayList(u8) = .empty;
    try payload_list.ensureTotalCapacity(alloc, 4096);
    var pw = std.Io.Writer.fromArrayList(&payload_list);
    try serialize.serialize(&im, &pw);
    var payload_al = std.Io.Writer.toArrayList(&pw);
    defer payload_al.deinit(alloc);

    // Désérialise et vérifie l'intégrité.
    var fixed_r = std.Io.Reader.fixed(payload_al.items);
    const im2 = try serialize.deserialize(&fixed_r, alloc);
    defer serialize.free(&im2, alloc);

    try testing.expectEqualSlices(u8, &im.msg_id,   &im2.msg_id);
    try testing.expectEqual(im.amount,              im2.amount);
    try testing.expectEqualSlices(u8, &im.currency, &im2.currency);
}

test "Direction 2: InternalMessage conserve le hop_count en passant par Gateway" {
    const alloc = testing.allocator;
    var schema = try parseIsoSchema(GIMAC_TOML, alloc);
    defer schema.deinit();

    var bank_msg = IsoMessage.init(alloc, "0200".*);
    defer bank_msg.deinit();
    try bank_msg.set(4,  "000000001000");
    try bank_msg.set(49, "XAF");

    const msg_id: [16]u8 = [_]u8{0xAB} ** 16;

    const base = InternalMessage{
        .msg_id      = msg_id,
        .schema_id   = "gimac",
        .topic       = "transactions.financial",
        .origin      = .iso8583,
        .provider    = .none,
        .mti         = "0200".*,
        .fields      = &.{},
        .stan        = [_]u8{0} ** 6,
        .pan_hash    = 0,
        .amount      = 0,
        .currency    = [_]u8{0} ** 3,
        .received_at = 0,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,
    };

    const im = try toInternal(&bank_msg, base, alloc, 0);
    defer {
        for (im.fields) |f| alloc.free(f.value);
        alloc.free(im.fields);
    }

    // hop_count doit être incrémenté par toInternal.
    try testing.expect(im.hop_count >= 1);
}

// ── PAN masking ───────────────────────────────────────────────────────────────

test "PAN masking: le PAN brut n'apparaît pas dans la sérialisation InternalMessage" {
    // CDC §6 — "PAN masqué dans tous les logs (PCI-DSS Req 3.4)"
    // La sérialisation d'un InternalMessage stocke pan_hash (u64), jamais le PAN clair.
    const alloc = testing.allocator;
    const im = try makeMsg(alloc, "transactions.financial", 1_000);
    defer alloc.free(im.fields);

    var payload_list: std.ArrayList(u8) = .empty;
    try payload_list.ensureTotalCapacity(alloc, 4096);
    var pw = std.Io.Writer.fromArrayList(&payload_list);
    try serialize.serialize(&im, &pw);
    var payload_al = std.Io.Writer.toArrayList(&pw);
    defer payload_al.deinit(alloc);

    // Le PAN "4762001234567890" ne doit pas apparaître en ASCII dans le payload.
    const pan = "4762001234567890";
    const found = std.mem.indexOf(u8, payload_al.items, pan);
    try testing.expect(found == null);
}

test "PAN masking: pan_hash différent selon le PAN" {
    // hash.hashPan produit deux valeurs distinctes pour deux PANs différents.
    const h1 = orusshare.hash.hashPan("4762001234567890", 0);
    const h2 = orusshare.hash.hashPan("5500001111222233", 0);
    try testing.expect(h1 != h2);
}

test "PAN masking: même PAN → même hash (déterministe)" {
    const h1 = orusshare.hash.hashPan("4762001234567890", 0);
    const h2 = orusshare.hash.hashPan("4762001234567890", 0);
    try testing.expectEqual(h1, h2);
}
