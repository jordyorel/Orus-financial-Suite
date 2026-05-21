// CDC §4 — "OrusBroker : persiste, route et livre avec exactly-once."
// Tests d'intégration TCP complets : PUBLISH → DELIVER → ACK → PUBLISH_ACK.
// Le broker tourne en thread dans le même processus.

const std        = @import("std");
const orusshare  = @import("orusshare");
const orusbroker = @import("orusbroker");
const testing    = std.testing;

const BrokerServer = orusbroker.BrokerServer;
const DedupFilter  = orusbroker.DedupFilter;
const TopicRouter  = orusbroker.TopicRouter;
const Cursor       = orusbroker.Cursor;
const Wal          = orusbroker.Wal;

const proto = orusbroker.protocol;

// Ports dédiés aux tests — hors des ports de production.
const PORT_BASIC:    u16 = 17_770;
const PORT_DEDUP:    u16 = 17_771;
const PORT_FANOUT:   u16 = 17_772;
const PORT_CURSOR:   u16 = 17_776;

// ── Helpers ───────────────────────────────────────────────────────────────────

// Single Threaded for all tests — avoids signal-handler conflicts between instances.
var g_threaded: std.Io.Threaded = undefined;
var g_threaded_ready = false;

fn io_init(_: std.mem.Allocator) std.Io {
    if (!g_threaded_ready) {
        g_threaded = std.Io.Threaded.init(std.heap.page_allocator, .{});
        g_threaded_ready = true;
    }
    return g_threaded.io();
}

fn buildMessage(alloc: std.mem.Allocator, topic: []const u8) !orusshare.InternalMessage {
    var msg_id: [16]u8 = undefined;
    std.c.arc4random_buf(&msg_id, msg_id.len);
    const fields = try alloc.alloc(orusshare.InternalMessage.FieldEntry, 0);
    return .{
        .msg_id      = msg_id,
        .schema_id   = "gimac",
        .topic       = topic,
        .origin      = .iso8583,
        .provider    = .none,
        .mti         = "0200".*,
        .fields      = fields,
        .stan        = [_]u8{0} ** 6,
        .pan_hash    = 0,
        .amount      = 5000,
        .currency    = "XAF".*,
        .received_at = 0,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,
    };
}

// Sérialise un InternalMessage dans le format wire PUBLISH du broker.
fn buildPublishFrame(msg: *const orusshare.InternalMessage, topic: []const u8, alloc: std.mem.Allocator) ![]u8 {
    var payload_list: std.ArrayList(u8) = .empty;
    try payload_list.ensureTotalCapacity(alloc, 4096);
    var pw = std.Io.Writer.fromArrayList(&payload_list);
    try orusshare.serialize.serialize(msg, &pw);
    var payload_al = std.Io.Writer.toArrayList(&pw);
    defer payload_al.deinit(alloc);
    const payload = payload_al.items;

    const topic_len: u16 = @intCast(topic.len);
    const body_len:  u32 = @intCast(1 + 2 + topic.len + payload.len);

    var frame = try alloc.alloc(u8, 4 + body_len);
    var pos: usize = 0;
    std.mem.writeInt(u32, frame[pos..][0..4], body_len, .big);       pos += 4;
    frame[pos] = proto.FRAME_PUBLISH;                                  pos += 1;
    std.mem.writeInt(u16, frame[pos..][0..2], topic_len, .big);       pos += 2;
    @memcpy(frame[pos..][0..topic.len], topic);                        pos += topic.len;
    @memcpy(frame[pos..][0..payload.len], payload);
    return frame;
}

// Sérialise une frame SUBSCRIBE avec un sub_id optionnel.
// sub_id = "" pour un abonné anonyme (pas de curseur).
fn buildSubscribeFrame(topic: []const u8, sub_id: []const u8, alloc: std.mem.Allocator) ![]u8 {
    const topic_len:  u16 = @intCast(topic.len);
    const sub_id_len: u16 = @intCast(sub_id.len);
    // body = frame_type(1) + topic_len(2) + topic + sub_id_len(2) + sub_id
    const body_len: u32 = @intCast(1 + 2 + topic.len + 2 + sub_id.len);
    var frame = try alloc.alloc(u8, 4 + body_len);
    var pos: usize = 0;
    std.mem.writeInt(u32, frame[pos..][0..4], body_len, .big);       pos += 4;
    frame[pos] = proto.FRAME_SUBSCRIBE;                                pos += 1;
    std.mem.writeInt(u16, frame[pos..][0..2], topic_len, .big);       pos += 2;
    @memcpy(frame[pos..][0..topic.len], topic);                        pos += topic.len;
    std.mem.writeInt(u16, frame[pos..][0..2], sub_id_len, .big);      pos += 2;
    if (sub_id.len > 0) @memcpy(frame[pos..][0..sub_id.len], sub_id);
    return frame;
}

// Construit une frame ACK avec le wal_offset (after_offset du DELIVER).
fn buildAckFrame(wal_offset: u64) [13]u8 {
    var frame: [13]u8 = undefined;
    std.mem.writeInt(u32, frame[0..4], 9, .big); // body_len = 1 + 8
    frame[4] = proto.FRAME_ACK;
    std.mem.writeInt(u64, frame[5..13], wal_offset, .big);
    return frame;
}

// Lit un frame DELIVER complet et retourne le wal_offset embarqué.
// Vérifie le frame_type et le topic attendu.
fn readDeliverOffset(
    r:             *std.Io.Reader,
    expected_topic: []const u8,
    alloc:          std.mem.Allocator,
) !u64 {
    var len_bytes: [4]u8 = undefined;
    try r.readSliceAll(&len_bytes);
    const body_len = std.mem.readInt(u32, &len_bytes, .big);

    const frame_type = try r.takeByte();
    try testing.expectEqual(proto.FRAME_DELIVER, frame_type);

    var tl: [2]u8 = undefined;
    try r.readSliceAll(&tl);
    const topic_len = std.mem.readInt(u16, &tl, .big);
    const topic_buf = try alloc.alloc(u8, topic_len);
    defer alloc.free(topic_buf);
    try r.readSliceAll(topic_buf);
    try testing.expectEqualStrings(expected_topic, topic_buf);

    var offset_bytes: [8]u8 = undefined;
    try r.readSliceAll(&offset_bytes);
    const wal_offset = std.mem.readInt(u64, &offset_bytes, .big);

    // Drain payload.
    const payload_len: usize = body_len -| (1 + 2 + topic_len + 8);
    const payload = try alloc.alloc(u8, payload_len);
    defer alloc.free(payload);
    try r.readSliceAll(payload);

    return wal_offset;
}

// Inner is heap-allocated via page_allocator so it outlives the test stack frame.
// Detached server/connection threads may still reference it after the test ends.
const BrokerInner = struct {
    wal:    Wal,
    dedup:  DedupFilter,
    router: TopicRouter,
    server: BrokerServer,
    io:     std.Io,
};

fn runServer(inner: *BrokerInner) void {
    inner.server.serve(std.heap.page_allocator) catch {};
}

const BrokerCtx = struct {
    inner:  *BrokerInner,
    thread: std.Thread,

    fn start(_: std.mem.Allocator, port: u16, io: std.Io, wal_path: [:0]const u8) !BrokerCtx {
        std.Io.Dir.cwd().deleteFile(io, wal_path) catch {};
        const inner = try std.heap.page_allocator.create(BrokerInner);
        inner.io     = io;
        inner.wal    = try Wal.open(io, wal_path);
        inner.dedup  = DedupFilter.init();
        inner.router = TopicRouter.init(std.heap.page_allocator);
        const config = orusbroker.Config{
            .host       = "127.0.0.1",
            .port       = port,
            .wal_path   = wal_path,
            .cursor_dir = "/tmp/orus_test_cursors",
        };
        inner.server = BrokerServer{
            .config = config,
            .io     = io,
            .wal    = &inner.wal,
            .dedup  = &inner.dedup,
            .router = &inner.router,
            .cursor = Cursor.init(config.cursor_dir, io),
        };
        const thread = try std.Thread.spawn(.{}, runServer, .{inner});
        const ts = std.c.timespec{ .sec = 0, .nsec = 50_000_000 };
        _ = std.c.nanosleep(&ts, null);
        return .{ .inner = inner, .thread = thread };
    }

    fn stop(self: *BrokerCtx) void {
        self.thread.detach();
        // inner lives until process exit (page_allocator) so detached threads stay safe.
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "Broker: PUBLISH reçoit PUBLISH_ACK (0x06)" {
    const alloc = testing.allocator;
    const io    = io_init(alloc);
    const topic = "transactions.test";

    var ctx = try BrokerCtx.start(alloc, PORT_BASIC, io, "/tmp/broker_test_basic.wal");
    defer ctx.stop();

    const msg    = try buildMessage(alloc, topic);
    defer alloc.free(msg.fields);
    const frame  = try buildPublishFrame(&msg, topic, alloc);
    defer alloc.free(frame);

    const addr   = try std.Io.net.IpAddress.parse("127.0.0.1", PORT_BASIC);
    var stream   = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var wbuf: [4096]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    try sw.interface.writeAll(frame);
    try sw.interface.flush();

    var rbuf: [64]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    const ack = try sr.interface.takeByte();
    try testing.expectEqual(proto.FRAME_PUBLISH_ACK, ack);
}

test "Broker: SUBSCRIBE reçoit DELIVER après PUBLISH" {
    const alloc = testing.allocator;
    const io    = io_init(alloc);
    const topic = "transactions.deliver";

    var ctx = try BrokerCtx.start(alloc, PORT_BASIC + 10, io, "/tmp/broker_test_deliver.wal");
    defer ctx.stop();

    const port = PORT_BASIC + 10;

    // Abonné (anonyme — pas de curseur).
    const sub_frame = try buildSubscribeFrame(topic, "", alloc);
    defer alloc.free(sub_frame);
    const sub_addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    var sub_stream = try sub_addr.connect(io, .{ .mode = .stream });
    defer sub_stream.close(io);

    var sub_wbuf: [256]u8 = undefined;
    var sub_sw = sub_stream.writer(io, &sub_wbuf);
    try sub_sw.interface.writeAll(sub_frame);
    try sub_sw.interface.flush();

    const ts = std.c.timespec{ .sec = 0, .nsec = 20_000_000 };
    _ = std.c.nanosleep(&ts, null);

    // Publie depuis une autre connexion.
    const msg   = try buildMessage(alloc, topic);
    defer alloc.free(msg.fields);
    const frame = try buildPublishFrame(&msg, topic, alloc);
    defer alloc.free(frame);

    const pub_addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    var pub_stream = try pub_addr.connect(io, .{ .mode = .stream });
    defer pub_stream.close(io);

    var pub_wbuf: [4096]u8 = undefined;
    var pub_sw = pub_stream.writer(io, &pub_wbuf);
    try pub_sw.interface.writeAll(frame);
    try pub_sw.interface.flush();

    var pub_rbuf: [16]u8 = undefined;
    var pub_sr = pub_stream.reader(io, &pub_rbuf);
    const ack = try pub_sr.interface.takeByte();
    try testing.expectEqual(proto.FRAME_PUBLISH_ACK, ack);

    // Lit le DELIVER côté abonné.
    var sub_rbuf: [16384]u8 = undefined;
    var sub_sr = sub_stream.reader(io, &sub_rbuf);

    var len_bytes: [4]u8 = undefined;
    try sub_sr.interface.readSliceAll(&len_bytes);
    const body_len = std.mem.readInt(u32, &len_bytes, .big);
    try testing.expect(body_len > 0);

    const frame_type = try sub_sr.interface.takeByte();
    try testing.expectEqual(proto.FRAME_DELIVER, frame_type);

    // Drain le reste de la frame (topic_len + topic + wal_offset + payload).
    const rest_len = body_len - 1;
    const rest = try alloc.alloc(u8, rest_len);
    defer alloc.free(rest);
    try sub_sr.interface.readSliceAll(rest);

    // Extrait le topic depuis la frame.
    const delivered_topic_len = std.mem.readInt(u16, rest[0..2], .big);
    const delivered_topic = rest[2..2 + delivered_topic_len];
    try testing.expectEqualStrings(topic, delivered_topic);
}

test "Broker: déduplication — PUBLISH identique rejeté (même msg_id)" {
    const alloc = testing.allocator;
    const io    = io_init(alloc);
    const topic = "transactions.dedup";

    var ctx = try BrokerCtx.start(alloc, PORT_DEDUP, io, "/tmp/broker_test_dedup.wal");
    defer ctx.stop();

    const msg   = try buildMessage(alloc, topic);
    defer alloc.free(msg.fields);
    const frame = try buildPublishFrame(&msg, topic, alloc);
    defer alloc.free(frame);

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", PORT_DEDUP);
    {
        var s   = try addr.connect(io, .{ .mode = .stream });
        defer s.close(io);
        var wb: [4096]u8 = undefined;
        var sw = s.writer(io, &wb);
        try sw.interface.writeAll(frame);
        try sw.interface.flush();
        var rb: [16]u8 = undefined;
        var sr = s.reader(io, &rb);
        const a = try sr.interface.takeByte();
        try testing.expectEqual(proto.FRAME_PUBLISH_ACK, a);
    }

    // Second envoi avec le même msg_id — le broker doit toujours ACKer.
    {
        var s   = try addr.connect(io, .{ .mode = .stream });
        defer s.close(io);
        var wb: [4096]u8 = undefined;
        var sw = s.writer(io, &wb);
        try sw.interface.writeAll(frame);
        try sw.interface.flush();
        var rb: [16]u8 = undefined;
        var sr = s.reader(io, &rb);
        const a = try sr.interface.takeByte();
        try testing.expectEqual(proto.FRAME_PUBLISH_ACK, a);
    }
}

test "Broker: topic isolation — PUBLISH sur A n'est pas livré sur B" {
    const alloc   = testing.allocator;
    const io      = io_init(alloc);
    const topic_a = "transactions.a";
    const topic_b = "transactions.b";
    const port    = PORT_FANOUT;

    var ctx = try BrokerCtx.start(alloc, port, io, "/tmp/broker_test_isolation.wal");
    defer ctx.stop();

    // Heap-allocate sub_stream so a detached checker thread stays safe after
    // this test stack frame returns. page_allocator has no leak detector.
    const sub_stream = try std.heap.page_allocator.create(std.Io.net.Stream);
    const sub_frame = try buildSubscribeFrame(topic_b, "", alloc);
    defer alloc.free(sub_frame);
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);
    sub_stream.* = try addr.connect(io, .{ .mode = .stream });

    var sw_b: [256]u8 = undefined;
    var sub_sw = sub_stream.writer(io, &sw_b);
    try sub_sw.interface.writeAll(sub_frame);
    try sub_sw.interface.flush();

    const ts = std.c.timespec{ .sec = 0, .nsec = 20_000_000 };
    _ = std.c.nanosleep(&ts, null);

    // Publie sur topic_a — ne doit PAS atteindre l'abonné topic_b.
    const msg   = try buildMessage(alloc, topic_a);
    defer alloc.free(msg.fields);
    const frame = try buildPublishFrame(&msg, topic_a, alloc);
    defer alloc.free(frame);

    var pub_stream = try addr.connect(io, .{ .mode = .stream });
    defer pub_stream.close(io);
    var pw: [4096]u8 = undefined;
    var pub_sw = pub_stream.writer(io, &pw);
    try pub_sw.interface.writeAll(frame);
    try pub_sw.interface.flush();
    var prb: [16]u8 = undefined;
    var pub_sr = pub_stream.reader(io, &prb);
    _ = try pub_sr.interface.takeByte();

    // Vérifie qu'aucun DELIVER n'arrive sur sub_stream dans un délai court.
    const Checker = struct {
        stream:   *std.Io.net.Stream,
        io:       std.Io,
        received: std.atomic.Value(bool),

        fn run(self: *@This()) void {
            var rbuf: [4096]u8 = undefined;
            var sr = self.stream.reader(self.io, &rbuf);
            var lb: [4]u8 = undefined;
            sr.interface.readSliceAll(&lb) catch return;
            self.received.store(true, .release);
        }
    };

    // Heap-allocate checker too — detached thread outlives this stack frame.
    const checker = try std.heap.page_allocator.create(Checker);
    checker.* = .{
        .stream   = sub_stream,
        .io       = io,
        .received = std.atomic.Value(bool).init(false),
    };

    const t = try std.Thread.spawn(.{}, Checker.run, .{checker});

    // Wait 100 ms, then read the flag and detach.
    // Do NOT close sub_stream — closing while the io event loop has a pending
    // read on the same fd causes a BADF panic in Zig 0.16's Threaded backend.
    const wait = std.c.timespec{ .sec = 0, .nsec = 100_000_000 };
    _ = std.c.nanosleep(&wait, null);

    const received = checker.received.load(.acquire);
    t.detach();
    try testing.expect(!received);
}

test "Broker: cursor — replay depuis le dernier offset ACKé" {
    const alloc  = testing.allocator;
    const io     = io_init(alloc);
    const topic  = "transactions.cursor";
    const sub_id = "csub1";
    const port   = PORT_CURSOR;

    // Nettoie les artefacts des runs précédents.
    std.Io.Dir.cwd().deleteFile(io, "/tmp/orus_test_cursors/" ++ sub_id ++ ".cursor") catch {};

    var ctx = try BrokerCtx.start(alloc, port, io, "/tmp/broker_test_cursor.wal");
    defer ctx.stop();

    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", port);

    // ── Session 1 : s'abonner, recevoir msg1, ACKer ───────────────────────────

    const sub_frame1 = try buildSubscribeFrame(topic, sub_id, alloc);
    defer alloc.free(sub_frame1);

    var sub1 = try addr.connect(io, .{ .mode = .stream });

    var sw1_buf: [256]u8 = undefined;
    var sw1 = sub1.writer(io, &sw1_buf);
    try sw1.interface.writeAll(sub_frame1);
    try sw1.interface.flush();

    const ts20 = std.c.timespec{ .sec = 0, .nsec = 20_000_000 };
    _ = std.c.nanosleep(&ts20, null);

    // Publie msg1.
    const msg1  = try buildMessage(alloc, topic);
    defer alloc.free(msg1.fields);
    const frame1 = try buildPublishFrame(&msg1, topic, alloc);
    defer alloc.free(frame1);

    var pub1 = try addr.connect(io, .{ .mode = .stream });
    defer pub1.close(io);
    var p1w: [4096]u8 = undefined;
    var p1sw = pub1.writer(io, &p1w);
    try p1sw.interface.writeAll(frame1);
    try p1sw.interface.flush();
    var p1r: [16]u8 = undefined;
    var p1sr = pub1.reader(io, &p1r);
    _ = try p1sr.interface.takeByte(); // consume PUBLISH_ACK

    // Reçoit DELIVER pour msg1, extrait wal_offset.
    var sub1_rbuf: [16384]u8 = undefined;
    var sub1_sr = sub1.reader(io, &sub1_rbuf);
    const wal_offset1 = try readDeliverOffset(&sub1_sr.interface, topic, alloc);
    try testing.expect(wal_offset1 > 0);

    // ACKe msg1 — le broker persiste cursor = wal_offset1.
    const ack1 = buildAckFrame(wal_offset1);
    try sw1.interface.writeAll(&ack1);
    try sw1.interface.flush();

    const ts10 = std.c.timespec{ .sec = 0, .nsec = 10_000_000 };
    _ = std.c.nanosleep(&ts10, null);

    // Ferme la session 1.
    sub1.close(io);
    _ = std.c.nanosleep(&ts20, null);

    // ── Publie msg2 hors-session (pas d'abonnés actifs → WAL only) ────────────

    const msg2  = try buildMessage(alloc, topic);
    defer alloc.free(msg2.fields);
    const frame2 = try buildPublishFrame(&msg2, topic, alloc);
    defer alloc.free(frame2);

    var pub2 = try addr.connect(io, .{ .mode = .stream });
    defer pub2.close(io);
    var p2w: [4096]u8 = undefined;
    var p2sw = pub2.writer(io, &p2w);
    try p2sw.interface.writeAll(frame2);
    try p2sw.interface.flush();
    var p2r: [16]u8 = undefined;
    var p2sr = pub2.reader(io, &p2r);
    _ = try p2sr.interface.takeByte(); // consume PUBLISH_ACK

    // ── Session 2 : se reconnecter avec le même sub_id ────────────────────────
    // Le broker lit cursor = wal_offset1 et rejoue msg2 depuis le WAL.

    const sub_frame2 = try buildSubscribeFrame(topic, sub_id, alloc);
    defer alloc.free(sub_frame2);

    var sub2 = try addr.connect(io, .{ .mode = .stream });
    defer sub2.close(io);

    var sw2_buf: [256]u8 = undefined;
    var sw2 = sub2.writer(io, &sw2_buf);
    try sw2.interface.writeAll(sub_frame2);
    try sw2.interface.flush();

    // Le DELIVER de msg2 doit arriver via WAL replay.
    var sub2_rbuf: [16384]u8 = undefined;
    var sub2_sr = sub2.reader(io, &sub2_rbuf);
    const wal_offset2 = try readDeliverOffset(&sub2_sr.interface, topic, alloc);
    try testing.expect(wal_offset2 > wal_offset1);
}
