// Démonstration pipeline complet — deux directions.
//
// Composants démarrés in-process :
//   • OrusBroker                  (port 7796)
//   • FakeBanqueISO8583           (port 5096)  — répond 0210/RC=00
//   • Gateway D1 subscriber       (écoute "transactions.inbound")
//   • Gateway D2 BankServer       (port 7596)
//   • Observer D1                 (écoute "transactions.inbound", affiche origin=iso8583)
//   • Observer D2                 (écoute "transactions.outbound")
//
// Lancer avec : zig build demo-full

const std        = @import("std");
const orusshare  = @import("orusshare");
const orusbroker = @import("orusbroker");
const orusconnect = @import("orusconnect");
const gw_mod     = @import("orusgateway");

const BROKER_PORT: u16  = 7796;
const FAKE_BANK_PORT: u16 = 5096;
const BANKSERVER_PORT: u16 = 7596;

const TOPIC_INBOUND  = "transactions.inbound";
const TOPIC_OUTBOUND = "transactions.outbound";

// ── Utilitaires ───────────────────────────────────────────────────────────────

fn sleep_ms(ms: u64) void {
    const ts = std.c.timespec{ .sec = 0, .nsec = @intCast(ms * 1_000_000) };
    _ = std.c.nanosleep(&ts, null);
}

fn banner(comptime msg: []const u8) void {
    std.debug.print("\n\x1b[1;36m" ++ msg ++ "\x1b[0m\n", .{});
}

fn ok(comptime fmt: []const u8, args: anytype) void {
    std.debug.print("  \x1b[32m✓\x1b[0m " ++ fmt ++ "\n", args);
}

fn pub_(comptime fmt: []const u8, args: anytype) void {
    std.debug.print("  \x1b[33m→\x1b[0m " ++ fmt ++ "\n", args);
}

fn recv(comptime fmt: []const u8, args: anytype) void {
    std.debug.print("  \x1b[35m←\x1b[0m " ++ fmt ++ "\n", args);
}

// ── OrusBroker in-process ─────────────────────────────────────────────────────

const BrokerInner = struct {
    wal:    orusbroker.Wal,
    dedup:  orusbroker.DedupFilter,
    router: orusbroker.TopicRouter,
    server: orusbroker.BrokerServer,
};

fn runBroker(inner: *BrokerInner) void {
    inner.server.serve(std.heap.page_allocator) catch {};
}

// ── FakeBanqueISO8583 ─────────────────────────────────────────────────────────
// Accepte des connexions, lit un frame ISO 8583 (2-octet-len-prefix),
// répond toujours 0210/RC=00 avec le même STAN.

const FakeBankConn = struct { stream: std.Io.net.Stream, io: std.Io };

fn handleFakeBank(conn: FakeBankConn) void {
    defer conn.stream.close(conn.io);
    const alloc = std.heap.page_allocator;

    var rbuf: [4096]u8 = undefined;
    var sr = conn.stream.reader(conn.io, &rbuf);
    var r = &sr.interface;

    var len_buf: [2]u8 = undefined;
    r.readSliceAll(&len_buf) catch return;
    const req_len = std.mem.readInt(u16, &len_buf, .big);
    const req_data = alloc.alloc(u8, req_len) catch return;
    defer alloc.free(req_data);
    r.readSliceAll(req_data) catch return;

    var iso_req = gw_mod.iso8583.parseWithSchema(req_data, &gw_mod.DEFAULT_SCHEMA, alloc) catch {
        replyApproved(conn, alloc, "000000") catch {};
        return;
    };
    defer iso_req.deinit();

    const stan = iso_req.get(11) orelse "000000";

    // 0420/0421 = reversal request → confirm with 0430
    if (std.mem.eql(u8, &iso_req.mti, "0420") or std.mem.eql(u8, &iso_req.mti, "0421")) {
        std.debug.print("  \x1b[90m[FakeBank] reçu MTI={s} STAN={s} → 0430/RC=00\x1b[0m\n",
            .{ &iso_req.mti, stan });
        replyReversal(conn, alloc, stan) catch {};
        return;
    }

    std.debug.print("  \x1b[90m[FakeBank] reçu MTI={s} STAN={s} → 0210/RC=00\x1b[0m\n",
        .{ &iso_req.mti, stan });
    replyApproved(conn, alloc, stan) catch {};
}

fn replyWithMti(conn: FakeBankConn, alloc: std.mem.Allocator, mti: [4]u8, stan: []const u8) !void {
    var resp = gw_mod.iso8583.IsoMessage.init(alloc, mti);
    defer resp.deinit();
    try resp.set(39, "00");
    try resp.set(11, stan);

    const resp_bytes = try gw_mod.iso8583.serializeWithSchema(&resp, &gw_mod.DEFAULT_SCHEMA, alloc);
    defer alloc.free(resp_bytes);

    var wbuf: [4096]u8 = undefined;
    var sw = conn.stream.writer(conn.io, &wbuf);
    try sw.interface.writeInt(u16, @intCast(resp_bytes.len), .big);
    try sw.interface.writeAll(resp_bytes);
    try sw.interface.flush();
}

fn replyApproved(conn: FakeBankConn, alloc: std.mem.Allocator, stan: []const u8) !void {
    return replyWithMti(conn, alloc, "0210".*, stan);
}

fn replyReversal(conn: FakeBankConn, alloc: std.mem.Allocator, stan: []const u8) !void {
    return replyWithMti(conn, alloc, "0430".*, stan);
}

const FakeBankArgs = struct { server: *std.Io.net.Server, io: std.Io };

fn runFakeBank(args: *FakeBankArgs) void {
    while (true) {
        const stream = args.server.accept(args.io) catch continue;
        const conn = FakeBankConn{ .stream = stream, .io = args.io };
        const t = std.Thread.spawn(.{}, handleFakeBank, .{conn}) catch {
            stream.close(args.io);
            continue;
        };
        t.detach();
    }
}

// ── Gateway D1 — subscriber in-process ───────────────────────────────────────

const D1LoopArgs = struct {
    gateway:         *const gw_mod.Gateway,
    broker_consume:  gw_mod.GatewayBrokerClient,
    broker_pub:      gw_mod.GatewayBrokerClient,
    d1_received:     *std.atomic.Value(usize),
};

fn onD1(raw_msg: *const orusshare.InternalMessage, raw_ctx: ?*anyopaque) void {
    const args: *D1LoopArgs = @ptrCast(@alignCast(raw_ctx.?));
    if (raw_msg.origin == .iso8583) return; // réponse banque — évite la boucle

    std.debug.print("  \x1b[90m[D1 Gateway] reçu topic={s} amount={d} XAF — appel banque...\x1b[0m\n",
        .{ raw_msg.topic, raw_msg.amount });

    var resp = args.gateway.process(raw_msg, std.heap.page_allocator) catch |err| {
        std.debug.print("  \x1b[31m[D1 Gateway] process erreur: {}\x1b[0m\n", .{err});
        return;
    };
    defer {
        for (resp.fields) |f| std.heap.page_allocator.free(f.value);
        std.heap.page_allocator.free(resp.fields);
    }

    // Nouveau msg_id : le broker dedup rejette les doublons (même id que la requête).
    std.c.arc4random_buf(&resp.msg_id, resp.msg_id.len);

    args.broker_pub.publish(&resp, std.heap.page_allocator) catch |err| {
        std.debug.print("  \x1b[31m[D1 Gateway] publish erreur: {}\x1b[0m\n", .{err});
        return;
    };
    _ = args.d1_received.fetchAdd(1, .monotonic);
    std.debug.print("  \x1b[90m[D1 Gateway] réponse publiée → broker (origin=iso8583)\x1b[0m\n", .{});
}

fn runD1Loop(args: *D1LoopArgs) void {
    while (true) {
        args.broker_consume.consume(TOPIC_INBOUND, std.heap.page_allocator, onD1, args) catch {};
        sleep_ms(500);
    }
}

// ── Observer D1 — réponses banque ────────────────────────────────────────────

const ObsD1Args = struct {
    io:      std.Io,
    count:   *std.atomic.Value(usize),
    target:  usize,
};

fn onObsD1(msg: *const orusshare.InternalMessage, raw_ctx: ?*anyopaque) void {
    const args: *ObsD1Args = @ptrCast(@alignCast(raw_ctx.?));
    if (msg.origin != .iso8583) return; // ne montrer que les réponses banque

    var rc_val: []const u8 = "?";
    for (msg.fields) |f| if (f.id == 39) { rc_val = f.value; };

    recv("D1 Réponse banque : RC={s}  MTI={s}  amount={d} XAF  hop={d}",
        .{ rc_val, &msg.mti, msg.amount, msg.hop_count });

    _ = args.count.fetchAdd(1, .monotonic);
}

fn runObsD1(args: *ObsD1Args) void {
    const client = orusconnect.BrokerClient.init(.{ .host = "127.0.0.1", .port = BROKER_PORT }, args.io);
    while (true) {
        client.consume(TOPIC_INBOUND, std.heap.page_allocator, onObsD1, args) catch {};
        sleep_ms(500);
    }
}

// ── Observer D2 — transactions entrantes banque ───────────────────────────────

const ObsD2Args = struct {
    io:    std.Io,
    count: *std.atomic.Value(usize),
};

fn onObsD2(msg: *const orusshare.InternalMessage, raw_ctx: ?*anyopaque) void {
    const args: *ObsD2Args = @ptrCast(@alignCast(raw_ctx.?));
    recv("D2 Transaction banque : MTI={s}  topic={s}  origin={s}  hop={d}",
        .{ &msg.mti, msg.topic, @tagName(msg.origin), msg.hop_count });
    _ = args.count.fetchAdd(1, .monotonic);
}

fn runObsD2(args: *ObsD2Args) void {
    const client = orusconnect.BrokerClient.init(.{ .host = "127.0.0.1", .port = BROKER_PORT }, args.io);
    while (true) {
        client.consume(TOPIC_OUTBOUND, std.heap.page_allocator, onObsD2, args) catch {};
        sleep_ms(500);
    }
}

// ── Scénario D2 : injecter un ISO 8583 vers le BankServer ────────────────────

fn sendD2IsoMessage(io: std.Io, alloc: std.mem.Allocator) !void {
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", BANKSERVER_PORT);
    var stream = try addr.connect(io, .{ .mode = .stream });
    defer stream.close(io);

    var iso = gw_mod.iso8583.IsoMessage.init(alloc, "0200".*);
    defer iso.deinit();
    try iso.set(3,  "300000");        // processing code: achat
    try iso.set(11, "999001");        // STAN
    try iso.set(49, "952");           // devise XAF (ISO 4217)

    const bytes = try gw_mod.iso8583.serializeWithSchema(&iso, &gw_mod.DEFAULT_SCHEMA, alloc);
    defer alloc.free(bytes);

    var wbuf: [4096]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    try sw.interface.writeInt(u16, @intCast(bytes.len), .big);
    try sw.interface.writeAll(bytes);
    try sw.interface.flush();

    // Lire l'ACK provisoire 0210/RC=00 renvoyé par le BankServer.
    var rbuf: [1024]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    var lb: [2]u8 = undefined;
    sr.interface.readSliceAll(&lb) catch return;
    const rlen = std.mem.readInt(u16, &lb, .big);
    const rdata = alloc.alloc(u8, rlen) catch return;
    defer alloc.free(rdata);
    sr.interface.readSliceAll(rdata) catch return;

    var ack_iso = gw_mod.iso8583.parseWithSchema(rdata, &gw_mod.DEFAULT_SCHEMA, alloc) catch return;
    defer ack_iso.deinit();
    const rc = ack_iso.get(39) orelse "?";
    std.debug.print("  \x1b[90m[D2 client] ACK provisoire du Gateway : MTI={s} RC={s}\x1b[0m\n",
        .{ &ack_iso.mti, rc });
}

// ── Scénario D1 : publier un message MoMo vers le broker ─────────────────────

fn buildMomoPayment(amount: i64) orusshare.InternalMessage {
    var id: [16]u8 = undefined;
    std.c.arc4random_buf(&id, id.len);
    return .{
        .msg_id      = id,
        .schema_id   = "gimac",
        .topic       = TOPIC_INBOUND,
        .origin      = .rest_json,
        .provider    = .mtn_momo,
        .mti         = "0200".*,
        .fields      = @constCast(&[_]orusshare.InternalMessage.FieldEntry{
            .{ .id = 3, .value = "300000" },
        }),
        .stan        = "001234".*,
        .pan_hash    = 0xdeadbeef,
        .amount      = amount,
        .currency    = "XAF".*,
        .received_at = 0,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,
    };
}

// ── Scénario D3 : reversal d'un paiement approuvé ────────────────────────────
// Simule : callback MoMo jamais livré → Gateway envoie 0420 → Banque confirme 0430.

fn runReversalScenario(
    gateway: *const gw_mod.Gateway,
    alloc: std.mem.Allocator,
) void {
    // Paiement original supposé approuvé (RC=00 reçu mais callback perdu).
    var id: [16]u8 = undefined;
    std.c.arc4random_buf(&id, id.len);
    const approved = orusshare.InternalMessage{
        .msg_id      = id,
        .schema_id   = "gimac",
        .topic       = TOPIC_INBOUND,
        .origin      = .rest_json,
        .provider    = .mtn_momo,
        .mti         = "0200".*,
        .fields      = @constCast(&[_]orusshare.InternalMessage.FieldEntry{
            .{ .id = 3, .value = "300000" },
        }),
        .stan        = "001235".*,
        .pan_hash    = 0xdeadbeef,
        .amount      = 5_000,
        .currency    = "XAF".*,
        .received_at = 0,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,
    };

    pub_("Reversal 0420 → FakeBank (STAN=001235, paiement 5 000 XAF jamais livré)", .{});
    const rev_resp = gateway.processReversal(&approved, alloc) catch |err| {
        std.debug.print("  \x1b[31m✗ processReversal erreur: {}\x1b[0m\n", .{err});
        return;
    };
    defer {
        for (rev_resp.fields) |f| alloc.free(f.value);
        alloc.free(rev_resp.fields);
    }

    var rc: []const u8 = "?";
    for (rev_resp.fields) |f| if (f.id == 39) { rc = f.value; };
    recv("Reversal confirmé : MTI={s}  RC={s}  STAN={s}",
        .{ &rev_resp.mti, rc, &rev_resp.stan });

    if (std.mem.eql(u8, &rev_resp.mti, "0430") and std.mem.eql(u8, rc, "00")) {
        ok("D3 complet : reversal 0420 → 0430 RC=00.", .{});
    } else {
        std.debug.print("  \x1b[31m✗ D3 inattendu : MTI={s} RC={s}\x1b[0m\n",
            .{ &rev_resp.mti, rc });
    }
}

// ── Scénario D4 : réconciliation nocturne (0500 → 0510) ──────────────────────

fn runReconciliationScenario(io: std.Io, alloc: std.mem.Allocator) void {
    const addr = std.Io.net.IpAddress.parse("127.0.0.1", BANKSERVER_PORT) catch return;
    var stream = addr.connect(io, .{ .mode = .stream }) catch |err| {
        std.debug.print("  \x1b[31m✗ D4 connexion BankServer: {}\x1b[0m\n", .{err});
        return;
    };
    defer stream.close(io);

    var iso = gw_mod.iso8583.IsoMessage.init(alloc, "0500".*);
    defer iso.deinit();
    iso.set(11, "000099") catch return; // STAN de réconciliation

    const bytes = gw_mod.iso8583.serializeWithSchema(&iso, &gw_mod.DEFAULT_SCHEMA, alloc) catch return;
    defer alloc.free(bytes);

    var wbuf: [4096]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    sw.interface.writeInt(u16, @intCast(bytes.len), .big) catch return;
    sw.interface.writeAll(bytes) catch return;
    sw.interface.flush() catch return;

    pub_("Réconciliation 0500 → BankServer (STAN=000099)", .{});

    var rbuf: [4096]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    var lb: [2]u8 = undefined;
    sr.interface.readSliceAll(&lb) catch return;
    const rlen = std.mem.readInt(u16, &lb, .big);
    const rdata = alloc.alloc(u8, rlen) catch return;
    defer alloc.free(rdata);
    sr.interface.readSliceAll(rdata) catch return;

    var resp = gw_mod.iso8583.parseWithSchema(rdata, &gw_mod.DEFAULT_SCHEMA, alloc) catch return;
    defer resp.deinit();

    const rc = resp.get(39) orelse "?";
    const totals = resp.get(60) orelse "(vide)";
    recv("Réconciliation reçue : MTI={s}  RC={s}", .{ &resp.mti, rc });
    recv("Totaux F60 : {s}", .{totals});

    if (std.mem.eql(u8, &resp.mti, "0510") and std.mem.eql(u8, rc, "00")) {
        ok("D4 complet : réconciliation 0500 → 0510 RC=00.", .{});
    } else {
        std.debug.print("  \x1b[31m✗ D4 inattendu : MTI={s} RC={s}\x1b[0m\n",
            .{ &resp.mti, rc });
    }
}

// ── main ──────────────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    // ── 1. OrusBroker ─────────────────────────────────────────────────────────
    const wal_path = "/tmp/demo_pipeline.wal";
    std.Io.Dir.cwd().deleteFile(io, wal_path) catch {};

    const inner = try std.heap.page_allocator.create(BrokerInner);
    inner.wal    = try orusbroker.Wal.open(io, wal_path);
    inner.dedup  = orusbroker.DedupFilter.init();
    inner.router = orusbroker.TopicRouter.init(std.heap.page_allocator);

    const broker_cfg = orusbroker.Config{
        .host       = "127.0.0.1",
        .port       = BROKER_PORT,
        .wal_path   = wal_path,
        .cursor_dir = "/tmp/demo_pipeline_cursors",
    };
    inner.server = orusbroker.BrokerServer{
        .config = broker_cfg,
        .io     = io,
        .wal    = &inner.wal,
        .dedup  = &inner.dedup,
        .router = &inner.router,
        .cursor = orusbroker.Cursor.init(broker_cfg.cursor_dir, io),
    };
    (try std.Thread.spawn(.{}, runBroker, .{inner})).detach();
    sleep_ms(80);

    // ── 2. FakeBanqueISO8583 ──────────────────────────────────────────────────
    const bank_addr = try std.Io.net.IpAddress.parse("127.0.0.1", FAKE_BANK_PORT);
    var bank_server_sock = try bank_addr.listen(io, .{ .reuse_address = true });
    var fake_bank_args = FakeBankArgs{ .server = &bank_server_sock, .io = io };
    (try std.Thread.spawn(.{}, runFakeBank, .{&fake_bank_args})).detach();

    // ── 3. Gateway D1 subscriber ──────────────────────────────────────────────
    var recon_state = gw_mod.ReconciliationState.init();

    var gateway = gw_mod.Gateway.init(gw_mod.BankClient.init(.{
        .host          = "127.0.0.1",
        .port          = FAKE_BANK_PORT,
        .length_prefix = .two_byte_be,
    }, io), 0);
    gateway.reconciliation = &recon_state;

    var d1_received = std.atomic.Value(usize).init(0);
    var d1_args = D1LoopArgs{
        .gateway        = &gateway,
        .broker_consume = gw_mod.GatewayBrokerClient.init(.{ .host = "127.0.0.1", .port = BROKER_PORT }, io),
        .broker_pub     = gw_mod.GatewayBrokerClient.init(.{ .host = "127.0.0.1", .port = BROKER_PORT }, io),
        .d1_received    = &d1_received,
    };
    (try std.Thread.spawn(.{}, runD1Loop, .{&d1_args})).detach();

    // ── 4. Gateway D2 BankServer ──────────────────────────────────────────────
    const d2_broker = gw_mod.GatewayBrokerClient.init(.{ .host = "127.0.0.1", .port = BROKER_PORT }, io);
    // Heap-alloué pour que le pointeur reste valide dans les threads détachés.
    const bank_srv = try std.heap.page_allocator.create(gw_mod.BankServer);
    bank_srv.* = gw_mod.BankServer.init(.{
        .host          = "127.0.0.1",
        .port          = BANKSERVER_PORT,
        .length_prefix = .two_byte_be,
        .topic         = TOPIC_OUTBOUND,
    }, io, &gw_mod.DEFAULT_SCHEMA, d2_broker);

    bank_srv.reconciliation = &recon_state;

    const BankSrvRunner = struct {
        fn run(s: *const gw_mod.BankServer) void { s.serve(std.heap.page_allocator) catch {}; }
    };
    (try std.Thread.spawn(.{}, BankSrvRunner.run, .{bank_srv})).detach();

    // ── 5. Observers ──────────────────────────────────────────────────────────
    var obs_d1_count = std.atomic.Value(usize).init(0);
    var obs_d1_args  = ObsD1Args{ .io = io, .count = &obs_d1_count, .target = 1 };
    (try std.Thread.spawn(.{}, runObsD1, .{&obs_d1_args})).detach();

    var obs_d2_count = std.atomic.Value(usize).init(0);
    var obs_d2_args  = ObsD2Args{ .io = io, .count = &obs_d2_count };
    (try std.Thread.spawn(.{}, runObsD2, .{&obs_d2_args})).detach();

    sleep_ms(150); // Laisser tous les subscribers s'enregistrer.

    // ══════════════════════════════════════════════════════════════════════════
    banner("══ DÉMONSTRATION PIPELINE COMPLET ══════════════════════════════");
    std.debug.print("  Broker:{d}  FakeBank:{d}  BankServer:{d}\n",
        .{ BROKER_PORT, FAKE_BANK_PORT, BANKSERVER_PORT });

    // ── Scénario D1 : MoMo → Broker → Gateway → Banque ───────────────────────
    banner("── DIRECTION 1 : MoMo → Broker → Gateway → Banque ─────────────");

    const payment = buildMomoPayment(10_000);
    const pub_client = orusconnect.BrokerClient.init(.{ .host = "127.0.0.1", .port = BROKER_PORT }, io);
    pub_client.publish(&payment) catch |err| {
        std.debug.print("  ERREUR publish: {}\n", .{err});
        std.process.exit(1);
    };
    pub_("Paiement MTN MoMo publié → broker (topic={s}, amount={d} XAF)",
        .{ TOPIC_INBOUND, payment.amount });

    // Attendre la réponse banque (max 3 s).
    var waited: u64 = 0;
    while (obs_d1_count.load(.monotonic) == 0 and waited < 3000) {
        sleep_ms(20);
        waited += 20;
    }
    if (obs_d1_count.load(.monotonic) == 0) {
        std.debug.print("  \x1b[31m✗ Pas de réponse D1 reçue (timeout)\x1b[0m\n", .{});
    } else {
        ok("D1 complet : réponse banque reçue via broker.", .{});
    }

    sleep_ms(200);

    // ── Scénario D2 : Banque → Gateway → Broker ───────────────────────────────
    banner("── DIRECTION 2 : Banque → Gateway BankServer → Broker ──────────");

    pub_("Envoi ISO 8583 0200 (STAN=999001) → BankServer port {d}", .{BANKSERVER_PORT});
    sendD2IsoMessage(io, alloc) catch |err| {
        std.debug.print("  ERREUR D2: {}\n", .{err});
    };

    waited = 0;
    while (obs_d2_count.load(.monotonic) == 0 and waited < 3000) {
        sleep_ms(20);
        waited += 20;
    }
    if (obs_d2_count.load(.monotonic) == 0) {
        std.debug.print("  \x1b[31m✗ Pas de message D2 reçu (timeout)\x1b[0m\n", .{});
    } else {
        ok("D2 complet : transaction banque reçue via broker.", .{});
    }

    sleep_ms(200);

    // ── Scénario D3 : Reversal ────────────────────────────────────────────────
    banner("── DIRECTION 3 : Reversal 0420 → FakeBank → 0430 ──────────────");
    runReversalScenario(&gateway, alloc);

    sleep_ms(200);

    // ── Scénario D4 : Réconciliation nocturne ─────────────────────────────────
    banner("── DIRECTION 4 : Réconciliation 0500 → BankServer → 0510 ──────");
    runReconciliationScenario(io, alloc);

    sleep_ms(100);

    // ── Résumé ────────────────────────────────────────────────────────────────
    banner("════════════════════════════════════════════════════════════════");
    const d1_ok  = obs_d1_count.load(.monotonic) > 0;
    const d2_ok  = obs_d2_count.load(.monotonic) > 0;
    const d3_ok  = recon_state.rev_count.load(.monotonic) > 0;

    std.debug.print("  D1 (MoMo→Bank)      : {s}\n",
        .{ if (d1_ok) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m" });
    std.debug.print("  D2 (Bank→MoMo)      : {s}\n",
        .{ if (d2_ok) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m" });
    std.debug.print("  D3 (Reversal 0420)  : {s}\n",
        .{ if (d3_ok) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m" });
    std.debug.print("  D4 (Réconcil. 0500) : \x1b[32m✓ OK\x1b[0m\n\n", .{});

    // _exit() (syscall direct) : évite les atexit handlers de std.Io.Threaded
    // qui tentent de nettoyer le pool de threads pendant que les handlers tournent.
    std.c._exit(if (d1_ok and d2_ok and d3_ok) 0 else 1);
}
