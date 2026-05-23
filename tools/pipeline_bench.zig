// Pipeline complet avec dashboard web en temps réel.
// Lance : zig build pipeline
// Ouvre : http://localhost:7780
//
// Deux chemins en parallèle :
//   • Broker throughput  — publisher → broker → subscriber (D1/D2)
//   • Gateway compliance — GatewayWorker → ISO 8583 → FakeBank (avec MAC + audit)
//
// Fonctionnalités réglementaires actives pendant le bench :
//   Sign-on 0800/301   avant le démarrage de la charge
//   MAC Field 64        sur chaque message sortant vers la banque
//   Heartbeat 0800/801  toutes les 30 s via le ticker
//   AuditLog            fsync par transaction traitée par le gateway

const std        = @import("std");
const orusshare  = @import("orusshare");
const orusbroker = @import("orusbroker");
const orusconnect = @import("orusconnect");
const gw_mod     = @import("orusgateway");
const builtin    = @import("builtin");

const BROKER_PORT:    u16 = 7797;
const FAKE_BANK_PORT: u16 = 5097;
const HTTP_PORT:      u16 = 7780;
const TOPIC_D1 = "pipeline.inbound";
const TOPIC_D2 = "pipeline.outbound";
const AUDIT_LOG_PATH = "/tmp/pipeline_bench_audit.log";

// ── Métriques atomiques ───────────────────────────────────────────────────────

var g_d1_del     = std.atomic.Value(u64).init(0);
var g_d2_del     = std.atomic.Value(u64).init(0);
var g_rev_del    = std.atomic.Value(u64).init(0);
var g_pub_errors = std.atomic.Value(u64).init(0);
var g_gw_proc    = std.atomic.Value(u64).init(0); // transactions gateway traitées
var g_heartbeats = std.atomic.Value(u64).init(0);
var g_signon_ok  = std.atomic.Value(bool).init(false);
var g_start_ns:  i64 = 0;

const LAT_CAP = 2000;
var g_lat:    [LAT_CAP]u64 = undefined;
var g_lat_idx = std.atomic.Value(usize).init(0);
var g_p50_us  = std.atomic.Value(u64).init(0);
var g_p99_us  = std.atomic.Value(u64).init(0);

var g_stopped = std.atomic.Value(bool).init(false);

fn sigHandler(sig: std.c.SIG) callconv(.c) void {
    _ = sig;
    g_stopped.store(true, .monotonic);
}

// Seqlock : gen impair pendant l'écriture, pair sinon.
var g_hist_gen  = std.atomic.Value(usize).init(0);
var g_hist_d1:  [60]u64 = [_]u64{0} ** 60;
var g_hist_d2:  [60]u64 = [_]u64{0} ** 60;
var g_hist_rev: [60]u64 = [_]u64{0} ** 60;
var g_hist_gw:  [60]u64 = [_]u64{0} ** 60;
var g_hist_head = std.atomic.Value(usize).init(0);
var g_hist_n    = std.atomic.Value(usize).init(0);

// ── Broker in-process ─────────────────────────────────────────────────────────

const BrokerInner = struct {
    wal:    orusbroker.Wal,
    dedup:  orusbroker.DedupFilter,
    router: orusbroker.TopicRouter,
    server: orusbroker.BrokerServer,
};

fn runBroker(inner: *BrokerInner) void {
    inner.server.serve(std.heap.page_allocator) catch {};
}

// ── FakeBank in-process ───────────────────────────────────────────────────────
// 0800 → 0810 (sign-on F96, echo sans clé)
// 0420/0421 → 0430 RC=00
// autres → 0210 RC=00

const FAKE_SESSION_KEY = "orus_bench_key_32bytes_padxxxxxx";

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
        replyMti(conn, alloc, "0210".*, "000000") catch {};
        return;
    };
    defer iso_req.deinit();
    const stan = iso_req.get(11) orelse "000000";

    if (std.mem.eql(u8, &iso_req.mti, "0800")) {
        const nmi = iso_req.get(70) orelse "";
        replyNetMgmt(conn, alloc, &iso_req, nmi) catch {};
        return;
    }
    if (std.mem.eql(u8, &iso_req.mti, "0420") or std.mem.eql(u8, &iso_req.mti, "0421")) {
        replyMti(conn, alloc, "0430".*, stan) catch {};
        return;
    }
    replyMti(conn, alloc, "0210".*, stan) catch {};
}

fn replyMti(conn: FakeBankConn, alloc: std.mem.Allocator, mti: [4]u8, stan: []const u8) !void {
    var resp = gw_mod.iso8583.IsoMessage.init(alloc, mti);
    defer resp.deinit();
    try resp.set(39, "00");
    try resp.set(11, stan);
    try resp.set(49, "XAF");
    const bytes = try gw_mod.iso8583.serializeWithSchema(&resp, &gw_mod.DEFAULT_SCHEMA, alloc);
    defer alloc.free(bytes);
    var wbuf: [4096]u8 = undefined;
    var sw = conn.stream.writer(conn.io, &wbuf);
    try sw.interface.writeInt(u16, @intCast(bytes.len), .big);
    try sw.interface.writeAll(bytes);
    try sw.interface.flush();
}

fn replyNetMgmt(
    conn:    FakeBankConn,
    alloc:   std.mem.Allocator,
    request: *const gw_mod.iso8583.IsoMessage,
    nmi:     []const u8,
) !void {
    var resp = gw_mod.iso8583.IsoMessage.init(alloc, "0810".*);
    defer resp.deinit();
    try resp.set(39, "00");
    if (request.get(11)) |s| try resp.set(11, s);
    try resp.set(70, nmi);
    if (std.mem.eql(u8, nmi, "301")) try resp.set(96, FAKE_SESSION_KEY);
    const bytes = try gw_mod.iso8583.serializeWithSchema(&resp, &gw_mod.DEFAULT_SCHEMA, alloc);
    defer alloc.free(bytes);
    var wbuf: [4096]u8 = undefined;
    var sw = conn.stream.writer(conn.io, &wbuf);
    try sw.interface.writeInt(u16, @intCast(bytes.len), .big);
    try sw.interface.writeAll(bytes);
    try sw.interface.flush();
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

// ── Publisher (broker throughput path) ───────────────────────────────────────

const PubArgs = struct { io: std.Io, topic: []const u8, is_d1: bool };

fn publishLoop(args: PubArgs) void {
    var publisher = orusconnect.Publisher.init(
        .{ .host = "127.0.0.1", .port = BROKER_PORT }, args.io,
    );
    defer publisher.deinit();

    var prng: u64 = undefined;
    std.c.arc4random_buf(@ptrCast(&prng), @sizeOf(u64));

    while (true) {
        prng ^= prng << 13; prng ^= prng >> 7; prng ^= prng << 17;
        var id: [16]u8 = undefined;
        std.c.arc4random_buf(&id, id.len);
        var ts: std.c.timespec = undefined;
        _ = std.c.clock_gettime(.MONOTONIC, &ts);
        const now_ns: i64 = ts.sec *% std.time.ns_per_s + ts.nsec;
        const is_reversal = (prng % 20 == 0);
        const msg = orusshare.InternalMessage{
            .msg_id      = id,
            .schema_id   = "gimac",
            .topic       = args.topic,
            .origin      = if (args.is_d1) .rest_json else .iso8583,
            .provider    = .mtn_momo,
            .mti         = if (is_reversal) "0420".* else "0200".*,
            .fields      = &.{},
            .stan        = "000000".*,
            .pan_hash    = prng,
            .amount      = @intCast(1_000 + prng % 99_000),
            .currency    = "XAF".*,
            .received_at = now_ns,
            .source_ip   = [_]u8{0} ** 16,
            .hop_count   = 1,
            .external_id = null,
        };
        publisher.publish(&msg) catch {
            _ = g_pub_errors.fetchAdd(1, .monotonic);
            continue;
        };
    }
}

// ── Subscriber (broker throughput path) ───────────────────────────────────────

fn onMsg(msg: *const orusshare.InternalMessage, raw_ctx: ?*anyopaque) void {
    const is_d1: *bool = @ptrCast(@alignCast(raw_ctx.?));
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const now_ns: i64 = ts.sec *% std.time.ns_per_s + ts.nsec;
    if (msg.received_at > 0 and now_ns > msg.received_at) {
        const lat: u64 = @intCast(now_ns - msg.received_at);
        const idx = g_lat_idx.fetchAdd(1, .monotonic) % LAT_CAP;
        g_lat[idx] = lat;
    }
    const is_rev = std.mem.eql(u8, &msg.mti, "0420") or std.mem.eql(u8, &msg.mti, "0421");
    if (is_rev) {
        _ = g_rev_del.fetchAdd(1, .monotonic);
    } else if (is_d1.*) {
        _ = g_d1_del.fetchAdd(1, .monotonic);
    } else {
        _ = g_d2_del.fetchAdd(1, .monotonic);
    }
}

const SubArgs = struct { io: std.Io, topic: []const u8, is_d1: bool };

fn subLoop(args: SubArgs) void {
    var flag = args.is_d1;
    const client = orusconnect.BrokerClient.init(
        .{ .host = "127.0.0.1", .port = BROKER_PORT }, args.io,
    );
    while (true) {
        client.consume(args.topic, std.heap.page_allocator, onMsg, &flag) catch {};
        _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 10_000_000 }, null);
    }
}

// ── Gateway worker (compliance path) ─────────────────────────────────────────
// Génère des InternalMessage, les envoie via Gateway → FakeBank (ISO 8583 réel).
// Chaque transaction passe par MAC F64 + audit log fsync.

const GwWorkerArgs = struct {
    gateway:   *gw_mod.Gateway,
    audit_log: *orusshare.AuditLog,
};

fn gatewayWorker(args: GwWorkerArgs) void {
    const alloc = std.heap.page_allocator;
    var prng: u64 = undefined;
    std.c.arc4random_buf(@ptrCast(&prng), @sizeOf(u64));

    while (true) {
        prng ^= prng << 13; prng ^= prng >> 7; prng ^= prng << 17;
        var id: [16]u8 = undefined;
        std.c.arc4random_buf(&id, id.len);

        const is_reversal = (prng % 20 == 0);
        const amount: i64 = @intCast(1_000 + prng % 99_000);

        var stan_buf: [6]u8 = undefined;
        _ = std.fmt.bufPrint(&stan_buf, "{d:0>6}", .{prng % 1_000_000}) catch continue;

        const req = orusshare.InternalMessage{
            .msg_id      = id,
            .schema_id   = "gimac",
            .topic       = TOPIC_D1,
            .origin      = .rest_json,
            .provider    = .mtn_momo,
            .mti         = if (is_reversal) "0200".* else "0200".*,
            .fields      = &.{},
            .stan        = stan_buf,
            .pan_hash    = prng,
            .amount      = amount,
            .currency    = "XAF".*,
            .received_at = 0,
            .source_ip   = [_]u8{0} ** 16,
            .hop_count   = 1,
            .external_id = null,
        };

        const resp = blk: {
            if (is_reversal) {
                break :blk args.gateway.processReversal(&req, alloc) catch continue;
            } else {
                break :blk args.gateway.process(&req, alloc) catch continue;
            }
        };
        defer {
            for (resp.fields) |f| alloc.free(f.value);
            alloc.free(resp.fields);
        }

        // Audit log — enregistre chaque transaction traitée.
        var rc: []const u8 = "";
        for (resp.fields) |f| if (f.id == 39) { rc = f.value; };
        args.audit_log.record(
            &resp.mti, &resp.stan, resp.amount, &resp.currency,
            rc, @tagName(resp.provider), resp.msg_id,
        );

        _ = g_gw_proc.fetchAdd(1, .monotonic);
    }
}

// ── Ticker (percentiles + historique + heartbeats) ────────────────────────────

const TickerArgs = struct {
    gateway:   *gw_mod.Gateway,
    stan:      *u32,
};

fn ticker(args: TickerArgs) void {
    const alloc = std.heap.page_allocator;
    var prev_d1:  u64 = 0;
    var prev_d2:  u64 = 0;
    var prev_rev: u64 = 0;
    var prev_gw:  u64 = 0;
    var tick_count: u64 = 0;

    while (true) {
        _ = std.c.nanosleep(&.{ .sec = 1, .nsec = 0 }, null);
        tick_count += 1;

        // Percentiles de latence — calculés avant le seqlock, stockés dedans
        // pour que le handler HTTP lise toujours P50 et P99 de la même fenêtre.
        var p50_val: u64 = 0;
        var p99_val: u64 = 0;
        const n_raw = g_lat_idx.load(.monotonic);
        const n = @min(n_raw, LAT_CAP);
        if (n > 1) {
            var tmp: [LAT_CAP]u64 = undefined;
            @memcpy(tmp[0..n], g_lat[0..n]);
            std.mem.sort(u64, tmp[0..n], {}, std.sort.asc(u64));
            p50_val = tmp[n / 2] / 1000;
            p99_val = tmp[n * 99 / 100] / 1000;
        }

        const d1  = g_d1_del.load(.monotonic);
        const d2  = g_d2_del.load(.monotonic);
        const rev = g_rev_del.load(.monotonic);
        const gw  = g_gw_proc.load(.monotonic);
        const tps1   = d1  - prev_d1;
        const tps2   = d2  - prev_d2;
        const tpsrev = rev - prev_rev;
        const tpsgw  = gw  - prev_gw;
        prev_d1  = d1;
        prev_d2  = d2;
        prev_rev = rev;
        prev_gw  = gw;

        _ = g_hist_gen.fetchAdd(1, .release);
        const head = g_hist_head.load(.monotonic);
        g_hist_d1[head]  = tps1;
        g_hist_d2[head]  = tps2;
        g_hist_rev[head] = tpsrev;
        g_hist_gw[head]  = tpsgw;
        g_hist_head.store((head + 1) % 60, .monotonic);
        const cur_n = g_hist_n.load(.monotonic);
        if (cur_n < 60) _ = g_hist_n.fetchAdd(1, .monotonic);
        // Stocker P50/P99 à l'intérieur du seqlock : le reader HTTP
        // verra toujours une paire (P50, P99) cohérente (même fenêtre).
        g_p50_us.store(p50_val, .monotonic);
        g_p99_us.store(p99_val, .monotonic);
        _ = g_hist_gen.fetchAdd(1, .release);

        // Heartbeat 0800/801 toutes les 30 s.
        if (tick_count % 30 == 0 and g_signon_ok.load(.monotonic)) {
            args.gateway.sendHeartbeat(args.stan, alloc) catch {};
            _ = g_heartbeats.fetchAdd(1, .monotonic);
        }
    }
}

// ── HTTP ──────────────────────────────────────────────────────────────────────

const HttpCtx = struct { stream: std.Io.net.Stream, io: std.Io, wal_path: []const u8 };

fn httpConn(ctx: HttpCtx) void {
    defer ctx.stream.close(ctx.io);
    var rbuf: [2048]u8 = undefined;
    var sr = ctx.stream.reader(ctx.io, &rbuf);
    var line: [256]u8 = undefined;
    var ln: usize = 0;
    while (ln < line.len) {
        const b = sr.interface.takeByte() catch return;
        if (b == '\n') break;
        if (b != '\r') { line[ln] = b; ln += 1; }
    }
    var tail: [4]u8 = .{ 0, 0, 0, 0 };
    while (true) {
        const b = sr.interface.takeByte() catch break;
        tail[0] = tail[1]; tail[1] = tail[2]; tail[2] = tail[3]; tail[3] = b;
        if (std.mem.eql(u8, &tail, "\r\n\r\n")) break;
    }
    const path = line[0..ln];
    if (std.mem.startsWith(u8, path, "GET /metrics")) {
        serveJson(ctx.stream, ctx.io, ctx.wal_path);
    } else if (std.mem.startsWith(u8, path, "GET / ") or std.mem.eql(u8, path, "GET /")) {
        serveHtml(ctx.stream, ctx.io);
    } else {
        var wbuf: [128]u8 = undefined;
        var sw = ctx.stream.writer(ctx.io, &wbuf);
        sw.interface.writeAll("HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n") catch {};
        sw.interface.flush() catch {};
    }
}

fn wp(buf: []u8, pos: *usize, comptime fmt: []const u8, args: anytype) void {
    const s = std.fmt.bufPrint(buf[pos.*..], fmt, args) catch return;
    pos.* += s.len;
}

fn serveJson(stream: std.Io.net.Stream, io: std.Io, wal_path: []const u8) void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const now_ns: i64 = ts.sec *% std.time.ns_per_s + ts.nsec;
    const elapsed: u64 = @intCast(@max(0, @divTrunc(now_ns - g_start_ns, std.time.ns_per_s)));

    var hd1:  [60]u64 = undefined;
    var hd2:  [60]u64 = undefined;
    var hrev: [60]u64 = undefined;
    var hgw:  [60]u64 = undefined;
    var hn:     usize = 0;
    var p50_snap: u64 = 0;
    var p99_snap: u64 = 0;
    while (true) {
        const gen1 = g_hist_gen.load(.acquire);
        if (gen1 % 2 != 0) continue;
        hn = g_hist_n.load(.monotonic);
        const hstart: usize = if (hn < 60) 0 else g_hist_head.load(.monotonic);
        for (0..hn) |i| {
            const idx = (hstart + i) % 60;
            hd1[i]  = g_hist_d1[idx];
            hd2[i]  = g_hist_d2[idx];
            hrev[i] = g_hist_rev[idx];
            hgw[i]  = g_hist_gw[idx];
        }
        // Lire P50/P99 dans la même section protégée : garantit que
        // les deux valeurs proviennent de la même fenêtre de tri.
        p50_snap = g_p50_us.load(.monotonic);
        p99_snap = g_p99_us.load(.monotonic);
        const gen2 = g_hist_gen.load(.acquire);
        if (gen1 == gen2) break;
    }

    const d1_tps  = if (hn > 0) hd1[hn - 1]  else 0;
    const d2_tps  = if (hn > 0) hd2[hn - 1]  else 0;
    const rev_tps = if (hn > 0) hrev[hn - 1] else 0;
    const gw_tps  = if (hn > 0) hgw[hn - 1]  else 0;

    const wal_stat = std.Io.Dir.cwd().statFile(io, wal_path, .{}) catch null;
    const wal_mb: f64 = if (wal_stat) |st|
        @as(f64, @floatFromInt(st.size)) / (1024.0 * 1024.0) else 0;
    const audit_stat = std.Io.Dir.cwd().statFile(io, AUDIT_LOG_PATH, .{}) catch null;
    const audit_kb: u64 = if (audit_stat) |st| st.size / 1024 else 0;

    var ru: std.c.rusage = undefined;
    _ = std.c.getrusage(0, &ru);
    const raw_rss: usize = @intCast(ru.maxrss);
    const rss_mb = if (builtin.os.tag == .macos) raw_rss / (1024*1024) else raw_rss / 1024;

    var body: [8192]u8 = undefined;
    var pos:  usize    = 0;

    wp(&body, &pos, "{{", .{});
    wp(&body, &pos, "\"elapsed_s\":{d}",       .{elapsed});
    wp(&body, &pos, ",\"d1_tps\":{d}",         .{d1_tps});
    wp(&body, &pos, ",\"d2_tps\":{d}",         .{d2_tps});
    wp(&body, &pos, ",\"rev_tps\":{d}",        .{rev_tps});
    wp(&body, &pos, ",\"gw_tps\":{d}",         .{gw_tps});
    wp(&body, &pos, ",\"d1_total\":{d}",       .{g_d1_del.load(.monotonic)});
    wp(&body, &pos, ",\"d2_total\":{d}",       .{g_d2_del.load(.monotonic)});
    wp(&body, &pos, ",\"rev_total\":{d}",      .{g_rev_del.load(.monotonic)});
    wp(&body, &pos, ",\"gw_total\":{d}",       .{g_gw_proc.load(.monotonic)});
    wp(&body, &pos, ",\"pub_errors\":{d}",     .{g_pub_errors.load(.monotonic)});
    wp(&body, &pos, ",\"p50_us\":{d}",         .{p50_snap});
    wp(&body, &pos, ",\"p99_us\":{d}",         .{p99_snap});
    wp(&body, &pos, ",\"rss_mb\":{d}",         .{rss_mb});
    wp(&body, &pos, ",\"wal_mb\":{d:.1}",      .{wal_mb});
    wp(&body, &pos, ",\"audit_kb\":{d}",       .{audit_kb});
    wp(&body, &pos, ",\"heartbeats\":{d}",     .{g_heartbeats.load(.monotonic)});
    wp(&body, &pos, ",\"signon_ok\":{s}",      .{if (g_signon_ok.load(.monotonic)) "true" else "false"});
    wp(&body, &pos, ",\"hist_d1\":[",          .{});
    for (0..hn) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{hd1[i]});
    }
    wp(&body, &pos, "],\"hist_d2\":[", .{});
    for (0..hn) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{hd2[i]});
    }
    wp(&body, &pos, "],\"hist_rev\":[", .{});
    for (0..hn) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{hrev[i]});
    }
    wp(&body, &pos, "],\"hist_gw\":[", .{});
    for (0..hn) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{hgw[i]});
    }
    wp(&body, &pos, "]}}", .{});

    const body_s = body[0..pos];
    var wbuf: [512]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    var hdr: [256]u8 = undefined;
    const hdr_s = std.fmt.bufPrint(&hdr,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{body_s.len},
    ) catch return;
    sw.interface.writeAll(hdr_s) catch return;
    sw.interface.writeAll(body_s) catch return;
    sw.interface.flush() catch {};
}

fn serveHtml(stream: std.Io.net.Stream, io: std.Io) void {
    var wbuf: [8192]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    var hdr: [256]u8 = undefined;
    const hdr_s = std.fmt.bufPrint(&hdr,
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{DASHBOARD_HTML.len},
    ) catch return;
    sw.interface.writeAll(hdr_s) catch return;
    sw.interface.writeAll(DASHBOARD_HTML) catch return;
    sw.interface.flush() catch {};
}

const HttpSrvArgs = struct { io: std.Io, wal_path: []const u8 };

fn httpServer(args: HttpSrvArgs) void {
    const addr = std.Io.net.IpAddress.parse("0.0.0.0", HTTP_PORT) catch return;
    var server = addr.listen(args.io, .{ .reuse_address = true }) catch {
        std.log.err("HTTP: port {d} indisponible", .{HTTP_PORT});
        return;
    };
    defer server.deinit(args.io);
    std.log.info("Dashboard : \x1b[1;36mhttp://localhost:{d}\x1b[0m  (Ctrl+C pour quitter)", .{HTTP_PORT});
    while (!g_stopped.load(.monotonic)) {
        const stream = server.accept(args.io) catch {
            if (g_stopped.load(.monotonic)) break;
            continue;
        };
        const ctx = HttpCtx{ .stream = stream, .io = args.io, .wal_path = args.wal_path };
        const t = std.Thread.spawn(.{}, httpConn, .{ctx}) catch {
            stream.close(args.io);
            continue;
        };
        t.detach();
    }
}

// ── Récapitulatif final ───────────────────────────────────────────────────────

fn printRecap(wal_path: []const u8, io: std.Io) void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const now_ns: i64 = ts.sec *% std.time.ns_per_s + ts.nsec;
    const elapsed_s: u64 = @intCast(@max(1, @divTrunc(now_ns - g_start_ns, std.time.ns_per_s)));

    const d1     = g_d1_del.load(.monotonic);
    const d2     = g_d2_del.load(.monotonic);
    const rev    = g_rev_del.load(.monotonic);
    const gw     = g_gw_proc.load(.monotonic);
    const errors = g_pub_errors.load(.monotonic);
    const hbs    = g_heartbeats.load(.monotonic);

    const n_raw = g_lat_idx.load(.monotonic);
    const n = @min(n_raw, LAT_CAP);
    var p50: u64 = 0; var p95: u64 = 0; var p99: u64 = 0; var pmax: u64 = 0;
    if (n > 1) {
        var tmp: [LAT_CAP]u64 = undefined;
        @memcpy(tmp[0..n], g_lat[0..n]);
        std.mem.sort(u64, tmp[0..n], {}, std.sort.asc(u64));
        p50  = tmp[n / 2]        / 1000;
        p95  = tmp[n * 95 / 100] / 1000;
        p99  = tmp[n * 99 / 100] / 1000;
        pmax = tmp[n - 1]        / 1000;
    }

    const wal_stat = std.Io.Dir.cwd().statFile(io, wal_path, .{}) catch null;
    const wal_mb: f64 = if (wal_stat) |st|
        @as(f64, @floatFromInt(st.size)) / (1024.0 * 1024.0) else 0;
    const audit_stat = std.Io.Dir.cwd().statFile(io, AUDIT_LOG_PATH, .{}) catch null;
    const audit_kb: u64 = if (audit_stat) |st| st.size / 1024 else 0;

    var ru: std.c.rusage = undefined;
    _ = std.c.getrusage(0, &ru);
    const raw_rss: usize = @intCast(ru.maxrss);
    const rss_mb = if (builtin.os.tag == .macos) raw_rss / (1024*1024) else raw_rss / 1024;

    std.debug.print("\n\n\x1b[1;36m══════════════════════════════════════════════════════\x1b[0m\n", .{});
    std.debug.print("\x1b[1mRÉCAPITULATIF — {d}s de fonctionnement\x1b[0m\n", .{elapsed_s});
    std.debug.print("\x1b[1;36m══════════════════════════════════════════════════════\x1b[0m\n\n", .{});

    std.debug.print("  \x1b[1mBroker throughput\x1b[0m\n", .{});
    std.debug.print("    D1 MoMo→Bank livré : \x1b[36m{d:>10}\x1b[0m msgs  (\x1b[1;36m{d}\x1b[0m tx/s moy)\n",
        .{ d1, d1 / elapsed_s });
    std.debug.print("    D2 Bank→MoMo livré : \x1b[33m{d:>10}\x1b[0m msgs  (\x1b[1;33m{d}\x1b[0m tx/s moy)\n",
        .{ d2, d2 / elapsed_s });
    std.debug.print("    Reversals 0420     : \x1b[31m{d:>10}\x1b[0m msgs  (~{d}% du trafic)\n",
        .{ rev, if (d1 + d2 + rev > 0) rev * 100 / (d1 + d2 + rev) else 0 });
    std.debug.print("    Erreurs pub        : \x1b[{s}m{d:>10}\x1b[0m\n\n",
        .{ if (errors == 0) "32" else "31", errors });

    std.debug.print("  \x1b[1mGateway compliance (ISO 8583 + MAC + audit)\x1b[0m\n", .{});
    std.debug.print("    Transactions traitées : \x1b[32m{d:>10}\x1b[0m  ({d} tx/s moy)\n",
        .{ gw, gw / elapsed_s });
    std.debug.print("    Heartbeats envoyés    : \x1b[32m{d:>10}\x1b[0m  (toutes les 30 s)\n", .{hbs});
    std.debug.print("    Sign-on 0800/301      : {s}\n",
        .{if (g_signon_ok.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    Audit log             : {s}  ({d} KB)\n\n", .{ AUDIT_LOG_PATH, audit_kb });

    if (n > 1) {
        std.debug.print("  \x1b[1mLatence broker (publish → deliver)\x1b[0m  [{d} échantillons]\n", .{n});
        std.debug.print("    P50  : \x1b[32m{d:>6} µs  ({d:.2} ms)\x1b[0m\n",
            .{ p50,  @as(f64, @floatFromInt(p50))  / 1000.0 });
        std.debug.print("    P95  : \x1b[33m{d:>6} µs  ({d:.2} ms)\x1b[0m\n",
            .{ p95,  @as(f64, @floatFromInt(p95))  / 1000.0 });
        std.debug.print("    P99  : \x1b[33m{d:>6} µs  ({d:.2} ms)\x1b[0m\n",
            .{ p99,  @as(f64, @floatFromInt(p99))  / 1000.0 });
        std.debug.print("    Max  : \x1b[31m{d:>6} µs  ({d:.2} ms)\x1b[0m\n\n",
            .{ pmax, @as(f64, @floatFromInt(pmax)) / 1000.0 });
    }

    std.debug.print("  \x1b[1mRessources\x1b[0m\n", .{});
    std.debug.print("    Mémoire RSS : {d} MB\n",     .{rss_mb});
    std.debug.print("    WAL écrit   : {d:.1} MB\n",  .{wal_mb});
    std.debug.print("    Audit log   : {d} KB\n\n",   .{audit_kb});

    std.debug.print("\x1b[1;36m══════════════════════════════════════════════════════\x1b[0m\n\n", .{});
}

// ── Dashboard HTML ────────────────────────────────────────────────────────────

const DASHBOARD_HTML =
    \\<!DOCTYPE html>
    \\<html lang="fr">
    \\<head>
    \\<meta charset="UTF-8">
    \\<meta name="viewport" content="width=device-width,initial-scale=1">
    \\<title>OrusBroker — Pipeline Live</title>
    \\<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    \\<style>
    \\*{box-sizing:border-box;margin:0;padding:0}
    \\body{background:#0d0d1a;color:#ccc;font-family:ui-monospace,'Courier New',monospace;padding:24px}
    \\h1{color:#00d4ff;font-size:1.3em;margin-bottom:4px}
    \\.sub{color:#555;font-size:.8em;margin-bottom:22px}
    \\.sub b{color:#888}
    \\.cards{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:22px}
    \\.card{background:#111120;border:1px solid #1e1e38;border-radius:8px;padding:14px 18px;min-width:150px}
    \\.v{font-size:1.75em;font-weight:700;line-height:1.1}
    \\.l{font-size:.72em;color:#555;margin-top:3px}
    \\.c1{color:#00d4ff}.c2{color:#ff6b35}.cg{color:#44dd88}.cr{color:#ff4444}.cn{color:#aaa}.cw{color:#ffcc00}
    \\.box{background:#111120;border:1px solid #1e1e38;border-radius:8px;padding:20px;margin-bottom:14px}
    \\.box h2{font-size:.78em;color:#555;margin-bottom:14px;letter-spacing:.06em;text-transform:uppercase}
    \\canvas{max-height:220px}
    \\.footer{color:#333;font-size:.72em;margin-top:18px}
    \\</style>
    \\</head>
    \\<body>
    \\<h1>&#9889; OrusBroker &mdash; Pipeline Live</h1>
    \\<p class="sub">Durée : <b id="el">--:--:--</b> &nbsp;&middot;&nbsp; <span id="ts">connexion...</span></p>
    \\<div class="cards">
    \\  <div class="card"><div class="v c1" id="c1">—</div><div class="l">D1 tx/s &nbsp;MoMo&#8594;Bank</div></div>
    \\  <div class="card"><div class="v c2" id="c2">—</div><div class="l">D2 tx/s &nbsp;Bank&#8594;MoMo</div></div>
    \\  <div class="card"><div class="v cr" id="crv">—</div><div class="l">Rev/s &nbsp;0420 reversals</div></div>
    \\  <div class="card"><div class="v cw" id="cgw">—</div><div class="l">Gateway tx/s (MAC+audit)</div></div>
    \\  <div class="card"><div class="v cn" id="ct">—</div><div class="l">Total livré</div></div>
    \\  <div class="card"><div class="v cg" id="p50">—</div><div class="l">Latence P50</div></div>
    \\  <div class="card"><div class="v cg" id="p99">—</div><div class="l">Latence P99</div></div>
    \\  <div class="card"><div class="v cg" id="ce">—</div><div class="l">Erreurs pub</div></div>
    \\  <div class="card"><div class="v cg" id="sg">—</div><div class="l">Sign-on 0800/301</div></div>
    \\  <div class="card"><div class="v cg" id="hb">—</div><div class="l">Heartbeats 0800/801</div></div>
    \\  <div class="card"><div class="v cn" id="ak">—</div><div class="l">Audit log</div></div>
    \\  <div class="card"><div class="v cn" id="rs">—</div><div class="l">Mémoire RSS</div></div>
    \\  <div class="card"><div class="v cn" id="wl">—</div><div class="l">WAL écrit</div></div>
    \\</div>
    \\<div class="box">
    \\  <h2>Throughput &mdash; messages / seconde (60 dernières secondes)</h2>
    \\  <canvas id="ch"></canvas>
    \\</div>
    \\<p class="footer">OrusInfra &middot; broker:7797 &middot; fakebank:5097 &middot; dashboard:7780</p>
    \\<script>
    \\let ch=null;
    \\const f=n=>n.toLocaleString('fr-FR');
    \\const fms=us=>(us/1000).toFixed(2)+' ms';
    \\const ft=s=>[Math.floor(s/3600),Math.floor(s%3600/60),s%60].map(x=>String(x).padStart(2,'0')).join(':');
    \\window.onload=()=>{
    \\  const ctx=document.getElementById('ch').getContext('2d');
    \\  ch=new Chart(ctx,{type:'line',data:{labels:[],datasets:[
    \\    {label:'D1 MoMo→Bank',data:[],borderColor:'#00d4ff',backgroundColor:'rgba(0,212,255,.07)',tension:.3,fill:true,pointRadius:0,borderWidth:2},
    \\    {label:'D2 Bank→MoMo',data:[],borderColor:'#ff6b35',backgroundColor:'rgba(255,107,53,.07)',tension:.3,fill:true,pointRadius:0,borderWidth:2},
    \\    {label:'Reversals 0420',data:[],borderColor:'#ff4444',backgroundColor:'rgba(255,68,68,.07)',tension:.3,fill:true,pointRadius:0,borderWidth:2},
    \\    {label:'Gateway (MAC+audit)',data:[],borderColor:'#ffcc00',backgroundColor:'rgba(255,204,0,.07)',tension:.3,fill:true,pointRadius:0,borderWidth:2}
    \\  ]},options:{animation:false,responsive:true,
    \\    scales:{
    \\      y:{beginAtZero:true,grid:{color:'#1a1a2e'},ticks:{color:'#555',callback:v=>f(v)}},
    \\      x:{grid:{color:'#1a1a2e'},ticks:{color:'#555',maxTicksLimit:10}}
    \\    },
    \\    plugins:{
    \\      legend:{labels:{color:'#888',boxWidth:10,font:{size:11}}},
    \\      tooltip:{backgroundColor:'#1a1a2e',titleColor:'#888',bodyColor:'#ccc'}
    \\    }
    \\  }});
    \\  tick();setInterval(tick,1000);
    \\};
    \\async function tick(){
    \\  try{
    \\    const m=await(await fetch('/metrics')).json();
    \\    document.getElementById('c1').textContent=f(m.d1_tps);
    \\    document.getElementById('c2').textContent=f(m.d2_tps);
    \\    document.getElementById('crv').textContent=f(m.rev_tps);
    \\    document.getElementById('cgw').textContent=f(m.gw_tps);
    \\    document.getElementById('ct').textContent=f(m.d1_total+m.d2_total+m.rev_total+m.gw_total);
    \\    document.getElementById('p50').textContent=fms(m.p50_us);
    \\    document.getElementById('p99').textContent=fms(m.p99_us);
    \\    const ce=document.getElementById('ce');
    \\    ce.textContent=f(m.pub_errors);ce.className='v '+(m.pub_errors>0?'cr':'cg');
    \\    const sg=document.getElementById('sg');
    \\    sg.textContent=m.signon_ok?'✓ actif':'✗ inactif';sg.className='v '+(m.signon_ok?'cg':'cr');
    \\    document.getElementById('hb').textContent=f(m.heartbeats);
    \\    document.getElementById('ak').textContent=m.audit_kb+' KB';
    \\    document.getElementById('rs').textContent=m.rss_mb+' MB';
    \\    document.getElementById('wl').textContent=m.wal_mb.toFixed(1)+' MB';
    \\    document.getElementById('el').textContent=ft(m.elapsed_s);
    \\    document.getElementById('ts').textContent=new Date().toLocaleTimeString();
    \\    const n=m.hist_d1.length;
    \\    ch.data.labels=m.hist_d1.map((_,i)=>i===n-1?'now':'-'+(n-1-i)+'s');
    \\    ch.data.datasets[0].data=m.hist_d1;
    \\    ch.data.datasets[1].data=m.hist_d2;
    \\    ch.data.datasets[2].data=m.hist_rev;
    \\    ch.data.datasets[3].data=m.hist_gw;
    \\    ch.update('none');
    \\  }catch(e){document.getElementById('ts').textContent='⚠ '+e.message;}
    \\}
    \\</script>
    \\</body>
    \\</html>
;

// ── main ──────────────────────────────────────────────────────────────────────

pub fn main() !void {
    const pub_threads: usize = blk: {
        const s = std.c.getenv("PIPELINE_PUB_THREADS") orelse break :blk 4;
        break :blk std.fmt.parseInt(usize, std.mem.span(s), 10) catch 4;
    };
    const gw_threads: usize = blk: {
        const s = std.c.getenv("PIPELINE_GW_THREADS") orelse break :blk 2;
        break :blk std.fmt.parseInt(usize, std.mem.span(s), 10) catch 2;
    };

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    const wal_path = "/tmp/pipeline_bench.wal";
    std.Io.Dir.cwd().deleteFile(io, wal_path) catch {};

    var ts0: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts0);
    g_start_ns = ts0.sec *% std.time.ns_per_s + ts0.nsec;

    // ── Broker ────────────────────────────────────────────────────────────────
    const inner = try std.heap.page_allocator.create(BrokerInner);
    inner.wal    = try orusbroker.Wal.open(io, wal_path);
    inner.dedup  = orusbroker.DedupFilter.init();
    inner.router = orusbroker.TopicRouter.init(std.heap.page_allocator);
    const broker_cfg = orusbroker.Config{
        .host = "127.0.0.1", .port = BROKER_PORT,
        .wal_path = wal_path, .cursor_dir = "/tmp/pipeline_cursors",
    };
    inner.server = orusbroker.BrokerServer{
        .config = broker_cfg, .io = io,
        .wal = &inner.wal, .dedup = &inner.dedup, .router = &inner.router,
        .cursor = orusbroker.Cursor.init(broker_cfg.cursor_dir, io),
    };
    (try std.Thread.spawn(.{}, runBroker, .{inner})).detach();
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 80_000_000 }, null);

    // ── FakeBank ──────────────────────────────────────────────────────────────
    const bank_addr = try std.Io.net.IpAddress.parse("127.0.0.1", FAKE_BANK_PORT);
    var bank_server_sock = try bank_addr.listen(io, .{ .reuse_address = true });
    var fake_bank_args = FakeBankArgs{ .server = &bank_server_sock, .io = io };
    (try std.Thread.spawn(.{}, runFakeBank, .{&fake_bank_args})).detach();

    // ── MacEngine + AuditLog ──────────────────────────────────────────────────
    var mac_engine = gw_mod.MacEngine.init();
    std.Io.Dir.cwd().deleteFile(io, AUDIT_LOG_PATH) catch {};
    var audit_log = try orusshare.AuditLog.open(io, AUDIT_LOG_PATH);
    audit_log.sync_every = 64; // bench: sync every 64 records instead of every write
    defer audit_log.close();

    // ── Gateway + Sign-on ─────────────────────────────────────────────────────
    const gateway = try std.heap.page_allocator.create(gw_mod.Gateway);
    gateway.* = gw_mod.Gateway.init(gw_mod.BankClient.init(.{
        .host = "127.0.0.1", .port = FAKE_BANK_PORT, .length_prefix = .two_byte_be,
    }, io), 0);
    gateway.mac_engine = &mac_engine;

    var stan: u32 = 0;
    gateway.signOn(&stan, alloc) catch |err| {
        std.log.warn("sign-on échoué: {} — MAC désactivé pour ce bench", .{err});
    };
    if (mac_engine.active) {
        g_signon_ok.store(true, .monotonic);
        std.log.info("Sign-on 0800/301 ✓ — MacEngine actif (HMAC-SHA256/8 octets)", .{});
    }

    // ── Subscribers (broker throughput path) ──────────────────────────────────
    for (0..2) |_| {
        (try std.Thread.spawn(.{}, subLoop, .{SubArgs{ .io = io, .topic = TOPIC_D1, .is_d1 = true  }})).detach();
        (try std.Thread.spawn(.{}, subLoop, .{SubArgs{ .io = io, .topic = TOPIC_D2, .is_d1 = false }})).detach();
    }
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);

    // ── Publishers (broker throughput path) ───────────────────────────────────
    for (0..pub_threads) |_| {
        (try std.Thread.spawn(.{}, publishLoop, .{PubArgs{ .io = io, .topic = TOPIC_D1, .is_d1 = true  }})).detach();
        (try std.Thread.spawn(.{}, publishLoop, .{PubArgs{ .io = io, .topic = TOPIC_D2, .is_d1 = false }})).detach();
    }

    // ── Gateway workers (compliance path) ────────────────────────────────────
    const gw_args = GwWorkerArgs{ .gateway = gateway, .audit_log = &audit_log };
    for (0..gw_threads) |_| {
        (try std.Thread.spawn(.{}, gatewayWorker, .{gw_args})).detach();
    }

    // ── Ticker (heartbeats toutes les 30 s) ───────────────────────────────────
    const ticker_args = TickerArgs{ .gateway = gateway, .stan = &stan };
    (try std.Thread.spawn(.{}, ticker, .{ticker_args})).detach();

    // Handler SIGINT/SIGTERM.
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = sigHandler },
        .mask    = std.posix.sigemptyset(),
        .flags   = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT,  &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    // HTTP server — bloque jusqu'au Ctrl+C.
    httpServer(.{ .io = io, .wal_path = wal_path });

    printRecap(wal_path, io);
    std.c._exit(0);
}
