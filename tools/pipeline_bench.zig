// Pipeline complet avec dashboard web en temps réel.
// Lance : zig build pipeline
// Ouvre : http://localhost:7780

const std        = @import("std");
const orusshare  = @import("orusshare");
const orusbroker = @import("orusbroker");
const orusconnect = @import("orusconnect");
const builtin    = @import("builtin");

const BROKER_PORT: u16 = 7797;
const HTTP_PORT:   u16 = 7780;
const TOPIC_D1 = "pipeline.inbound";
const TOPIC_D2 = "pipeline.outbound";

// ── Métriques atomiques ───────────────────────────────────────────────────────

var g_d1_del     = std.atomic.Value(u64).init(0);
var g_d2_del     = std.atomic.Value(u64).init(0);
var g_rev_del    = std.atomic.Value(u64).init(0); // reversals (0420/0421) livrés
var g_pub_errors = std.atomic.Value(u64).init(0);
var g_start_ns:  i64 = 0;

const LAT_CAP = 2000;
var g_lat:     [LAT_CAP]u64 = undefined;
var g_lat_idx  = std.atomic.Value(usize).init(0);
var g_p50_us   = std.atomic.Value(u64).init(0);
var g_p99_us   = std.atomic.Value(u64).init(0);

// Signal — Ctrl+C pose ce flag pour sortir proprement et afficher le récap.
var g_stopped = std.atomic.Value(bool).init(false);

fn sigHandler(sig: std.c.SIG) callconv(.c) void {
    _ = sig;
    g_stopped.store(true, .monotonic);
}

// Seqlock léger : gen est impair pendant l'écriture, pair sinon.
// Un seul thread écrit (ticker), les lecteurs HTTP réessaient si gen change.
var g_hist_gen  = std.atomic.Value(usize).init(0);
var g_hist_d1:   [60]u64 = [_]u64{0} ** 60;
var g_hist_d2:   [60]u64 = [_]u64{0} ** 60;
var g_hist_rev:  [60]u64 = [_]u64{0} ** 60;
var g_hist_head  = std.atomic.Value(usize).init(0);
var g_hist_n     = std.atomic.Value(usize).init(0);

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

// ── Publisher ─────────────────────────────────────────────────────────────────

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

        // 5% des messages sont des reversals (0420) pour mesurer l'overhead réel.
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

// ── Subscriber ────────────────────────────────────────────────────────────────

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

// ── Ticker (percentiles + historique, 1 Hz) ───────────────────────────────────

fn ticker(_: u8) void {
    var prev_d1:  u64 = 0;
    var prev_d2:  u64 = 0;
    var prev_rev: u64 = 0;

    while (true) {
        _ = std.c.nanosleep(&.{ .sec = 1, .nsec = 0 }, null);

        const n_raw = g_lat_idx.load(.monotonic);
        const n = @min(n_raw, LAT_CAP);
        if (n > 1) {
            var tmp: [LAT_CAP]u64 = undefined;
            @memcpy(tmp[0..n], g_lat[0..n]);
            std.mem.sort(u64, tmp[0..n], {}, std.sort.asc(u64));
            g_p50_us.store(tmp[n / 2] / 1000, .monotonic);
            g_p99_us.store(tmp[n * 99 / 100] / 1000, .monotonic);
        }

        const d1  = g_d1_del.load(.monotonic);
        const d2  = g_d2_del.load(.monotonic);
        const rev = g_rev_del.load(.monotonic);
        const tps1  = d1  - prev_d1;
        const tps2  = d2  - prev_d2;
        const tpsrev = rev - prev_rev;
        prev_d1  = d1;
        prev_d2  = d2;
        prev_rev = rev;

        _ = g_hist_gen.fetchAdd(1, .release);
        const head = g_hist_head.load(.monotonic);
        g_hist_d1[head]  = tps1;
        g_hist_d2[head]  = tps2;
        g_hist_rev[head] = tpsrev;
        g_hist_head.store((head + 1) % 60, .monotonic);
        const cur_n = g_hist_n.load(.monotonic);
        if (cur_n < 60) _ = g_hist_n.fetchAdd(1, .monotonic);
        _ = g_hist_gen.fetchAdd(1, .release);
    }
}

// ── HTTP ──────────────────────────────────────────────────────────────────────

const HttpCtx = struct { stream: std.Io.net.Stream, io: std.Io, wal_path: []const u8 };

fn httpConn(ctx: HttpCtx) void {
    const stream = ctx.stream;
    const io     = ctx.io;
    defer stream.close(io);

    var rbuf: [2048]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    const r = &sr.interface;

    var line: [256]u8 = undefined;
    var ln: usize = 0;
    while (ln < line.len) {
        const b = r.takeByte() catch return;
        if (b == '\n') break;
        if (b != '\r') { line[ln] = b; ln += 1; }
    }
    // Drainer les en-têtes.
    var tail: [4]u8 = .{ 0, 0, 0, 0 };
    while (true) {
        const b = r.takeByte() catch break;
        tail[0] = tail[1]; tail[1] = tail[2]; tail[2] = tail[3]; tail[3] = b;
        if (std.mem.eql(u8, &tail, "\r\n\r\n")) break;
    }

    const path = line[0..ln];
    if (std.mem.startsWith(u8, path, "GET /metrics")) {
        serveJson(stream, io, ctx.wal_path);
    } else if (std.mem.startsWith(u8, path, "GET / ") or std.mem.eql(u8, path, "GET /")) {
        serveHtml(stream, io);
    } else {
        var wbuf: [128]u8 = undefined;
        var sw = stream.writer(io, &wbuf);
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
    var hn:   usize   = 0;
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
        }
        const gen2 = g_hist_gen.load(.acquire);
        if (gen1 == gen2) break;
    }

    const d1_tps  = if (hn > 0) hd1[hn - 1]  else 0;
    const d2_tps  = if (hn > 0) hd2[hn - 1]  else 0;
    const rev_tps = if (hn > 0) hrev[hn - 1] else 0;

    const wal_stat = std.Io.Dir.cwd().statFile(io, wal_path, .{}) catch null;
    const wal_mb: f64 = if (wal_stat) |st|
        @as(f64, @floatFromInt(st.size)) / (1024.0 * 1024.0) else 0;

    var ru: std.c.rusage = undefined;
    _ = std.c.getrusage(0, &ru);
    const raw_rss: usize = @intCast(ru.maxrss);
    const rss_mb = if (builtin.os.tag == .macos) raw_rss / (1024*1024) else raw_rss / 1024;

    var body: [8192]u8 = undefined;
    var pos:  usize    = 0;

    wp(&body, &pos, "{{", .{});
    wp(&body, &pos, "\"elapsed_s\":{d}",    .{elapsed});
    wp(&body, &pos, ",\"d1_tps\":{d}",      .{d1_tps});
    wp(&body, &pos, ",\"d2_tps\":{d}",      .{d2_tps});
    wp(&body, &pos, ",\"rev_tps\":{d}",     .{rev_tps});
    wp(&body, &pos, ",\"d1_total\":{d}",    .{g_d1_del.load(.monotonic)});
    wp(&body, &pos, ",\"d2_total\":{d}",    .{g_d2_del.load(.monotonic)});
    wp(&body, &pos, ",\"rev_total\":{d}",   .{g_rev_del.load(.monotonic)});
    wp(&body, &pos, ",\"pub_errors\":{d}",  .{g_pub_errors.load(.monotonic)});
    wp(&body, &pos, ",\"p50_us\":{d}",      .{g_p50_us.load(.monotonic)});
    wp(&body, &pos, ",\"p99_us\":{d}",      .{g_p99_us.load(.monotonic)});
    wp(&body, &pos, ",\"rss_mb\":{d}",      .{rss_mb});
    wp(&body, &pos, ",\"wal_mb\":{d:.1}",   .{wal_mb});
    wp(&body, &pos, ",\"hist_d1\":[",       .{});
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
    wp(&body, &pos, "]}}", .{});

    const body_s = body[0..pos];
    var wbuf: [512]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    const w = &sw.interface;
    var hdr: [256]u8 = undefined;
    const hdr_s = std.fmt.bufPrint(&hdr,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{body_s.len},
    ) catch return;
    w.writeAll(hdr_s) catch return;
    w.writeAll(body_s) catch return;
    w.flush() catch {};
}

fn serveHtml(stream: std.Io.net.Stream, io: std.Io) void {
    var wbuf: [8192]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    const w = &sw.interface;
    var hdr: [256]u8 = undefined;
    const hdr_s = std.fmt.bufPrint(&hdr,
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{DASHBOARD_HTML.len},
    ) catch return;
    w.writeAll(hdr_s) catch return;
    w.writeAll(DASHBOARD_HTML) catch return;
    w.flush() catch {};
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
    const errors = g_pub_errors.load(.monotonic);

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
    var ru: std.c.rusage = undefined;
    _ = std.c.getrusage(0, &ru);
    const raw_rss: usize = @intCast(ru.maxrss);
    const rss_mb = if (builtin.os.tag == .macos) raw_rss / (1024*1024) else raw_rss / 1024;

    std.debug.print("\n\n\x1b[1;36m══════════════════════════════════════════════════════\x1b[0m\n", .{});
    std.debug.print("\x1b[1mRÉCAPITULATIF — {d}s de fonctionnement\x1b[0m\n", .{elapsed_s});
    std.debug.print("\x1b[1;36m══════════════════════════════════════════════════════\x1b[0m\n\n", .{});

    std.debug.print("  \x1b[1mDébit\x1b[0m\n", .{});
    std.debug.print("    D1 MoMo→Bank livré : \x1b[36m{d:>10}\x1b[0m msgs  (\x1b[1;36m{d}\x1b[0m tx/s moy)\n",
        .{ d1, d1 / elapsed_s });
    std.debug.print("    D2 Bank→MoMo livré : \x1b[33m{d:>10}\x1b[0m msgs  (\x1b[1;33m{d}\x1b[0m tx/s moy)\n",
        .{ d2, d2 / elapsed_s });
    std.debug.print("    Reversals 0420     : \x1b[31m{d:>10}\x1b[0m msgs  (\x1b[1;31m{d}\x1b[0m rev/s moy)  (~{d}% du trafic)\n",
        .{ rev, rev / elapsed_s, if (d1 + d2 + rev > 0) rev * 100 / (d1 + d2 + rev) else 0 });
    std.debug.print("    Total              : \x1b[1m{d:>10}\x1b[0m msgs  ({d} tx/s moy)\n",
        .{ d1 + d2 + rev, (d1 + d2 + rev) / elapsed_s });
    std.debug.print("    Erreurs pub        : \x1b[{s}m{d:>10}\x1b[0m\n\n",
        .{ if (errors == 0) "32" else "31", errors });

    if (n > 1) {
        std.debug.print("  \x1b[1mLatence (publish → deliver)\x1b[0m  [{d} échantillons]\n", .{n});
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
    std.debug.print("    WAL écrit   : {d:.1} MB\n\n", .{wal_mb});

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
    \\.c1{color:#00d4ff}.c2{color:#ff6b35}.cg{color:#44dd88}.cr{color:#ff4444}.cn{color:#aaa}
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
    \\  <div class="card"><div class="v cr" id="cr">—</div><div class="l">Rev/s &nbsp;0420 reversals</div></div>
    \\  <div class="card"><div class="v cn" id="ct">—</div><div class="l">Total livré</div></div>
    \\  <div class="card"><div class="v cg" id="p50">—</div><div class="l">Latence P50</div></div>
    \\  <div class="card"><div class="v cg" id="p99">—</div><div class="l">Latence P99</div></div>
    \\  <div class="card"><div class="v cg" id="ce">—</div><div class="l">Erreurs pub</div></div>
    \\  <div class="card"><div class="v cn" id="rs">—</div><div class="l">Mémoire RSS</div></div>
    \\  <div class="card"><div class="v cn" id="wl">—</div><div class="l">WAL écrit</div></div>
    \\</div>
    \\<div class="box">
    \\  <h2>Throughput &mdash; messages / seconde (60 dernières secondes)</h2>
    \\  <canvas id="ch"></canvas>
    \\</div>
    \\<p class="footer">OrusInfra &middot; broker:7797 &middot; dashboard:7780 &middot; topics: pipeline.inbound + pipeline.outbound</p>
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
    \\    {label:'Reversals 0420',data:[],borderColor:'#ff4444',backgroundColor:'rgba(255,68,68,.07)',tension:.3,fill:true,pointRadius:0,borderWidth:2}
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
    \\    document.getElementById('cr').textContent=f(m.rev_tps);
    \\    document.getElementById('ct').textContent=f(m.d1_total+m.d2_total+m.rev_total);
    \\    document.getElementById('p50').textContent=fms(m.p50_us);
    \\    document.getElementById('p99').textContent=fms(m.p99_us);
    \\    const ce=document.getElementById('ce');
    \\    ce.textContent=f(m.pub_errors);ce.className='v '+(m.pub_errors>0?'cr':'cg');
    \\    document.getElementById('rs').textContent=m.rss_mb+' MB';
    \\    document.getElementById('wl').textContent=m.wal_mb.toFixed(1)+' MB';
    \\    document.getElementById('el').textContent=ft(m.elapsed_s);
    \\    document.getElementById('ts').textContent=new Date().toLocaleTimeString();
    \\    const n=m.hist_d1.length;
    \\    ch.data.labels=m.hist_d1.map((_,i)=>i===n-1?'now':'-'+(n-1-i)+'s');
    \\    ch.data.datasets[0].data=m.hist_d1;
    \\    ch.data.datasets[1].data=m.hist_d2;
    \\    ch.data.datasets[2].data=m.hist_rev;
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

    // Broker.
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

    // Subscribers (2 par topic).
    for (0..2) |_| {
        (try std.Thread.spawn(.{}, subLoop, .{SubArgs{ .io = io, .topic = TOPIC_D1, .is_d1 = true  }})).detach();
        (try std.Thread.spawn(.{}, subLoop, .{SubArgs{ .io = io, .topic = TOPIC_D2, .is_d1 = false }})).detach();
    }
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);

    // Publishers.
    for (0..pub_threads) |_| {
        (try std.Thread.spawn(.{}, publishLoop, .{PubArgs{ .io = io, .topic = TOPIC_D1, .is_d1 = true  }})).detach();
        (try std.Thread.spawn(.{}, publishLoop, .{PubArgs{ .io = io, .topic = TOPIC_D2, .is_d1 = false }})).detach();
    }

    // Ticker.
    (try std.Thread.spawn(.{}, ticker, .{@as(u8, 0)})).detach();

    // Handler SIGINT/SIGTERM pour afficher le récap à l'arrêt.
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
