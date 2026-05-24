// HTTP dashboard — serves metrics JSON (/metrics) and the live HTML dashboard (/).
// Uses libc socket() directly — NOT std.Io.net — so macOS doesn't mark the
// socket with kqueue/non-blocking flags that cause EADDRNOTAVAIL on loopback.

const std     = @import("std");
const builtin = @import("builtin");
const s       = @import("state.zig");
const ticker_mod = @import("ticker.zig");

// ── Helpers ───────────────────────────────────────────────────────────────────

fn httpWrite(fd: s.HttpFd, data: []const u8) void {
    var off: usize = 0;
    while (off < data.len) {
        const n = s.csock.write(fd, data[off..].ptr, data[off..].len);
        if (n <= 0) return;
        off += @intCast(n);
    }
}

fn httpReadByte(fd: s.HttpFd) ?u8 {
    var b: u8 = 0;
    const n = s.csock.read(fd, &b, 1);
    if (n <= 0) return null;
    return b;
}

fn wp(buf: []u8, pos: *usize, comptime fmt: []const u8, args: anytype) void {
    const out = std.fmt.bufPrint(buf[pos.*..], fmt, args) catch return;
    pos.* += out.len;
}

fn parseU32Param(path: []const u8, key: []const u8) ?u32 {
    const q = std.mem.indexOfScalar(u8, path, '?') orelse return null;
    var rest = path[q + 1 ..];
    while (rest.len > 0) {
        const amp = std.mem.indexOfScalar(u8, rest, '&') orelse rest.len;
        const pair = rest[0..amp];
        if (std.mem.indexOf(u8, pair, key)) |ki| {
            const eq = ki + key.len;
            if (eq < pair.len and pair[eq] == '=') {
                var val = pair[eq + 1 ..];
                if (std.mem.indexOfScalar(u8, val, ' ')) |sp| val = val[0..sp];
                return std.fmt.parseInt(u32, val, 10) catch null;
            }
        }
        rest = if (amp < rest.len) rest[amp + 1 ..] else "";
    }
    return null;
}

// ── /metrics ─────────────────────────────────────────────────────────────────

fn serveMetrics(fd: s.HttpFd) void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const now_ns: i64 = ts.sec *% std.time.ns_per_s + ts.nsec;
    const elapsed: u64 = @intCast(@max(1, @divTrunc(now_ns - s.g_start_ns, std.time.ns_per_s)));

    var hgw: [60]u64 = undefined;
    var hn: usize = 0;
    while (true) {
        const gen1 = s.g_hist_gen.load(.acquire);
        if (gen1 % 2 != 0) continue;
        hn = s.g_hist_n.load(.monotonic);
        const hstart: usize = if (hn < 60) 0 else s.g_hist_head.load(.monotonic);
        for (0..hn) |i| hgw[i] = s.g_hist_gw[(hstart + i) % 60];
        const gen2 = s.g_hist_gen.load(.acquire);
        if (gen1 == gen2) break;
    }
    const pct = ticker_mod.computePercentiles();

    const sent     = s.g_sent.load(.monotonic);
    const ok       = s.g_ok.load(.monotonic);
    const declined = s.g_declined.load(.monotonic);
    const err_conn = s.g_err_conn.load(.monotonic);
    const err_mac  = s.g_err_mac.load(.monotonic);
    const in_flight: i64 = @as(i64, @intCast(sent)) -
        @as(i64, @intCast(ok + declined + err_conn + err_mac));

    var ru: std.c.rusage = undefined;
    _ = std.c.getrusage(0, &ru);
    const raw_rss: usize = @intCast(ru.maxrss);
    const rss_mb = if (builtin.os.tag == .macos) raw_rss / (1024 * 1024) else raw_rss / 1024;

    var body: [262144]u8 = undefined;
    var pos: usize = 0;
    wp(&body, &pos, "{{", .{});
    wp(&body, &pos, "\"elapsed_s\":{d}",    .{elapsed});
    wp(&body, &pos, ",\"sent\":{d}",        .{sent});
    wp(&body, &pos, ",\"ok\":{d}",          .{ok});
    wp(&body, &pos, ",\"declined\":{d}",    .{declined});
    wp(&body, &pos, ",\"err_conn\":{d}",    .{err_conn});
    wp(&body, &pos, ",\"err_mac\":{d}",     .{err_mac});
    wp(&body, &pos, ",\"in_flight\":{d}",   .{in_flight});
    wp(&body, &pos, ",\"bank_noresp\":{d}", .{s.g_bank_noresp.load(.monotonic)});
    wp(&body, &pos, ",\"bank_rc00\":{d}",   .{s.g_bank_rc00.load(.monotonic)});
    wp(&body, &pos, ",\"bank_rc51\":{d}",   .{s.g_bank_rc51.load(.monotonic)});
    wp(&body, &pos, ",\"bank_rc91\":{d}",   .{s.g_bank_rc91.load(.monotonic)});
    wp(&body, &pos, ",\"latency_ms\":{d}",  .{s.g_latency_ms.load(.monotonic)});
    wp(&body, &pos, ",\"noresp_pct\":{d}",  .{s.g_noresp_pct.load(.monotonic)});
    wp(&body, &pos, ",\"rc51_pct\":{d}",    .{s.g_rc51_pct.load(.monotonic)});
    wp(&body, &pos, ",\"rc91_pct\":{d}",    .{s.g_rc91_pct.load(.monotonic)});
    wp(&body, &pos, ",\"p50_us\":{d}",      .{pct.p50});
    wp(&body, &pos, ",\"p99_us\":{d}",      .{pct.p99});
    wp(&body, &pos, ",\"rss_mb\":{d}",      .{rss_mb});
    wp(&body, &pos, ",\"gw_tps\":{d}",      .{if (hn > 0) hgw[hn - 1] else 0});
    wp(&body, &pos, ",\"peak_tps\":{d}",    .{s.g_peak_tps.load(.monotonic)});
    const hb_last   = s.g_hb_last_ns.load(.monotonic);
    const hb_since_s: i64 = if (hb_last > 0) @divTrunc(now_ns - hb_last, std.time.ns_per_s) else -1;
    const last_tx   = s.g_last_tx_ns.load(.monotonic);
    const tx_since_s: i64 = if (last_tx > 0) @divTrunc(now_ns - last_tx, std.time.ns_per_s) else -1;
    wp(&body, &pos, ",\"startup_d0\":{d}", .{@intFromBool(s.g_startup_d0.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d1\":{d}", .{@intFromBool(s.g_startup_d1.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d2\":{d}", .{@intFromBool(s.g_startup_d2.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d3\":{d}", .{@intFromBool(s.g_startup_d3.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d4\":{d}", .{@intFromBool(s.g_startup_d4.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d5\":{d}", .{@intFromBool(s.g_startup_d5.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d6\":{d}", .{@intFromBool(s.g_startup_d6.load(.monotonic))});
    wp(&body, &pos, ",\"cashout_sent\":{d}",    .{s.g_cashout_sent.load(.monotonic)});
    wp(&body, &pos, ",\"cashout_ok\":{d}",      .{s.g_cashout_ok.load(.monotonic)});
    wp(&body, &pos, ",\"cashout_fail\":{d}",    .{s.g_cashout_fail.load(.monotonic)});
    wp(&body, &pos, ",\"hb_ok\":{d}",           .{s.g_hb_ok.load(.monotonic)});
    wp(&body, &pos, ",\"hb_fail\":{d}",         .{s.g_hb_fail.load(.monotonic)});
    wp(&body, &pos, ",\"hb_since_s\":{d}",      .{hb_since_s});
    wp(&body, &pos, ",\"tx_since_s\":{d}",      .{tx_since_s});
    wp(&body, &pos, ",\"reversals\":{d}",        .{s.g_reversals.load(.monotonic)});
    wp(&body, &pos, ",\"rev_ok\":{d}",           .{s.g_rev_ok.load(.monotonic)});
    wp(&body, &pos, ",\"rev_fail\":{d}",         .{s.g_rev_fail.load(.monotonic)});
    wp(&body, &pos, ",\"d2_sent\":{d}",          .{s.g_d2_sent.load(.monotonic)});
    wp(&body, &pos, ",\"d2_received\":{d}",      .{s.g_d2_received.load(.monotonic)});
    wp(&body, &pos, ",\"audit_lines\":{d}",      .{s.g_audit_lines.load(.monotonic)});
    wp(&body, &pos, ",\"wal_kb\":{d}",           .{s.g_wal_bytes.load(.monotonic) / 1024});
    wp(&body, &pos, ",\"mac_active\":{d}",       .{@intFromBool(s.g_mac_active.load(.monotonic))});
    wp(&body, &pos, ",\"hist_gw\":[", .{});
    for (0..hn) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{hgw[i]});
    }
    wp(&body, &pos, "],\"hist_d1\":[", .{});
    const nd1 = s.g_hist_d1_n.load(.monotonic);
    const hstart1: usize = if (nd1 < 60) 0 else s.g_hist_d1_head.load(.monotonic);
    for (0..nd1) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{s.g_hist_d1[(hstart1 + i) % 60]});
    }
    wp(&body, &pos, "],\"hist_d2\":[", .{});
    const nd2 = s.g_hist_d2_n.load(.monotonic);
    const hstart2: usize = if (nd2 < 60) 0 else s.g_hist_d2_head.load(.monotonic);
    for (0..nd2) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{s.g_hist_d2[(hstart2 + i) % 60]});
    }
    wp(&body, &pos, "],\"hist_d3\":[", .{});
    const nd3 = s.g_hist_d3_n.load(.monotonic);
    const hstart3: usize = if (nd3 < 60) 0 else s.g_hist_d3_head.load(.monotonic);
    for (0..nd3) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{s.g_hist_d3[(hstart3 + i) % 60]});
    }
    const tps_d3_now: u64 = if (nd3 > 0) s.g_hist_d3[(s.g_hist_d3_head.load(.monotonic) + 59) % 60] else 0;
    wp(&body, &pos, "],\"tps_d3\":{d}",         .{tps_d3_now});
    wp(&body, &pos, ",\"total_delivered\":{d}", .{ok + declined});
    wp(&body, &pos, ",\"d1_total\":{d}",        .{s.g_d1_total.load(.monotonic)});
    wp(&body, &pos, ",\"d2_total\":{d}",        .{s.g_d2_total.load(.monotonic)});
    wp(&body, &pos, "}}", .{});

    const body_s = body[0..pos];
    var hdr: [256]u8 = undefined;
    const hdr_s = std.fmt.bufPrint(&hdr,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{body_s.len}) catch return;
    httpWrite(fd, hdr_s);
    httpWrite(fd, body_s);
}

// ── POST /chaos ───────────────────────────────────────────────────────────────

fn handleChaosPost(path: []const u8, fd: s.HttpFd) void {
    if (parseU32Param(path, "latency_ms")) |v| s.g_latency_ms.store(@min(v, 2000), .monotonic);
    if (parseU32Param(path, "noresp_pct")) |v| s.g_noresp_pct.store(@min(v, 100),  .monotonic);
    if (parseU32Param(path, "rc51_pct"))   |v| s.g_rc51_pct.store(@min(v, 100),    .monotonic);
    if (parseU32Param(path, "rc91_pct"))   |v| s.g_rc91_pct.store(@min(v, 100),    .monotonic);
    httpWrite(fd, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n");
}

// ── GET / ─────────────────────────────────────────────────────────────────────

fn serveDashboard(fd: s.HttpFd) void {
    var hdr: [256]u8 = undefined;
    const hdr_s = std.fmt.bufPrint(&hdr,
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{DASHBOARD_HTML.len}) catch return;
    httpWrite(fd, hdr_s);
    httpWrite(fd, DASHBOARD_HTML);
}

fn httpConn(ctx: s.HttpCtx) void {
    defer _ = s.csock.close(ctx.fd);
    var line: [512]u8 = undefined;
    var ln: usize = 0;
    while (ln < line.len) {
        const b = httpReadByte(ctx.fd) orelse return;
        if (b == '\n') break;
        if (b != '\r') { line[ln] = b; ln += 1; }
    }
    var tail: [4]u8 = .{ 0, 0, 0, 0 };
    while (true) {
        const b = httpReadByte(ctx.fd) orelse break;
        tail[0] = tail[1]; tail[1] = tail[2]; tail[2] = tail[3]; tail[3] = b;
        if (std.mem.eql(u8, &tail, "\r\n\r\n")) break;
    }
    const path = line[0..ln];
    if (std.mem.startsWith(u8, path, "GET /metrics")) {
        serveMetrics(ctx.fd);
    } else if (std.mem.startsWith(u8, path, "POST /chaos")) {
        handleChaosPost(path, ctx.fd);
    } else if (std.mem.startsWith(u8, path, "GET / ") or std.mem.eql(u8, path, "GET /")) {
        serveDashboard(ctx.fd);
    } else {
        httpWrite(ctx.fd, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    }
}

pub fn httpServer() void {
    const sock = s.csock.socket(s.C_AF_INET, s.C_SOCK_STREAM, s.C_IPPROTO_TCP);
    if (sock < 0) { std.log.err("HTTP socket() failed", .{}); return; }
    defer _ = s.csock.close(sock);
    const opt: c_int = 1;
    _ = s.csock.setsockopt(sock, s.C_SOL_SOCKET, s.C_SO_REUSEADDR, @ptrCast(&opt), @sizeOf(c_int));
    _ = s.csock.setsockopt(sock, s.C_SOL_SOCKET, s.C_SO_REUSEPORT, @ptrCast(&opt), @sizeOf(c_int));
    const addr = s.SockaddrIn{
        .sin_port = std.mem.nativeToBig(u16, s.HTTP_PORT),
        .sin_addr = std.mem.nativeToBig(u32, 0x7F000001),
    };
    if (s.csock.bind(sock, &addr, @sizeOf(s.SockaddrIn)) < 0) {
        std.log.err("HTTP bind() failed", .{}); return;
    }
    if (s.csock.listen(sock, 64) < 0) {
        std.log.err("HTTP listen() failed", .{}); return;
    }
    std.log.info("Battle test dashboard : \x1b[1;35mhttp://127.0.0.1:{d}\x1b[0m", .{s.HTTP_PORT});
    while (!s.g_stopped.load(.monotonic)) {
        const client = s.csock.accept(sock, null, null);
        if (client < 0) {
            if (s.g_stopped.load(.monotonic)) break;
            continue;
        }
        const tv = s.CTimeval{ .tv_sec = 5 };
        _ = s.csock.setsockopt(client, s.C_SOL_SOCKET, s.C_SO_RCVTIMEO, @ptrCast(&tv), @sizeOf(s.CTimeval));
        const t = std.Thread.spawn(.{}, httpConn, .{s.HttpCtx{ .fd = client }}) catch {
            _ = s.csock.close(client);
            continue;
        };
        t.detach();
    }
}

// ── Dashboard HTML ────────────────────────────────────────────────────────────

const DASHBOARD_HTML =
    \\<!DOCTYPE html>
    \\<html lang="fr">
    \\<head>
    \\<meta charset="UTF-8">
    \\<title>OrusGateway</title>
    \\<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
    \\<style>
    \\*{box-sizing:border-box;margin:0;padding:0}
    \\html,body{background:#141414;color:#d4d4d4;font-family:-apple-system,'Helvetica Neue',sans-serif;font-size:13px;min-height:100vh}
    \\.topbar{display:flex;align-items:center;justify-content:space-between;padding:12px 24px;border-bottom:1px solid #2a2a2a;background:#141414}
    \\.brand{font-size:13px;font-weight:500;color:#e8e8e8;letter-spacing:-.01em}
    \\.brand span{color:#555}
    \\.live{display:inline-flex;align-items:center;gap:5px;font-size:11px;color:#555}
    \\.live-dot{width:6px;height:6px;border-radius:50%;background:#4ade80;animation:blink 1.4s infinite}
    \\@keyframes blink{0%,100%{opacity:1}50%{opacity:.2}}
    \\.meta{font-size:12px;color:#555;font-variant-numeric:tabular-nums}
    \\.body{display:grid;grid-template-columns:196px 1fr;min-height:calc(100vh - 45px)}
    \\.sidebar{border-right:1px solid #2a2a2a;padding:18px 0;background:#141414}
    \\.s-section{padding:0 16px;font-size:10px;font-weight:500;color:#444;letter-spacing:.1em;text-transform:uppercase;margin:16px 0 5px}
    \\.s-section:first-child{margin-top:0}
    \\.s-row{display:flex;align-items:center;gap:8px;padding:5px 16px}
    \\.s-dot{width:6px;height:6px;border-radius:50%;background:#333;flex-shrink:0;transition:background .3s}
    \\.s-dot.ok{background:#4ade80}
    \\.s-dot.warn{background:#fbbf24}
    \\.s-dot.fail{background:#f87171}
    \\.s-label{font-size:12px;color:#999;flex:1}
    \\.s-sub{font-size:11px;color:#444;font-variant-numeric:tabular-nums}
    \\.int-block{margin:14px;padding:14px;background:#1a1a1a;border:1px solid #2a2a2a;border-radius:6px}
    \\.int-verdict{font-size:12px;font-weight:500;margin-bottom:10px;color:#4ade80}
    \\.int-verdict.warn{color:#fbbf24}
    \\.int-verdict.fail{color:#f87171}
    \\.int-row{display:flex;justify-content:space-between;padding:3px 0;font-size:11px;border-bottom:1px solid #1f1f1f}
    \\.int-row:last-child{border:none}
    \\.int-row .lk{color:#555}
    \\.int-row .lv{font-variant-numeric:tabular-nums;color:#aaa}
    \\.main{padding:20px 24px;display:flex;flex-direction:column;gap:16px;background:#141414}
    \\.kpi-grid{display:grid;grid-template-columns:repeat(6,1fr);gap:1px;background:#2a2a2a;border:1px solid #2a2a2a;border-radius:6px;overflow:hidden}
    \\.kpi{background:#1a1a1a;padding:14px 16px}
    \\.kpi-label{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.08em;margin-bottom:6px}
    \\.kpi-val{font-size:20px;font-weight:500;color:#d4d4d4;font-variant-numeric:tabular-nums;letter-spacing:-.02em;line-height:1}
    \\.kpi-val.ok{color:#4ade80}
    \\.kpi-val.warn{color:#fbbf24}
    \\.kpi-val.fail{color:#f87171}
    \\.kpi-unit{font-size:10px;color:#444;margin-top:3px}
    \\.panel{background:#1a1a1a;border:1px solid #2a2a2a;border-radius:6px;padding:16px}
    \\.panel-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
    \\.panel-title{font-size:10px;color:#555;text-transform:uppercase;letter-spacing:.09em}
    \\.panel-val{font-size:12px;color:#d4d4d4;font-variant-numeric:tabular-nums;font-weight:500}
    \\.legend{display:flex;gap:16px;margin-bottom:10px}
    \\.leg{display:flex;align-items:center;gap:5px;font-size:11px;color:#555}
    \\.leg-line{width:16px;height:1px;background:#888}
    \\.leg-line.dashed{background:repeating-linear-gradient(90deg,#555 0,#555 3px,transparent 3px,transparent 6px)}
    \\.chaos-grid{display:grid;grid-template-columns:1fr 1fr;gap:6px 32px}
    \\.sl-row{display:flex;align-items:center;gap:10px;padding:5px 0;border-bottom:1px solid #1f1f1f}
    \\.sl-row:last-child{border:none}
    \\.sl-row label{font-size:11px;color:#555;width:150px;flex-shrink:0}
    \\.sl-row input[type=range]{flex:1;-webkit-appearance:none;height:1px;background:#333;outline:none;cursor:pointer}
    \\.sl-row input[type=range]::-webkit-slider-thumb{-webkit-appearance:none;width:11px;height:11px;border-radius:50%;background:#888;cursor:pointer;transition:background .15s}
    \\.sl-row input[type=range]::-webkit-slider-thumb:hover{background:#d4d4d4}
    \\.sl-row .sv{font-size:11px;color:#888;width:40px;text-align:right;font-variant-numeric:tabular-nums}
    \\.chaos-counters{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;background:#2a2a2a;border:1px solid #2a2a2a;border-radius:4px;overflow:hidden;margin-top:12px}
    \\.cc{background:#1a1a1a;padding:10px 12px}
    \\.cc-label{font-size:10px;color:#444;text-transform:uppercase;letter-spacing:.07em;margin-bottom:3px}
    \\.cc-val{font-size:15px;font-weight:500;color:#aaa;font-variant-numeric:tabular-nums}
    \\.footer{font-size:10px;color:#333;padding-top:4px;text-align:right}
    \\.tip{position:relative;display:inline-flex;align-items:center;cursor:default;margin-left:3px;vertical-align:middle}
    \\.tip-i{font-size:9px;color:#3a3a3a;line-height:1;user-select:none;transition:color .15s}
    \\.tip:hover .tip-i{color:#777}
    \\.tip-box{display:none;position:absolute;left:16px;top:50%;transform:translateY(-50%);width:230px;background:#1c1c1c;border:1px solid #2e2e2e;border-radius:5px;padding:9px 11px;font-size:10px;color:#999;line-height:1.55;z-index:300;pointer-events:none;box-shadow:0 4px 16px rgba(0,0,0,.5)}
    \\.tip:hover .tip-box{display:block}
    \\</style>
    \\</head>
    \\<body>
    \\
    \\<div class="topbar">
    \\  <div style="display:flex;align-items:center;gap:14px">
    \\    <div class="brand">OrusGateway <span>· Battle Test</span></div>
    \\    <div class="live"><span class="live-dot"></span>live</div>
    \\  </div>
    \\  <div style="display:flex;align-items:center;gap:20px">
    \\    <div class="meta" id="el">--:--:--</div>
    \\    <div class="meta" id="ts">—</div>
    \\  </div>
    \\</div>
    \\
    \\<div class="body">
    \\<div class="sidebar">
    \\  <div class="s-section">Pipeline</div>
    \\  <div class="s-row"><span class="s-dot" id="sd0"></span><span class="s-label">Handshake<span class="tip"><span class="tip-i">ⓘ</span><span class="tip-box">Ouverture de session entre le gateway et la banque. Échange ISO 8583 0800/301 → 0810 avec négociation de la clé MAC.</span></span></span><span class="s-sub" id="sd0-val">—</span></div>
    \\  <div class="s-row"><span class="s-dot" id="sd1"></span><span class="s-label">Transfer<span class="tip"><span class="tip-i">ⓘ</span><span class="tip-box">Paiement initié depuis le wallet mobile (MoMo) vers un compte bancaire. Message ISO 8583 0200 → 0210 avec MAC GIMAC/BEAC.</span></span></span><span class="s-sub" id="sd1-val">—</span></div>
    \\  <div class="s-row"><span class="s-dot" id="sd2"></span><span class="s-label">Relay<span class="tip"><span class="tip-i">ⓘ</span><span class="tip-box">La banque relaie la confirmation au broker interne (WAL). Permet la traçabilité et la livraison aux systèmes abonnés.</span></span></span><span class="s-sub" id="sd2-val">—</span></div>
    \\  <div class="s-row"><span class="s-dot" id="sd3"></span><span class="s-label">Rollback<span class="tip"><span class="tip-i">ⓘ</span><span class="tip-box">Annulation d'une transaction approuvée en cas d'anomalie. Message ISO 8583 0420 → 0430. Garantit l'intégrité financière.</span></span></span><span class="s-sub" id="sd3-val">—</span></div>
    \\  <div class="s-row"><span class="s-dot" id="sd4"></span><span class="s-label">Settlement<span class="tip"><span class="tip-i">ⓘ</span><span class="tip-box">Réconciliation de fin de journée entre le gateway et la banque. Message ISO 8583 0500 → 0510 pour aligner les compteurs.</span></span></span><span class="s-sub" id="sd4-val">—</span></div>
    \\  <div class="s-row"><span class="s-dot" id="sd5"></span><span class="s-label">Pulse<span class="tip"><span class="tip-i">ⓘ</span><span class="tip-box">Battement de cœur toutes les 30s pour vérifier que la ligne bancaire est vivante. Rouge si silence de plus de 90 secondes.</span></span></span><span class="s-sub" id="sd5-val">—</span></div>
    \\  <div class="s-row"><span class="s-dot" id="sd6"></span><span class="s-label">Cashout<span class="tip"><span class="tip-i">ⓘ</span><span class="tip-box">Flux inverse : la banque initie un virement vers un wallet mobile (Bank → MoMo). Direction en cours d'implémentation.</span></span></span><span class="s-sub" id="sd6-val">—</span></div>
    \\  <div class="s-section">Sécurité</div>
    \\  <div class="s-row"><span class="s-dot" id="smac"></span><span class="s-label">HMAC-SHA256<span class="tip"><span class="tip-i">ⓘ</span><span class="tip-box">Signature cryptographique sur chaque trame ISO 8583, conforme aux exigences GIMAC/BEAC. Zéro violation = intégrité totale.</span></span></span><span class="s-sub" id="smac-val">—</span></div>
    \\  <div class="s-row"><span class="s-dot" id="shb"></span><span class="s-label">Dernier Pulse</span><span class="s-sub" id="shb-val">—</span></div>
    \\  <div class="s-section">Intégrité</div>
    \\  <div class="int-block">
    \\    <div class="int-verdict" id="int-verdict">— en attente</div>
    \\    <div class="int-row"><span class="lk">Envoyées</span><span class="lv" id="i-sent">—</span></div>
    \\    <div class="int-row"><span class="lk">Approuvées</span><span class="lv" id="i-ok">—</span></div>
    \\    <div class="int-row"><span class="lk">Déclinées</span><span class="lv" id="i-dec">—</span></div>
    \\    <div class="int-row"><span class="lk">Err. connexion</span><span class="lv" id="i-ec">—</span></div>
    \\    <div class="int-row"><span class="lk">Err. MAC</span><span class="lv" id="i-em">—</span></div>
    \\    <div class="int-row"><span class="lk">Audit log</span><span class="lv" id="i-audit">—</span></div>
    \\    <div class="int-row"><span class="lk">WAL broker<span class="tip" style="margin-left:3px"><span class="tip-i">ⓘ</span><span class="tip-box">Write-Ahead Log du broker — chaque message publié est écrit sur disque avec CRC32 avant livraison. Garantit la durabilité.</span></span></span><span class="lv" id="i-wal">—</span></div>
    \\  </div>
    \\</div>
    \\
    \\<div class="main">
    \\  <div class="kpi-grid">
    \\    <div class="kpi"><div class="kpi-label">D1 · MoMo→Bank</div><div class="kpi-val" id="k-d1">—</div><div class="kpi-unit">tx / s</div></div>
    \\    <div class="kpi"><div class="kpi-label">D3 · Bank→MoMo</div><div class="kpi-val" id="k-d3">—</div><div class="kpi-unit">tx / s</div></div>
    \\    <div class="kpi"><div class="kpi-label">Peak tx/s</div><div class="kpi-val" id="k-peak">—</div><div class="kpi-unit">maximum historique</div></div>
    \\    <div class="kpi"><div class="kpi-label">Total livré</div><div class="kpi-val" id="k-total">—</div><div class="kpi-unit">transactions</div></div>
    \\    <div class="kpi"><div class="kpi-label">Latence P50</div><div class="kpi-val ok" id="k-p50">—</div><div class="kpi-unit">médiane</div></div>
    \\    <div class="kpi"><div class="kpi-label">Latence P99</div><div class="kpi-val" id="k-p99">—</div><div class="kpi-unit">99e percentile</div></div>
    \\    <div class="kpi"><div class="kpi-label">Mémoire RSS</div><div class="kpi-val" id="k-rss">—</div><div class="kpi-unit">empreinte réelle</div></div>
    \\  </div>
    \\
    \\  <div class="panel">
    \\    <div class="panel-header"><div class="panel-title">Throughput</div><div class="panel-val" id="tps-now">—</div></div>
    \\    <div class="legend">
    \\      <span class="leg"><span class="leg-line"></span>Gateway tx/s</span>
    \\      <span class="leg"><span class="leg-line dashed"></span>D1 MoMo→Bank</span>
    \\      <span class="leg"><span class="leg-line" style="border-color:#3d7a3d;border-style:dotted"></span>D3 Bank→MoMo</span>
    \\    </div>
    \\    <div style="position:relative;height:140px">
    \\      <canvas id="ch1" role="img" aria-label="Throughput Gateway et D1 sur 60 secondes">Chargement...</canvas>
    \\    </div>
    \\  </div>
    \\
    \\  <div class="panel">
    \\    <div class="panel-header"><div class="panel-title">Chaos control</div></div>
    \\    <div class="chaos-grid">
    \\      <div>
    \\        <div class="sl-row"><label>Latence banque</label><input type="range" min="0" max="300" value="0" step="1" id="s-lat" oninput="sv('v-lat',this.value+' ms');pc()"><span class="sv" id="v-lat">0 ms</span></div>
    \\        <div class="sl-row"><label>Sans réponse (%)</label><input type="range" min="0" max="30" value="0" step="1" id="s-no" oninput="sv('v-no',this.value+'%');pc()"><span class="sv" id="v-no">0%</span></div>
    \\      </div>
    \\      <div>
    \\        <div class="sl-row"><label>RC=51 insuf. (%)</label><input type="range" min="0" max="50" value="0" step="1" id="s-51" oninput="sv('v-51',this.value+'%');pc()"><span class="sv" id="v-51">0%</span></div>
    \\        <div class="sl-row"><label>RC=91 indispo (%)</label><input type="range" min="0" max="30" value="0" step="1" id="s-91" oninput="sv('v-91',this.value+'%');pc()"><span class="sv" id="v-91">0%</span></div>
    \\      </div>
    \\    </div>
    \\    <div class="chaos-counters">
    \\      <div class="cc"><div class="cc-label">No-resp</div><div class="cc-val" id="c-noresp">—</div></div>
    \\      <div class="cc"><div class="cc-label">RC=51</div><div class="cc-val" id="c-rc51">—</div></div>
    \\      <div class="cc"><div class="cc-label">RC=91</div><div class="cc-val" id="c-rc91">—</div></div>
    \\      <div class="cc"><div class="cc-label">Reversals OK</div><div class="cc-val" id="c-rev">—</div></div>
    \\    </div>
    \\  </div>
    \\
    \\  <div class="footer">broker:7799 · chaos-bank:5099 · bank-server:7797 · momo:7796 · dashboard:7782</div>
    \\</div>
    \\</div>
    \\
    \\<script>
    \\let ch1=null,ct=null;
    \\const ff=n=>n>=1e6?(n/1e6).toFixed(2)+'M':n>=1e3?(n/1e3).toFixed(1)+'k':String(Math.round(n));
    \\const fms=us=>us>=1000?(us/1000).toFixed(2)+' ms':Math.round(us)+' µs';
    \\const ft=s=>[Math.floor(s/3600),Math.floor(s%3600/60),s%60].map(x=>String(x).padStart(2,'0')).join(':');
    \\const flo=n=>Math.round(n).toLocaleString('fr-FR');
    \\function sv(id,v){const e=document.getElementById(id);if(e)e.textContent=v;}
    \\function dot(id,ok){const e=document.getElementById(id);if(!e)return;e.className='s-dot'+(ok===null?'':ok==='warn'?' warn':ok?' ok':' fail');}
    \\function pc(){
    \\  clearTimeout(ct);
    \\  ct=setTimeout(()=>{
    \\    const p='latency_ms='+document.getElementById('s-lat').value+'&noresp_pct='+document.getElementById('s-no').value+'&rc51_pct='+document.getElementById('s-51').value+'&rc91_pct='+document.getElementById('s-91').value;
    \\    fetch('/chaos?'+p,{method:'POST'}).catch(()=>{});
    \\  },150);
    \\}
    \\window.addEventListener('load',()=>{
    \\  const c=document.getElementById('ch1');
    \\  if(c&&window.Chart){
    \\    ch1=new Chart(c,{type:'line',data:{labels:[],datasets:[
    \\      {label:'Gateway',data:[],borderColor:'#888',backgroundColor:'rgba(136,136,136,.06)',tension:.4,fill:true,pointRadius:0,borderWidth:1.5},
    \\      {label:'D1',data:[],borderColor:'#444',backgroundColor:'transparent',tension:.4,fill:false,pointRadius:0,borderWidth:1,borderDash:[4,3]},
    \\      {label:'D3',data:[],borderColor:'#3d7a3d',backgroundColor:'transparent',tension:.4,fill:false,pointRadius:0,borderWidth:1,borderDash:[2,2]}
    \\    ]},options:{
    \\      animation:false,responsive:true,maintainAspectRatio:false,
    \\      scales:{
    \\        y:{beginAtZero:true,grid:{color:'#1f1f1f',lineWidth:1},border:{color:'#2a2a2a'},ticks:{color:'#444',font:{size:10},callback:v=>ff(v)}},
    \\        x:{grid:{color:'#1f1f1f',lineWidth:1},border:{color:'#2a2a2a'},ticks:{color:'#444',font:{size:10},maxTicksLimit:8}}
    \\      },
    \\      plugins:{legend:{display:false},tooltip:{backgroundColor:'#1a1a1a',borderColor:'#2a2a2a',borderWidth:1,titleColor:'#555',bodyColor:'#d4d4d4',padding:8,titleFont:{size:10},bodyFont:{size:11}}}
    \\    }});
    \\  }
    \\  tick();setInterval(tick,1000);
    \\});
    \\function tick(){
    \\  fetch('/metrics').then(r=>r.json()).then(m=>{
    \\    sv('ts',new Date().toLocaleTimeString('fr-FR'));
    \\    sv('el',ft(m.elapsed_s||0));
    \\    const macOk=m.mac_active===1;
    \\    dot('sd0',macOk);sv('sd0-val',macOk?'actif':'inactif');
    \\    const txs=m.tx_since_s??-1;
    \\    dot('sd1',txs>=0&&txs<10);sv('sd1-val',txs>=0?flo(m.gw_tps||0)+' tx/s':'—');
    \\    const d2s=m.d2_sent||0,d2r=m.d2_received||0;
    \\    dot('sd2',d2s>0?d2r>=d2s:null);sv('sd2-val',d2s>0?d2r+'/'+d2s:'—');
    \\    const revTotal=m.reversals||0,revOk=m.rev_ok||0,revFail=m.rev_fail||0;
    \\    const revRate=revTotal>0?revFail/revTotal:0;
    \\    dot('sd3',revTotal>0?(revFail===0?true:revRate<0.01?'warn':false):null);
    \\    sv('sd3-val',revTotal>0?flo(revOk)+'/'+flo(revTotal):'—');
    \\    dot('sd4',m.startup_d4===1);sv('sd4-val',m.startup_d4===1?'OK':'—');
    \\    const hbs=m.hb_since_s??-1;
    \\    dot('sd5',hbs>=0&&hbs<90);sv('sd5-val',hbs>=0?hbs+'s':'—');
    \\    const cashOk=m.cashout_ok||0,cashFail=m.cashout_fail||0,cashSent=m.cashout_sent||0;
    \\    dot('sd6',cashSent>0?(cashFail===0):(m.startup_d6===1?true:null));
    \\    sv('sd6-val',cashSent>0?flo(cashOk)+'/'+flo(cashSent):'—');
    \\    dot('smac',macOk);sv('smac-val',macOk?'actif':'inactif');
    \\    dot('shb',hbs>=0&&hbs<90);sv('shb-val',hbs>=0?hbs+'s':'—');
    \\    const wk=m.wal_kb||0;sv('i-wal',wk>=1024?(wk/1024).toFixed(1)+' MB':wk+' KB');
    \\    const p99=m.p99_us||0;
    \\    sv('k-d1',flo(m.gw_tps||0));sv('k-d3',flo(m.tps_d3||0));sv('k-peak',flo(m.peak_tps||0));
    \\    sv('k-total',ff((m.ok||0)+(m.declined||0)));sv('k-p50',fms(m.p50_us||0));sv('k-p99',fms(p99));
    \\    const p99el=document.getElementById('k-p99');
    \\    if(p99el)p99el.className='kpi-val'+(p99>50000?' fail':p99>5000?' warn':' ok');
    \\    sv('k-rss',(m.rss_mb||0)+' MB');
    \\    sv('c-noresp',flo(m.bank_noresp||0));sv('c-rc51',flo(m.bank_rc51||0));sv('c-rc91',flo(m.bank_rc91||0));
    \\    sv('c-rev',flo(m.rev_ok||0)+'/'+flo(m.reversals||0));
    \\    const mac_err=m.err_mac||0,conn_err=m.err_conn||0,sent=Math.max(m.sent||1,1);
    \\    const vd=document.getElementById('int-verdict');
    \\    if(vd){
    \\      if(mac_err>0){vd.className='int-verdict fail';vd.textContent='violation MAC';}
    \\      else if(conn_err*100/sent>=5){vd.className='int-verdict warn';vd.textContent='erreurs élevées';}
    \\      else{vd.className='int-verdict';vd.textContent='✓ intact';}
    \\    }
    \\    sv('i-sent',flo(m.sent||0));sv('i-ok',flo(m.ok||0));sv('i-dec',flo(m.declined||0));
    \\    sv('i-ec',flo(conn_err));sv('i-em',flo(mac_err));sv('i-audit',flo(m.audit_lines||0));
    \\    const hist=m.hist_gw||[],n=hist.length;
    \\    sv('tps-now',flo(hist[n-1]||0)+' tx/s');
    \\    if(ch1){
    \\      ch1.data.labels=hist.map((_,i)=>i===n-1?'now':'-'+(n-1-i)+'s');
    \\      ch1.data.datasets[0].data=[...hist];
    \\      ch1.data.datasets[1].data=[...(m.hist_d1||hist)];
    \\      ch1.data.datasets[2].data=[...(m.hist_d3||[])];
    \\      ch1.update('none');
    \\    }
    \\  }).catch(e=>{sv('ts','⚠ '+e.message);});
    \\}
    \\</script>
    \\</body>
    \\</html>
;
