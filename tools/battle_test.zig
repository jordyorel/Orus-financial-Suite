// Battle test — FakeChaosBank + integrity verifier + live chaos dashboard.
// Launch : zig build battle
// Open   : http://localhost:7782
//
// Adjust chaos at runtime:
//   POST /chaos?latency_ms=N&noresp_pct=N&rc51_pct=N&rc91_pct=N
//
// Integrity invariant (checked at exit):
//   sent == ok + declined + err_conn + err_mac  (every transaction is accounted for)
//   audit_log_lines == ok + declined            (every response has a log entry)

const std = @import("std");
const orusshare = @import("orusshare");
const orusbroker = @import("orusbroker");
const orusconnect = @import("orusconnect");
const gw_mod = @import("orusgateway");
const builtin = @import("builtin");

const BROKER_PORT: u16 = 7799;
const CHAOS_BANK_PORT: u16 = 5099;
const BANKSERVER_PORT: u16 = 7797;
const MOMO_PORT: u16 = 7796;
const HTTP_PORT: u16 = 7782;
// Cashout rate cap — prevents CPU starvation of gateway workers when bank
// latency is high (idle workers free CPU, cashout would otherwise monopolise it).
const CASHOUT_MAX_TPS: i64 = 5_000;
const CASHOUT_SLOT_NS: i64 = @divTrunc(std.time.ns_per_s, CASHOUT_MAX_TPS); // 200 µs
const TOPIC = "battle.inbound";
const TOPIC_OUTBOUND = "battle.outbound";
const AUDIT_PATH = "/tmp/battle_audit.log";
const WAL_PATH = "/tmp/battle.wal";

// ── Chaos config (live-adjustable via POST /chaos) ────────────────────────────
var g_latency_ms = std.atomic.Value(u32).init(0); // ms added before responding
var g_noresp_pct = std.atomic.Value(u32).init(0); // % requests with no response
var g_rc51_pct = std.atomic.Value(u32).init(0); // % RC=51 (insufficient funds)
var g_rc91_pct = std.atomic.Value(u32).init(0); // % RC=91 (issuer unavailable)

// ── Chaos bank events ─────────────────────────────────────────────────────────
var g_bank_noresp = std.atomic.Value(u64).init(0);
var g_bank_rc00 = std.atomic.Value(u64).init(0);
var g_bank_rc51 = std.atomic.Value(u64).init(0);
var g_bank_rc91 = std.atomic.Value(u64).init(0);

// ── Integrity counters ────────────────────────────────────────────────────────
var g_sent = std.atomic.Value(u64).init(0); // before gateway.process
var g_ok = std.atomic.Value(u64).init(0); // RC=00
var g_declined = std.atomic.Value(u64).init(0); // RC!=00 (legit business decline)
var g_err_conn = std.atomic.Value(u64).init(0); // BankUnreachable / MalformedResponse
var g_err_mac = std.atomic.Value(u64).init(0); // MacMismatch

// ── Throughput history (seqlock-protected, same pattern as pipeline_bench) ────
var g_hist_gen = std.atomic.Value(usize).init(0);
var g_hist_gw: [60]u64 = [_]u64{0} ** 60;
var g_hist_head = std.atomic.Value(usize).init(0);
var g_hist_n = std.atomic.Value(usize).init(0);
var g_prev_gw: u64 = 0;
var g_peak_tps = std.atomic.Value(u64).init(0); // running max over full test duration
var g_last_tx_ns = std.atomic.Value(i64).init(0); // monotonic ns of last completed transaction

// Live D1 TPS — updated every second by ticker; read by D3 cooperative throttle.
var g_d1_tps_live = std.atomic.Value(u64).init(0);

// D1 throughput history (MoMo→Bank)
var g_hist_d1: [60]u64 = [_]u64{0} ** 60;
var g_prev_d1: u64 = 0;
var g_hist_d1_head = std.atomic.Value(usize).init(0);
var g_hist_d1_n = std.atomic.Value(usize).init(0);

// D2 throughput history (Bank→Broker)
var g_hist_d2: [60]u64 = [_]u64{0} ** 60;
var g_prev_d2: u64 = 0;
var g_hist_d2_head = std.atomic.Value(usize).init(0);
var g_hist_d2_n = std.atomic.Value(usize).init(0);

// D3 throughput history (Bank→MoMo cashout)
var g_hist_d3: [60]u64 = [_]u64{0} ** 60;
var g_prev_d3: u64 = 0;
var g_hist_d3_head = std.atomic.Value(usize).init(0);
var g_hist_d3_n = std.atomic.Value(usize).init(0);

// Cumulative counters for D1/D2
var g_d1_total = std.atomic.Value(u64).init(0); // MoMo→Bank successful transactions
var g_d2_total = std.atomic.Value(u64).init(0); // Bank→Broker successful publications

// ── Latency (spinlock-protected ring buffer) ──────────────────────────────────
const LAT_CAP = 2000;
var g_lat: [LAT_CAP]u64 = [_]u64{0} ** LAT_CAP;
var g_lat_idx = std.atomic.Value(usize).init(0);
// g_lat_mu removed — g_lat_idx.fetchAdd gives each writer a unique slot;
// u64 writes are naturally atomic on ARM64/x86_64, no mutex needed.

var g_stopped = std.atomic.Value(bool).init(false);
var g_mac_active = std.atomic.Value(bool).init(false);
var g_start_ns: i64 = 0;

// ── Pipeline health (heartbeat / reversals / D2) ──────────────────────────────
var g_hb_ok = std.atomic.Value(u64).init(0);
var g_hb_fail = std.atomic.Value(u64).init(0);
var g_hb_last_ns = std.atomic.Value(i64).init(0);
var g_reversals = std.atomic.Value(u64).init(0); // injected
var g_rev_ok = std.atomic.Value(u64).init(0); // confirmed (0430/RC=00)
var g_rev_fail = std.atomic.Value(u64).init(0); // failed
var g_d2_sent = std.atomic.Value(u64).init(0); // injected → BankServer
var g_d2_received = std.atomic.Value(u64).init(0); // ACK received
var g_audit_lines = std.atomic.Value(u64).init(0); // lines written to audit log
var g_wal_bytes = std.atomic.Value(u64).init(0); // WAL file size (sampled by ticker)

// ── Startup validation flags (D0–D6) ─────────────────────────────────────────
var g_startup_d0 = std.atomic.Value(bool).init(false);
var g_startup_d1 = std.atomic.Value(bool).init(false);
var g_startup_d2 = std.atomic.Value(bool).init(false);
var g_startup_d3 = std.atomic.Value(bool).init(false);
var g_startup_d4 = std.atomic.Value(bool).init(false);
var g_startup_d5 = std.atomic.Value(bool).init(false);
var g_startup_d6 = std.atomic.Value(bool).init(false);

// ── Cashout counters (Bank→MoMo direction) ───────────────────────────────────
var g_cashout_sent = std.atomic.Value(u64).init(0);
var g_cashout_ok   = std.atomic.Value(u64).init(0);
var g_cashout_fail = std.atomic.Value(u64).init(0);

// ── MAC ───────────────────────────────────────────────────────────────────────
const SESSION_KEY = "orus_battle_key_32bytes_padxxxxx";
var chaos_mac = gw_mod.MacEngine.init();

fn sigHandler(sig: std.c.SIG) callconv(.c) void {
    _ = sig;
    g_stopped.store(true, .monotonic);
}

// ── Broker in-process ─────────────────────────────────────────────────────────

const BrokerInner = struct {
    wal: orusbroker.Wal,
    dedup: orusbroker.DedupFilter,
    router: orusbroker.TopicRouter,
    server: orusbroker.BrokerServer,
};
fn runBroker(inner: *BrokerInner) void {
    inner.server.serve(std.heap.c_allocator) catch {};
}

// ── FakeChaosBank (libc sockets — no std.Io sharing, pre-spawned pool) ───────

fn chaosMacReply(alloc: std.mem.Allocator, mti: [4]u8, stan: []const u8, rc: []const u8) ![]u8 {
    var resp = gw_mod.iso8583.IsoMessage.init(alloc, mti);
    defer resp.deinit();
    try resp.set(39, rc);
    try resp.set(11, stan);
    try resp.set(49, "XAF");
    const scope = try gw_mod.iso8583.serialize(&resp, alloc);
    defer alloc.free(scope);
    const mac_val = chaos_mac.compute(scope);
    try resp.set(64, &mac_val);
    return gw_mod.iso8583.serializeWithSchema(&resp, &gw_mod.DEFAULT_SCHEMA, alloc);
}

fn readAllFd(fd: c_int, buf: []u8) bool {
    var off: usize = 0;
    while (off < buf.len) {
        const n = csock.read(fd, buf[off..].ptr, buf.len - off);
        if (n <= 0) return false;
        off += @intCast(n);
    }
    return true;
}
fn writeAllFd(fd: c_int, buf: []const u8) bool {
    var off: usize = 0;
    while (off < buf.len) {
        const n = csock.write(fd, buf[off..].ptr, buf.len - off);
        if (n <= 0) return false;
        off += @intCast(n);
    }
    return true;
}

fn handleChaosFd(fd: c_int) void {
    const alloc = std.heap.c_allocator;
    // Keep-alive: serve multiple requests on the same connection so BankClient
    // never has to reconnect mid-session. Loop exits when the client closes
    // (readAllFd returns false) or on any unrecoverable framing error.
    while (true) {
        var len_buf: [2]u8 = undefined;
        if (!readAllFd(fd, &len_buf)) return;
        const req_len = std.mem.readInt(u16, &len_buf, .big);
        if (req_len == 0 or req_len > 4096) return;
        const req_data = alloc.alloc(u8, req_len) catch return;
        defer alloc.free(req_data);
        if (!readAllFd(fd, req_data)) return;

        var iso_req = gw_mod.iso8583.parseWithSchema(req_data, &gw_mod.DEFAULT_SCHEMA, alloc) catch return;
        defer iso_req.deinit();
        const stan = iso_req.get(11) orelse "000000";

        if (std.mem.eql(u8, &iso_req.mti, "0800")) {
            const nmi = iso_req.get(70) orelse "";
            var resp_iso = gw_mod.buildNetworkResponse(&iso_req, alloc, if (std.mem.eql(u8, nmi, "301")) SESSION_KEY else "") catch return;
            defer resp_iso.deinit();
            const bytes = gw_mod.iso8583.serializeWithSchema(&resp_iso, &gw_mod.DEFAULT_SCHEMA, alloc) catch return;
            defer alloc.free(bytes);
            var lout: [2]u8 = undefined;
            std.mem.writeInt(u16, &lout, @intCast(bytes.len), .big);
            if (!writeAllFd(fd, &lout)) return;
            if (!writeAllFd(fd, bytes)) return;
            continue; // keep connection alive for subsequent financial messages
        }

        var prng: u64 = undefined;
        std.c.arc4random_buf(@ptrCast(&prng), 8);

        if (prng % 100 < g_noresp_pct.load(.monotonic)) {
            _ = g_bank_noresp.fetchAdd(1, .monotonic);
            return; // deliberate no-response: close connection
        }

        const lat_ms = g_latency_ms.load(.monotonic);
        if (lat_ms > 0) {
            const ns: u64 = @as(u64, lat_ms) * 1_000_000;
            _ = std.c.nanosleep(&.{ .sec = 0, .nsec = @intCast(@min(ns, 999_999_999)) }, null);
        }

        const roll = (prng >> 8) % 100;
        const rc51 = g_rc51_pct.load(.monotonic);
        const rc91 = g_rc91_pct.load(.monotonic);
        const rc: []const u8 = if (roll < rc51) blk: {
            _ = g_bank_rc51.fetchAdd(1, .monotonic);
            break :blk "51";
        } else if (roll < rc51 + rc91) blk: {
            _ = g_bank_rc91.fetchAdd(1, .monotonic);
            break :blk "91";
        } else blk: {
            _ = g_bank_rc00.fetchAdd(1, .monotonic);
            break :blk "00";
        };

        const mti: [4]u8 = if (std.mem.eql(u8, &iso_req.mti, "0420") or
            std.mem.eql(u8, &iso_req.mti, "0421")) "0430".* else "0210".*;
        const bytes = chaosMacReply(alloc, mti, stan, rc) catch return;
        defer alloc.free(bytes);
        var lout: [2]u8 = undefined;
        std.mem.writeInt(u16, &lout, @intCast(bytes.len), .big);
        if (!writeAllFd(fd, &lout)) return;
        if (!writeAllFd(fd, bytes)) return;
    }
}

// Pre-spawned pool — libc accept() is safe to call concurrently from multiple
// threads on the same fd; the kernel serializes at the syscall level.
fn runChaosBankWorker(bank_sock: c_int) void {
    while (true) {
        const client = csock.accept(bank_sock, null, null);
        if (client < 0) {
            if (g_stopped.load(.monotonic)) return;
            continue;
        }
        const tv = CTimeval{ .tv_sec = 10 };
        _ = csock.setsockopt(client, C_SOL_SOCKET, C_SO_RCVTIMEO, @ptrCast(&tv), @sizeOf(CTimeval));
        handleChaosFd(client);
        _ = csock.close(client);
    }
}

// ── FakeMoMoServer (Direction 3 : Bank→MoMo cashout) ─────────────────────────
// Simple TCP server. Always responds with 0210/RC=00. No MAC, no chaos.

fn handleMoMoFd(fd: c_int) void {
    const alloc = std.heap.c_allocator;
    while (true) {
        var len_buf: [2]u8 = undefined;
        if (!readAllFd(fd, &len_buf)) return;
        const req_len = std.mem.readInt(u16, &len_buf, .big);
        if (req_len == 0 or req_len > 4096) return;
        const req_data = alloc.alloc(u8, req_len) catch return;
        defer alloc.free(req_data);
        if (!readAllFd(fd, req_data)) return;

        var iso_req = gw_mod.iso8583.parseWithSchema(req_data, &gw_mod.DEFAULT_SCHEMA, alloc) catch return;
        defer iso_req.deinit();
        const stan = iso_req.get(11) orelse "000000";

        var resp = gw_mod.iso8583.IsoMessage.init(alloc, "0210".*);
        defer resp.deinit();
        resp.set(39, "00") catch return;
        resp.set(11, stan) catch return;
        resp.set(49, "XAF") catch return;
        const bytes = gw_mod.iso8583.serializeWithSchema(&resp, &gw_mod.DEFAULT_SCHEMA, alloc) catch return;
        defer alloc.free(bytes);
        var lout: [2]u8 = undefined;
        std.mem.writeInt(u16, &lout, @intCast(bytes.len), .big);
        if (!writeAllFd(fd, &lout)) return;
        if (!writeAllFd(fd, bytes)) return;
    }
}

fn runMoMoWorker(momo_sock: c_int) void {
    while (true) {
        const client = csock.accept(momo_sock, null, null);
        if (client < 0) {
            if (g_stopped.load(.monotonic)) return;
            continue;
        }
        const tv = CTimeval{ .tv_sec = 10 };
        _ = csock.setsockopt(client, C_SOL_SOCKET, C_SO_RCVTIMEO, @ptrCast(&tv), @sizeOf(CTimeval));
        handleMoMoFd(client);
        _ = csock.close(client);
    }
}

// ── Cashout worker (Bank→MoMo direction) ─────────────────────────────────────

fn cashoutWorker() void {
    var threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const io = threaded.io();
    var client = gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = MOMO_PORT }, io);
    const alloc = std.heap.c_allocator;
    var ts_co: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts_co);
    var prng: u64 = @as(u64, @intCast(ts_co.sec)) *% 1_000_000_000 + @as(u64, @intCast(ts_co.nsec));

    var ts_slot: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts_slot);

    while (!g_stopped.load(.monotonic)) {
        prng ^= prng << 13;
        prng ^= prng >> 7;
        prng ^= prng << 17;
        var stan_buf: [6]u8 = undefined;
        _ = std.fmt.bufPrint(&stan_buf, "{d:0>6}", .{prng % 1_000_000}) catch continue;

        var iso = gw_mod.iso8583.IsoMessage.init(alloc, "0200".*);
        defer iso.deinit();
        iso.set(11, &stan_buf) catch continue;
        iso.set(32, "074") catch continue;
        iso.set(49, "XAF") catch continue;

        _ = g_cashout_sent.fetchAdd(1, .monotonic);
        if (client.send(&iso, alloc)) |resp| {
            defer @constCast(&resp).deinit();
            const rc = resp.get(39) orelse "";
            if (std.mem.eql(u8, rc, "00")) {
                _ = g_cashout_ok.fetchAdd(1, .monotonic);
            } else {
                _ = g_cashout_fail.fetchAdd(1, .monotonic);
            }
        } else |_| {
            _ = g_cashout_fail.fetchAdd(1, .monotonic);
        }

        // Cooperative D3 throttle — back off when D1 (MoMo→Bank) is degraded.
        // Prevents D3 from monopolising CPU/bank workers while D1 recovers.
        // Extra sleep is proportional to D1 deficit, capped at 4× slot time.
        const d1_live = g_d1_tps_live.load(.acquire);
        const peak = g_peak_tps.load(.monotonic);
        if (peak > 100 and d1_live < peak * 4 / 5) {
            // deficit_pct ∈ [0, 100]: how far below 80% of peak D1 currently is.
            const threshold = peak * 4 / 5;
            const deficit_pct = if (d1_live < threshold) (threshold - d1_live) * 100 / peak else 0;
            // 1% deficit → 40 µs backoff; 100% deficit → 4 ms backoff.
            const extra_ns: u64 = deficit_pct * 40_000;
            _ = std.c.nanosleep(&.{ .sec = 0, .nsec = @intCast(extra_ns) }, null);
        }

        // Rate-limit: sleep the remainder of the slot if the request was faster.
        var ts_now: std.c.timespec = undefined;
        _ = std.c.clock_gettime(.MONOTONIC, &ts_now);
        const elapsed_ns: i64 = (ts_now.sec - ts_slot.sec) * std.time.ns_per_s +
            (ts_now.nsec - ts_slot.nsec);
        if (elapsed_ns < CASHOUT_SLOT_NS) {
            const sleep_ns: i64 = CASHOUT_SLOT_NS - elapsed_ns;
            _ = std.c.nanosleep(&.{ .sec = 0, .nsec = @intCast(sleep_ns) }, null);
        }
        _ = std.c.clock_gettime(.MONOTONIC, &ts_slot);
    }
}

// ── BankServer D2 (Direction 2 : Banque → Broker) ────────────────────────────

const BankServerD2Args = struct {
    recon_state: *gw_mod.ReconciliationState,
};

fn runBankServerD2(args: BankServerD2Args) void {
    var srv_threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const srv_io = srv_threaded.io();
    const d2_broker = gw_mod.GatewayBrokerClient.init(.{ .host = "127.0.0.1", .port = BROKER_PORT }, srv_io);
    const bank_srv = std.heap.c_allocator.create(gw_mod.BankServer) catch return;
    bank_srv.* = gw_mod.BankServer.init(.{
        .host = "127.0.0.1",
        .port = BANKSERVER_PORT,
        .length_prefix = .two_byte_be,
        .topic = TOPIC_OUTBOUND,
    }, srv_io, &gw_mod.DEFAULT_SCHEMA, d2_broker);
    bank_srv.reconciliation = args.recon_state;
    bank_srv.serve(std.heap.c_allocator) catch {};
}

// Connect to BankServer, send 0200, wait for ACK. Returns true on success.
fn sendD2BankServer(d2_io: std.Io, alloc: std.mem.Allocator) bool {
    const addr = std.Io.net.IpAddress.parse("127.0.0.1", BANKSERVER_PORT) catch return false;
    var stream = addr.connect(d2_io, .{ .mode = .stream }) catch return false;
    defer stream.close(d2_io);

    var iso = gw_mod.iso8583.IsoMessage.init(alloc, "0200".*);
    defer iso.deinit();
    iso.set(11, "999001") catch return false;
    iso.set(49, "952") catch return false;

    const bytes = gw_mod.iso8583.serializeWithSchema(&iso, &gw_mod.DEFAULT_SCHEMA, alloc) catch return false;
    defer alloc.free(bytes);

    var wbuf: [4096]u8 = undefined;
    var sw = stream.writer(d2_io, &wbuf);
    sw.interface.writeInt(u16, @intCast(bytes.len), .big) catch return false;
    sw.interface.writeAll(bytes) catch return false;
    sw.interface.flush() catch return false;

    var rbuf: [1024]u8 = undefined;
    var sr = stream.reader(d2_io, &rbuf);
    var lb: [2]u8 = undefined;
    sr.interface.readSliceAll(&lb) catch return false;
    const rlen = std.mem.readInt(u16, &lb, .big);
    if (rlen == 0 or rlen > 4096) return false;
    const rdata = alloc.alloc(u8, rlen) catch return false;
    defer alloc.free(rdata);
    sr.interface.readSliceAll(rdata) catch return false;
    return true;
}

// ── D2 injector thread (sends 0200 to BankServer every 5 s) ──────────────────

fn d2InjectorWorker() void {
    var inj_threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const inj_io = inj_threaded.io();
    _ = std.c.nanosleep(&.{ .sec = 1, .nsec = 0 }, null); // let BankServer start
    while (!g_stopped.load(.monotonic)) {
        _ = std.c.nanosleep(&.{ .sec = 5, .nsec = 0 }, null);
        if (g_stopped.load(.monotonic)) break;
        _ = g_d2_sent.fetchAdd(1, .monotonic);
        if (sendD2BankServer(inj_io, std.heap.c_allocator))
            _ = g_d2_received.fetchAdd(1, .monotonic);
        _ = g_d2_total.fetchAdd(1, .monotonic);
    }
}

// ── Drain consumer — prevents broker deliver queue from accumulating ──────────
//
// Anonymous subscription (no sub_id) means no WAL replay on connect — only
// receives messages published after the subscribe frame arrives.  The router
// delivers inline while holding its mutex, so this consumer must drain fast.
// A loopback ACK roundtrip costs <20 µs; 20 000 ACK/s is well within budget.

fn drainCb(_: *const orusshare.InternalMessage, _: ?*anyopaque) void {}

fn drainConsumer() void {
    const alloc = std.heap.c_allocator;
    var dr_threaded = std.Io.Threaded.init(alloc, .{});
    const dr_io = dr_threaded.io();
    var client = gw_mod.GatewayBrokerClient.init(.{ .host = "127.0.0.1", .port = BROKER_PORT }, dr_io);
    while (!g_stopped.load(.monotonic)) {
        client.consume(TOPIC, alloc, drainCb, null) catch {};
        if (!g_stopped.load(.monotonic))
            _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 10_000_000 }, null); // 10 ms back-off
    }
}

// ── Heartbeat thread (0800/NMI=801 every 30 s) ───────────────────────────────

const HeartbeatArgs = struct { mac_engine: *gw_mod.MacEngine };

fn heartbeatWorker(args: HeartbeatArgs) void {
    var hb_threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const hb_io = hb_threaded.io();
    var hb_gw = gw_mod.Gateway.init(gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = CHAOS_BANK_PORT }, hb_io), 0);
    hb_gw.mac_engine = args.mac_engine;
    var hb_stan: u32 = 9000;
    var hb_buf: [65536]u8 = undefined;
    var hb_fba = std.heap.FixedBufferAllocator.init(&hb_buf);

    while (!g_stopped.load(.monotonic)) {
        _ = std.c.nanosleep(&.{ .sec = 30, .nsec = 0 }, null);
        if (g_stopped.load(.monotonic)) break;
        hb_fba.reset();
        if (hb_gw.sendHeartbeat(&hb_stan, hb_fba.allocator())) {
            _ = g_hb_ok.fetchAdd(1, .monotonic);
            var ts: std.c.timespec = undefined;
            _ = std.c.clock_gettime(.MONOTONIC, &ts);
            g_hb_last_ns.store(ts.sec *% std.time.ns_per_s + ts.nsec, .monotonic);
        } else |_| {
            _ = g_hb_fail.fetchAdd(1, .monotonic);
        }
    }
}

// ── Gateway worker ────────────────────────────────────────────────────────────

const GwArgs = struct { mac_engine: *gw_mod.MacEngine, audit: *orusshare.AuditLog, broker_port: u16 };

fn gatewayWorker(args: GwArgs) void {
    // Fixed 128 KB buffer per worker — reset each transaction, zero heap syscalls.
    var tx_buf: [131072]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&tx_buf);

    // Each worker owns its own Io event loop (kqueue on macOS).
    var worker_threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const worker_io = worker_threaded.io();
    var gw = gw_mod.Gateway.init(gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = CHAOS_BANK_PORT }, worker_io), 0);
    gw.mac_engine = args.mac_engine;
    // Each worker maintains a persistent broker connection for WAL publishing.
    var broker = gw_mod.GatewayBrokerClient.init(.{ .host = "127.0.0.1", .port = args.broker_port }, worker_io);
    defer broker.deinit();
    var prng: u64 = undefined;
    std.c.arc4random_buf(@ptrCast(&prng), 8);
    var ts_pub: std.c.timespec = undefined;

    while (!g_stopped.load(.monotonic)) {
        fba.reset(); // reuse the 128 KB buffer — free() on FBA is a no-op
        const alloc = fba.allocator();

        prng ^= prng << 13;
        prng ^= prng >> 7;
        prng ^= prng << 17;
        var id: [16]u8 = undefined;
        std.c.arc4random_buf(&id, id.len);
        var stan_buf: [6]u8 = undefined;
        _ = std.fmt.bufPrint(&stan_buf, "{d:0>6}", .{prng % 1_000_000}) catch continue;
        _ = std.c.clock_gettime(.MONOTONIC, &ts_pub);
        const t0: i64 = ts_pub.sec *% std.time.ns_per_s + ts_pub.nsec;

        const req = orusshare.InternalMessage{
            .msg_id = id,
            .schema_id = "gimac",
            .topic = TOPIC,
            .origin = .rest_json,
            .provider = .mtn_momo,
            .mti = "0200".*,
            .fields = &.{},
            .stan = stan_buf,
            .pan_hash = prng,
            .amount = @intCast(1_000 + prng % 99_000),
            .currency = "XAF".*,
            .received_at = 0,
            .source_ip = [_]u8{0} ** 16,
            .hop_count = 1,
            .external_id = null,
        };

        _ = g_sent.fetchAdd(1, .monotonic);

        const resp = gw.process(&req, alloc) catch |err| {
            switch (err) {
                error.MacMismatch => _ = g_err_mac.fetchAdd(1, .monotonic),
                else => _ = g_err_conn.fetchAdd(1, .monotonic),
            }
            continue;
        };
        defer {
            for (resp.fields) |f| alloc.free(f.value);
            alloc.free(resp.fields);
        }

        // Latency sample
        _ = std.c.clock_gettime(.MONOTONIC, &ts_pub);
        const t1: i64 = ts_pub.sec *% std.time.ns_per_s + ts_pub.nsec;
        if (t1 > t0) {
            const lat: u64 = @intCast(t1 - t0);
            const idx = g_lat_idx.fetchAdd(1, .monotonic) % LAT_CAP;
            g_lat[idx] = lat;
        }

        // RC tracking
        var rc: []const u8 = "";
        for (resp.fields) |f| if (f.id == 39) {
            rc = f.value;
        };
        const is_ok = std.mem.eql(u8, rc, "00");

        g_last_tx_ns.store(t1, .monotonic);

        // Audit before counting OK/declined — guarantees g_audit_lines <= responded.
        args.audit.record(&resp.mti, &resp.stan, resp.amount, &resp.currency, rc, @tagName(resp.provider), resp.msg_id);
        _ = g_d1_total.fetchAdd(1, .monotonic);
        _ = g_audit_lines.fetchAdd(1, .monotonic);

        if (is_ok) {
            _ = g_ok.fetchAdd(1, .monotonic);
            // 5% reversal injection on approved transactions
            prng ^= prng << 13;
            prng ^= prng >> 7;
            prng ^= prng << 17;
            if (prng % 100 < 5) {
                _ = g_reversals.fetchAdd(1, .monotonic);
                if (gw.processReversal(&req, alloc)) |rev| {
                    defer {
                        for (rev.fields) |f2| alloc.free(f2.value);
                        alloc.free(rev.fields);
                    }
                    var rev_rc: []const u8 = "";
                    for (rev.fields) |f2| if (f2.id == 39) {
                        rev_rc = f2.value;
                    };
                    if (std.mem.eql(u8, rev_rc, "00")) _ = g_rev_ok.fetchAdd(1, .monotonic) else _ = g_rev_fail.fetchAdd(1, .monotonic);
                } else |_| {
                    _ = g_rev_fail.fetchAdd(1, .monotonic);
                }
            }
        } else {
            _ = g_declined.fetchAdd(1, .monotonic);
        }

        // Publish to broker so every D1 transaction is durably WAL-recorded.
        // Best-effort: broker publish failure never aborts the payment path.
        broker.publish(&resp, alloc) catch {};
    }
}

// ── Ticker ────────────────────────────────────────────────────────────────────

fn ticker() void {
    while (true) {
        _ = std.c.nanosleep(&.{ .sec = 1, .nsec = 0 }, null);

        const cur_gw = g_ok.load(.monotonic) + g_declined.load(.monotonic);
        const tps_gw = cur_gw - g_prev_gw;
        g_prev_gw = cur_gw;

        _ = g_hist_gen.fetchAdd(1, .release);
        const head = g_hist_head.load(.monotonic);
        g_hist_gw[head] = tps_gw;
        g_hist_head.store((head + 1) % 60, .monotonic);
        const cur_n = g_hist_n.load(.monotonic);
        if (cur_n < 60) _ = g_hist_n.fetchAdd(1, .monotonic);
        _ = g_hist_gen.fetchAdd(1, .release);

        // Running peak — updated every second so it covers the full test duration.
        const prev_peak = g_peak_tps.load(.monotonic);
        if (tps_gw > prev_peak) g_peak_tps.store(tps_gw, .monotonic);

        // D1 tx/s (MoMo→Bank — same as gw_tps essentially)
        const cur_d1 = g_ok.load(.monotonic);
        const tps_d1 = cur_d1 - g_prev_d1;
        g_prev_d1 = cur_d1;
        // Expose live D1 TPS for cooperative D3 throttle.
        g_d1_tps_live.store(tps_d1, .release);
        const head1 = g_hist_d1_head.load(.monotonic);
        g_hist_d1[head1] = tps_d1;
        g_hist_d1_head.store((head1 + 1) % 60, .monotonic);
        const nd1 = g_hist_d1_n.load(.monotonic);
        if (nd1 < 60) _ = g_hist_d1_n.fetchAdd(1, .monotonic);

        // D2 tx/s (Bank→Broker)
        const cur_d2 = g_d2_received.load(.monotonic);
        const tps_d2 = cur_d2 - g_prev_d2;
        g_prev_d2 = cur_d2;
        const head2 = g_hist_d2_head.load(.monotonic);
        g_hist_d2[head2] = tps_d2;
        g_hist_d2_head.store((head2 + 1) % 60, .monotonic);
        const nd2 = g_hist_d2_n.load(.monotonic);
        if (nd2 < 60) _ = g_hist_d2_n.fetchAdd(1, .monotonic);

        // D3 tx/s (Bank→MoMo cashout)
        const cur_d3 = g_cashout_ok.load(.monotonic);
        const tps_d3 = cur_d3 - g_prev_d3;
        g_prev_d3 = cur_d3;
        const head3 = g_hist_d3_head.load(.monotonic);
        g_hist_d3[head3] = tps_d3;
        g_hist_d3_head.store((head3 + 1) % 60, .monotonic);
        const nd3 = g_hist_d3_n.load(.monotonic);
        if (nd3 < 60) _ = g_hist_d3_n.fetchAdd(1, .monotonic);

        // WAL size — O_RDONLY=0, SEEK_END=2 (POSIX)
        const wfd = csock.open(WAL_PATH, 0);
        if (wfd >= 0) {
            const wsz = csock.lseek(wfd, 0, 2);
            if (wsz >= 0) g_wal_bytes.store(@intCast(wsz), .monotonic);
            _ = csock.close(wfd);
        }
    }
}

// Percentiles computed on-demand from the ring buffer (no seqlock race possible).
fn computePercentiles() struct { p50: u64, p99: u64 } {
    const n_raw = g_lat_idx.load(.monotonic);
    const n = @min(n_raw, LAT_CAP);
    if (n <= 1) return .{ .p50 = 0, .p99 = 0 };
    var tmp: [LAT_CAP]u64 = undefined;
    @memcpy(tmp[0..n], g_lat[0..n]); // lock-free snapshot; rare torn u64 read is acceptable for percentiles
    std.mem.sort(u64, tmp[0..n], {}, std.sort.asc(u64));
    const raw50 = tmp[n / 2];
    const raw99 = tmp[n * 99 / 100];
    return .{
        .p50 = @min(raw50, raw99) / 1000,
        .p99 = @max(raw50, raw99) / 1000,
    };
}

// ── HTTP ──────────────────────────────────────────────────────────────────────
// Uses libc socket() directly — NOT std.Io.net — so macOS doesn't mark the
// socket with kqueue/non-blocking flags that cause EADDRNOTAVAIL on loopback.

// libc socket API (std.posix was gutted in Zig 0.16 in favour of std.Io)
const csock = struct {
    pub extern "c" fn socket(domain: c_int, type_: c_int, protocol: c_int) c_int;
    pub extern "c" fn setsockopt(fd: c_int, level: c_int, optname: c_int, optval: ?*const anyopaque, optlen: c_uint) c_int;
    pub extern "c" fn bind(fd: c_int, addr: ?*const anyopaque, addrlen: c_uint) c_int;
    pub extern "c" fn listen(fd: c_int, backlog: c_int) c_int;
    pub extern "c" fn accept(fd: c_int, addr: ?*anyopaque, addrlen: ?*c_uint) c_int;
    pub extern "c" fn close(fd: c_int) c_int;
    pub extern "c" fn read(fd: c_int, buf: ?*anyopaque, nbytes: usize) isize;
    pub extern "c" fn write(fd: c_int, buf: ?*const anyopaque, nbytes: usize) isize;
    pub extern "c" fn open(path: [*:0]const u8, flags: c_int, ...) c_int;
    pub extern "c" fn lseek(fd: c_int, offset: i64, whence: c_int) i64;
};
// macOS AF_INET / SOCK_STREAM / SOL_SOCKET / SO_REUSEADDR constants
const C_AF_INET: c_int = 2;
const C_SOCK_STREAM: c_int = 1;
const C_IPPROTO_TCP: c_int = 6;
const C_SOL_SOCKET: c_int = 0xffff;
const C_SO_REUSEADDR: c_int = 0x0004;
const C_SO_REUSEPORT: c_int = 0x0200;
const C_SO_RCVTIMEO: c_int = 0x1006; // macOS: receive timeout for accepted sockets
// macOS timeval (64-bit): tv_sec=long(8), tv_usec=int(4) + 4 pad = 16 bytes
const CTimeval = extern struct { tv_sec: i64 = 0, tv_usec: i32 = 0, _pad: i32 = 0 };
// macOS sockaddr_in (has sin_len prefix byte)
const SockaddrIn = extern struct {
    sin_len: u8 = @sizeOf(SockaddrIn),
    sin_family: u8 = 2,
    sin_port: u16 = 0,
    sin_addr: u32 = 0,
    sin_zero: [8]u8 = .{0} ** 8,
};

const HttpFd = c_int;
const HttpCtx = struct { fd: HttpFd };

fn httpWrite(fd: HttpFd, data: []const u8) void {
    var off: usize = 0;
    while (off < data.len) {
        const n = csock.write(fd, data[off..].ptr, data[off..].len);
        if (n <= 0) return;
        off += @intCast(n);
    }
}

fn httpReadByte(fd: HttpFd) ?u8 {
    var b: u8 = 0;
    const n = csock.read(fd, &b, 1);
    if (n <= 0) return null;
    return b;
}

fn wp(buf: []u8, pos: *usize, comptime fmt: []const u8, args: anytype) void {
    const s = std.fmt.bufPrint(buf[pos.*..], fmt, args) catch return;
    pos.* += s.len;
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
                // strip trailing " HTTP/1.1" or whitespace (last URL param)
                if (std.mem.indexOfScalar(u8, val, ' ')) |sp| val = val[0..sp];
                return std.fmt.parseInt(u32, val, 10) catch null;
            }
        }
        rest = if (amp < rest.len) rest[amp + 1 ..] else "";
    }
    return null;
}

fn serveMetrics(fd: HttpFd) void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const now_ns: i64 = ts.sec *% std.time.ns_per_s + ts.nsec;
    const elapsed: u64 = @intCast(@max(1, @divTrunc(now_ns - g_start_ns, std.time.ns_per_s)));

    var hgw: [60]u64 = undefined;
    var hn: usize = 0;
    while (true) {
        const gen1 = g_hist_gen.load(.acquire);
        if (gen1 % 2 != 0) continue;
        hn = g_hist_n.load(.monotonic);
        const hstart: usize = if (hn < 60) 0 else g_hist_head.load(.monotonic);
        for (0..hn) |i| hgw[i] = g_hist_gw[(hstart + i) % 60];
        const gen2 = g_hist_gen.load(.acquire);
        if (gen1 == gen2) break;
    }
    const pct = computePercentiles();
    const p50 = pct.p50;
    const p99 = pct.p99;

    const sent = g_sent.load(.monotonic);
    const ok = g_ok.load(.monotonic);
    const declined = g_declined.load(.monotonic);
    const err_conn = g_err_conn.load(.monotonic);
    const err_mac = g_err_mac.load(.monotonic);
    const in_flight: i64 = @as(i64, @intCast(sent)) - @as(i64, @intCast(ok + declined + err_conn + err_mac));

    var ru: std.c.rusage = undefined;
    _ = std.c.getrusage(0, &ru);
    const raw_rss: usize = @intCast(ru.maxrss);
    const rss_mb = if (builtin.os.tag == .macos) raw_rss / (1024 * 1024) else raw_rss / 1024;

    var body: [262144]u8 = undefined;
    var pos: usize = 0;
    wp(&body, &pos, "{{", .{});
    wp(&body, &pos, "\"elapsed_s\":{d}", .{elapsed});
    wp(&body, &pos, ",\"sent\":{d}", .{sent});
    wp(&body, &pos, ",\"ok\":{d}", .{ok});
    wp(&body, &pos, ",\"declined\":{d}", .{declined});
    wp(&body, &pos, ",\"err_conn\":{d}", .{err_conn});
    wp(&body, &pos, ",\"err_mac\":{d}", .{err_mac});
    wp(&body, &pos, ",\"in_flight\":{d}", .{in_flight});
    wp(&body, &pos, ",\"bank_noresp\":{d}", .{g_bank_noresp.load(.monotonic)});
    wp(&body, &pos, ",\"bank_rc00\":{d}", .{g_bank_rc00.load(.monotonic)});
    wp(&body, &pos, ",\"bank_rc51\":{d}", .{g_bank_rc51.load(.monotonic)});
    wp(&body, &pos, ",\"bank_rc91\":{d}", .{g_bank_rc91.load(.monotonic)});
    wp(&body, &pos, ",\"latency_ms\":{d}", .{g_latency_ms.load(.monotonic)});
    wp(&body, &pos, ",\"noresp_pct\":{d}", .{g_noresp_pct.load(.monotonic)});
    wp(&body, &pos, ",\"rc51_pct\":{d}", .{g_rc51_pct.load(.monotonic)});
    wp(&body, &pos, ",\"rc91_pct\":{d}", .{g_rc91_pct.load(.monotonic)});
    wp(&body, &pos, ",\"p50_us\":{d}", .{p50});
    wp(&body, &pos, ",\"p99_us\":{d}", .{p99});
    wp(&body, &pos, ",\"rss_mb\":{d}", .{rss_mb});
    wp(&body, &pos, ",\"gw_tps\":{d}", .{if (hn > 0) hgw[hn - 1] else 0});
    wp(&body, &pos, ",\"peak_tps\":{d}", .{g_peak_tps.load(.monotonic)});
    // Pipeline health
    const hb_last = g_hb_last_ns.load(.monotonic);
    const hb_since_s: i64 = if (hb_last > 0) @divTrunc(now_ns - hb_last, std.time.ns_per_s) else -1;

    const last_tx = g_last_tx_ns.load(.monotonic);
    const tx_since_s: i64 = if (last_tx > 0) @divTrunc(now_ns - last_tx, std.time.ns_per_s) else -1;
    wp(&body, &pos, ",\"startup_d0\":{d}", .{@intFromBool(g_startup_d0.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d1\":{d}", .{@intFromBool(g_startup_d1.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d2\":{d}", .{@intFromBool(g_startup_d2.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d3\":{d}", .{@intFromBool(g_startup_d3.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d4\":{d}", .{@intFromBool(g_startup_d4.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d5\":{d}", .{@intFromBool(g_startup_d5.load(.monotonic))});
    wp(&body, &pos, ",\"startup_d6\":{d}", .{@intFromBool(g_startup_d6.load(.monotonic))});
    wp(&body, &pos, ",\"cashout_sent\":{d}", .{g_cashout_sent.load(.monotonic)});
    wp(&body, &pos, ",\"cashout_ok\":{d}", .{g_cashout_ok.load(.monotonic)});
    wp(&body, &pos, ",\"cashout_fail\":{d}", .{g_cashout_fail.load(.monotonic)});
    wp(&body, &pos, ",\"hb_ok\":{d}", .{g_hb_ok.load(.monotonic)});
    wp(&body, &pos, ",\"hb_fail\":{d}", .{g_hb_fail.load(.monotonic)});
    wp(&body, &pos, ",\"hb_since_s\":{d}", .{hb_since_s});
    wp(&body, &pos, ",\"tx_since_s\":{d}", .{tx_since_s});
    wp(&body, &pos, ",\"reversals\":{d}", .{g_reversals.load(.monotonic)});
    wp(&body, &pos, ",\"rev_ok\":{d}", .{g_rev_ok.load(.monotonic)});
    wp(&body, &pos, ",\"rev_fail\":{d}", .{g_rev_fail.load(.monotonic)});
    wp(&body, &pos, ",\"d2_sent\":{d}", .{g_d2_sent.load(.monotonic)});
    wp(&body, &pos, ",\"d2_received\":{d}", .{g_d2_received.load(.monotonic)});
    wp(&body, &pos, ",\"audit_lines\":{d}", .{g_audit_lines.load(.monotonic)});
    wp(&body, &pos, ",\"wal_kb\":{d}", .{g_wal_bytes.load(.monotonic) / 1024});
    wp(&body, &pos, ",\"mac_active\":{d}", .{@intFromBool(g_mac_active.load(.monotonic))});
    wp(&body, &pos, ",\"hist_gw\":[", .{});
    for (0..hn) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{hgw[i]});
    }
    wp(&body, &pos, "],\"hist_d1\":[", .{});
    const nd1 = g_hist_d1_n.load(.monotonic);
    const hstart1: usize = if (nd1 < 60) 0 else g_hist_d1_head.load(.monotonic);
    for (0..nd1) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{g_hist_d1[(hstart1 + i) % 60]});
    }
    wp(&body, &pos, "],\"hist_d2\":[", .{});
    const nd2 = g_hist_d2_n.load(.monotonic);
    const hstart2: usize = if (nd2 < 60) 0 else g_hist_d2_head.load(.monotonic);
    for (0..nd2) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{g_hist_d2[(hstart2 + i) % 60]});
    }
    wp(&body, &pos, "],\"hist_d3\":[", .{});
    const nd3_api = g_hist_d3_n.load(.monotonic);
    const hstart3: usize = if (nd3_api < 60) 0 else g_hist_d3_head.load(.monotonic);
    for (0..nd3_api) |i| {
        if (i > 0) wp(&body, &pos, ",", .{});
        wp(&body, &pos, "{d}", .{g_hist_d3[(hstart3 + i) % 60]});
    }
    const tps_d3_now: u64 = if (nd3_api > 0) g_hist_d3[(g_hist_d3_head.load(.monotonic) + 59) % 60] else 0;
    wp(&body, &pos, "],\"tps_d3\":{d}", .{tps_d3_now});
    wp(&body, &pos, ",\"total_delivered\":{d}", .{ok + declined});
    wp(&body, &pos, ",\"d1_total\":{d}", .{g_d1_total.load(.monotonic)});
    wp(&body, &pos, ",\"d2_total\":{d}", .{g_d2_total.load(.monotonic)});
    wp(&body, &pos, "}}", .{});

    const body_s = body[0..pos];
    var hdr: [256]u8 = undefined;
    const hdr_s = std.fmt.bufPrint(&hdr, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{body_s.len}) catch return;
    httpWrite(fd, hdr_s);
    httpWrite(fd, body_s);
}

fn handleChaosPost(path: []const u8, fd: HttpFd) void {
    if (parseU32Param(path, "latency_ms")) |v| g_latency_ms.store(@min(v, 2000), .monotonic);
    if (parseU32Param(path, "noresp_pct")) |v| g_noresp_pct.store(@min(v, 100), .monotonic);
    if (parseU32Param(path, "rc51_pct")) |v| g_rc51_pct.store(@min(v, 100), .monotonic);
    if (parseU32Param(path, "rc91_pct")) |v| g_rc91_pct.store(@min(v, 100), .monotonic);
    httpWrite(fd, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n");
}

fn serveDashboard(fd: HttpFd) void {
    var hdr: [256]u8 = undefined;
    const hdr_s = std.fmt.bufPrint(&hdr, "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{DASHBOARD_HTML.len}) catch return;
    httpWrite(fd, hdr_s);
    httpWrite(fd, DASHBOARD_HTML);
}

fn httpConn(ctx: HttpCtx) void {
    defer _ = csock.close(ctx.fd);
    var line: [512]u8 = undefined;
    var ln: usize = 0;
    while (ln < line.len) {
        const b = httpReadByte(ctx.fd) orelse return;
        if (b == '\n') break;
        if (b != '\r') {
            line[ln] = b;
            ln += 1;
        }
    }
    // Drain remaining headers
    var tail: [4]u8 = .{ 0, 0, 0, 0 };
    while (true) {
        const b = httpReadByte(ctx.fd) orelse break;
        tail[0] = tail[1];
        tail[1] = tail[2];
        tail[2] = tail[3];
        tail[3] = b;
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

fn httpServer() void {
    const sock = csock.socket(C_AF_INET, C_SOCK_STREAM, C_IPPROTO_TCP);
    if (sock < 0) {
        std.log.err("HTTP socket() failed", .{});
        return;
    }
    defer _ = csock.close(sock);

    const opt: c_int = 1;
    _ = csock.setsockopt(sock, C_SOL_SOCKET, C_SO_REUSEADDR, @ptrCast(&opt), @sizeOf(c_int));
    _ = csock.setsockopt(sock, C_SOL_SOCKET, C_SO_REUSEPORT, @ptrCast(&opt), @sizeOf(c_int));

    const addr = SockaddrIn{
        .sin_port = std.mem.nativeToBig(u16, HTTP_PORT),
        .sin_addr = std.mem.nativeToBig(u32, 0x7F000001),
    };
    if (csock.bind(sock, &addr, @sizeOf(SockaddrIn)) < 0) {
        std.log.err("HTTP bind() failed", .{});
        return;
    }
    if (csock.listen(sock, 64) < 0) {
        std.log.err("HTTP listen() failed", .{});
        return;
    }
    std.log.info("Battle test dashboard : \x1b[1;35mhttp://127.0.0.1:{d}\x1b[0m", .{HTTP_PORT});

    while (!g_stopped.load(.monotonic)) {
        const client = csock.accept(sock, null, null);
        if (client < 0) {
            if (g_stopped.load(.monotonic)) break;
            continue;
        }
        // 5-second receive timeout — prevents httpConn threads from blocking forever
        // on slow/incomplete browser requests (Chrome speculative connections, etc.)
        const tv = CTimeval{ .tv_sec = 5 };
        _ = csock.setsockopt(client, C_SOL_SOCKET, C_SO_RCVTIMEO, @ptrCast(&tv), @sizeOf(CTimeval));
        const t = std.Thread.spawn(.{}, httpConn, .{HttpCtx{ .fd = client }}) catch {
            _ = csock.close(client);
            continue;
        };
        t.detach();
    }
}

// ── Startup validation helper ─────────────────────────────────────────────────

fn printStartup(name: []const u8, success: bool) void {
    if (success) {
        std.debug.print("  \x1b[32m✓\x1b[0m {s}\n", .{name});
    } else {
        std.debug.print("  \x1b[31m✗\x1b[0m {s}\n", .{name});
    }
}

// ── Final integrity report ────────────────────────────────────────────────────

fn printReport(_: std.Io) void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const now_ns: i64 = ts.sec *% std.time.ns_per_s + ts.nsec;
    const elapsed: u64 = @intCast(@max(1, @divTrunc(now_ns - g_start_ns, std.time.ns_per_s)));

    const sent = g_sent.load(.monotonic);
    const ok = g_ok.load(.monotonic);
    const declined = g_declined.load(.monotonic);
    const err_conn = g_err_conn.load(.monotonic);
    const err_mac = g_err_mac.load(.monotonic);
    const responded = ok + declined;

    // Use the atomic counter — avoids the buffered-not-flushed race with file counting.
    // Audit is recorded before g_ok/g_declined, so audit_lines <= responded always.
    const audit_lines = g_audit_lines.load(.monotonic);

    const peak_tps = g_peak_tps.load(.monotonic);

    // Latency
    const pct = computePercentiles();

    // Rates
    const success_pct: u64 = if (sent > 0) responded * 100 / sent else 0;
    const err_pct: u64 = if (sent > 0) (err_conn + err_mac) * 100 / sent else 0;
    const avg_tps: u64 = responded / elapsed;

    // Chaos config at stop
    const chaos_lat = g_latency_ms.load(.monotonic);
    const chaos_nrsp = g_noresp_pct.load(.monotonic);
    const chaos_r51 = g_rc51_pct.load(.monotonic);
    const chaos_r91 = g_rc91_pct.load(.monotonic);

    const pass = (err_mac == 0) and (audit_lines == responded) and (err_conn * 100 / @max(sent, 1) < 5);

    std.debug.print("\n\x1b[1;35m╔══════════════════════════════════════════════════════╗\x1b[0m\n", .{});
    std.debug.print("\x1b[1;35m║          BATTLE TEST RECAP — {d:>4}s                   ║\x1b[0m\n", .{elapsed});
    std.debug.print("\x1b[1;35m╚══════════════════════════════════════════════════════╝\x1b[0m\n\n", .{});

    std.debug.print("  \x1b[1mTransactions\x1b[0m\n", .{});
    std.debug.print("    Total envoyées   : {d}\n", .{sent});
    std.debug.print("    OK               : \x1b[32m{d}\x1b[0m  ({d} approuvées + {d} déclinées)\n", .{ responded, ok, declined });
    std.debug.print("    Erreurs connexion: \x1b[{s}m{d}\x1b[0m\n", .{ if (err_conn == 0) "32" else "33", err_conn });
    std.debug.print("    Erreurs MAC      : \x1b[{s}m{d}\x1b[0m\n", .{ if (err_mac == 0) "32" else "31", err_mac });
    std.debug.print("    Taux de succès   : \x1b[{s}m{d}%\x1b[0m  (erreurs: {d}%)\n\n", .{ if (success_pct >= 95) "32" else if (success_pct >= 80) "33" else "31", success_pct, err_pct });

    std.debug.print("  \x1b[1mThroughput\x1b[0m\n", .{});
    std.debug.print("    Moyenne          : {d} tx/s\n", .{avg_tps});
    std.debug.print("    Peak             : \x1b[1m{d} tx/s\x1b[0m\n\n", .{peak_tps});

    std.debug.print("  \x1b[1mLatence (last {d} samples)\x1b[0m\n", .{@min(g_lat_idx.load(.monotonic), LAT_CAP)});
    std.debug.print("    P50              : {d} µs\n", .{pct.p50});
    std.debug.print("    P99              : {d} µs\n\n", .{pct.p99});

    std.debug.print("  \x1b[1mSécurité MAC (GIMAC/BEAC)\x1b[0m\n", .{});
    std.debug.print("    Statut           : {s}\n", .{if (g_mac_active.load(.monotonic)) "\x1b[32mACTIF (HMAC-SHA256)\x1b[0m" else "\x1b[33mINACTIF\x1b[0m"});
    std.debug.print("    Violations MAC   : \x1b[{s}m{d}\x1b[0m\n\n", .{ if (err_mac == 0) "32" else "31", err_mac });

    std.debug.print("  \x1b[1mAudit log (non-répudiation BEAC)\x1b[0m\n", .{});
    std.debug.print("    Lignes enregistrées : {d}\n", .{audit_lines});
    std.debug.print("    Attendues           : {d}\n", .{responded});
    if (audit_lines == responded) {
        std.debug.print("    Intégrité           : \x1b[1;32m✓ INTACT\x1b[0m\n\n", .{});
    } else if (audit_lines > responded) {
        std.debug.print("    Intégrité           : \x1b[1;31m✗ DOUBLONS +{d}\x1b[0m\n\n", .{audit_lines - responded});
    } else {
        std.debug.print("    Intégrité           : \x1b[1;31m✗ PERTES -{d}\x1b[0m\n\n", .{responded - audit_lines});
    }

    std.debug.print("  \x1b[1mValidation pipeline D0–D6\x1b[0m\n", .{});
    std.debug.print("    D0 Sign-on  0800/301 : {s}\n", .{if (g_startup_d0.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D1 MoMo→Bank 0200    : {s}\n", .{if (g_startup_d1.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D2 Bank→Broker       : {s}\n", .{if (g_startup_d2.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D3 Reversal 0420     : {s}\n", .{if (g_startup_d3.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D4 Réconcil. 0500    : {s}\n", .{if (g_startup_d4.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D5 Heartbeat 0800    : {s}\n", .{if (g_startup_d5.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D6 Cashout Bank→MoMo : {s}\n\n", .{if (g_startup_d6.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});

    std.debug.print("  \x1b[1mHeartbeat (30 s)\x1b[0m\n", .{});
    std.debug.print("    OK               : \x1b[32m{d}\x1b[0m\n", .{g_hb_ok.load(.monotonic)});
    std.debug.print("    Échecs           : \x1b[{s}m{d}\x1b[0m\n\n", .{ if (g_hb_fail.load(.monotonic) == 0) "32" else "33", g_hb_fail.load(.monotonic) });

    const rev_total = g_reversals.load(.monotonic);
    std.debug.print("  \x1b[1mReversals injectés (5%% des RC=00)\x1b[0m\n", .{});
    std.debug.print("    Injectés         : {d}\n", .{rev_total});
    std.debug.print("    Confirmés 0430   : \x1b[32m{d}\x1b[0m\n", .{g_rev_ok.load(.monotonic)});
    std.debug.print("    Échecs           : \x1b[{s}m{d}\x1b[0m\n\n", .{ if (g_rev_fail.load(.monotonic) == 0) "32" else "33", g_rev_fail.load(.monotonic) });

    std.debug.print("  \x1b[1mDirection 2 — BankServer (port {d})\x1b[0m\n", .{BANKSERVER_PORT});
    std.debug.print("    Injections 0200  : {d}\n", .{g_d2_sent.load(.monotonic)});
    std.debug.print("    ACK reçus 0210   : \x1b[32m{d}\x1b[0m\n\n", .{g_d2_received.load(.monotonic)});

    std.debug.print("  \x1b[1mDirection 3 — Cashout Bank→MoMo (port {d})\x1b[0m\n", .{MOMO_PORT});
    std.debug.print("    Envoyés          : {d}\n", .{g_cashout_sent.load(.monotonic)});
    std.debug.print("    OK               : \x1b[32m{d}\x1b[0m\n", .{g_cashout_ok.load(.monotonic)});
    std.debug.print("    Échecs           : \x1b[{s}m{d}\x1b[0m\n\n", .{ if (g_cashout_fail.load(.monotonic) == 0) "32" else "33", g_cashout_fail.load(.monotonic) });

    std.debug.print("  \x1b[1mChaos bank (config à l'arrêt)\x1b[0m\n", .{});
    std.debug.print("    Latence injectée : {d} ms\n", .{chaos_lat});
    std.debug.print("    Sans réponse     : {d}%\n", .{chaos_nrsp});
    std.debug.print("    RC=51            : {d}%\n", .{chaos_r51});
    std.debug.print("    RC=91            : {d}%\n", .{chaos_r91});
    std.debug.print("    Traitées bank    : rc00={d}  rc51={d}  rc91={d}  noresp={d}\n\n", .{ g_bank_rc00.load(.monotonic), g_bank_rc51.load(.monotonic), g_bank_rc91.load(.monotonic), g_bank_noresp.load(.monotonic) });

    if (pass) {
        std.debug.print("  \x1b[1;32m✓ VERDICT : PASS\x1b[0m  — intégrité OK, sécurité OK, <5% erreurs\n\n", .{});
    } else {
        std.debug.print("  \x1b[1;31m✗ VERDICT : FAIL\x1b[0m\n", .{});
        if (err_mac > 0) std.debug.print("    → violations MAC détectées\n", .{});
        if (audit_lines != responded) std.debug.print("    → audit log incohérent\n", .{});
        if (err_conn * 100 / @max(sent, 1) >= 5)
            std.debug.print("    → taux d'erreurs connexion ≥5%\n", .{});
        std.debug.print("\n", .{});
    }
    std.debug.print("\x1b[1;35m══════════════════════════════════════════════════════\x1b[0m\n\n", .{});
}

// ── main ──────────────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    const sa = std.posix.Sigaction{
        .handler = .{ .handler = sigHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT, &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    std.Io.Dir.cwd().deleteFile(io, WAL_PATH) catch {};
    std.Io.Dir.cwd().deleteFile(io, AUDIT_PATH) catch {};

    var ts0: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts0);
    g_start_ns = ts0.sec *% std.time.ns_per_s + ts0.nsec;

    // ── Broker ────────────────────────────────────────────────────────────────
    const inner = try std.heap.c_allocator.create(BrokerInner);
    inner.wal = try orusbroker.Wal.open(io, WAL_PATH);
    inner.wal.sync_every = 0; // bench mode: buffer writes, flush on close
    inner.dedup = orusbroker.DedupFilter.init();
    inner.router = orusbroker.TopicRouter.init(std.heap.c_allocator);
    inner.server = orusbroker.BrokerServer{
        .config = .{ .host = "127.0.0.1", .port = BROKER_PORT, .wal_path = WAL_PATH, .cursor_dir = "/tmp/battle_cursors" },
        .io = io,
        .wal = &inner.wal,
        .dedup = &inner.dedup,
        .router = &inner.router,
        .cursor = orusbroker.Cursor.init("/tmp/battle_cursors", io),
    };
    (try std.Thread.spawn(.{}, runBroker, .{inner})).detach();
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 80_000_000 }, null);

    // ── FakeChaosBank (libc sockets, pre-spawned pool) ────────────────────────
    chaos_mac.setKey(SESSION_KEY);
    const bank_sock: c_int = blk: {
        const s = csock.socket(C_AF_INET, C_SOCK_STREAM, C_IPPROTO_TCP);
        if (s < 0) return error.ChaosBankSocket;
        const opt: c_int = 1;
        _ = csock.setsockopt(s, C_SOL_SOCKET, C_SO_REUSEADDR, @ptrCast(&opt), @sizeOf(c_int));
        _ = csock.setsockopt(s, C_SOL_SOCKET, C_SO_REUSEPORT, @ptrCast(&opt), @sizeOf(c_int));
        const baddr = SockaddrIn{
            .sin_port = std.mem.nativeToBig(u16, CHAOS_BANK_PORT),
            .sin_addr = std.mem.nativeToBig(u32, 0x7F000001),
        };
        if (csock.bind(s, &baddr, @sizeOf(SockaddrIn)) < 0) return error.ChaosBankBind;
        if (csock.listen(s, 1024) < 0) return error.ChaosBankListen;
        break :blk s;
    };
    // Bank pool = n_d1 + n_d3 + 14 slack (heartbeat, sign-on, D0–D6 startup probes).
    // Sized at runtime so the pool always covers the exact worker count detected.
    const cpu_count_bank: usize = std.Thread.getCpuCount() catch 8;
    const n_bank: usize = @min(512, cpu_count_bank * 32) + @max(1, cpu_count_bank / 2) + 14;
    for (0..n_bank) |_| (try std.Thread.spawn(.{}, runChaosBankWorker, .{bank_sock})).detach();
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);

    // ── MacEngine + AuditLog ──────────────────────────────────────────────────
    var mac_engine = gw_mod.MacEngine.init();
    var audit = try orusshare.AuditLog.open(io, AUDIT_PATH);
    // sync_every=0 : writes buffered, flushed on close() — integrity count still exact.
    // Set to 1 only when testing BEAC crash-recovery compliance (kills throughput 10x).
    audit.sync_every = 0;
    defer audit.close();

    // ── ReconciliationState (partagé BankServer D2) ───────────────────────────
    var recon_state = gw_mod.ReconciliationState.init();

    // ── BankServer D2 ─────────────────────────────────────────────────────────
    const bs_args = BankServerD2Args{ .recon_state = &recon_state };
    (try std.Thread.spawn(.{}, runBankServerD2, .{bs_args})).detach();
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 150_000_000 }, null);

    // ── Sign-on avec retry (timing race au démarrage) ────────────────────────
    var sign_gw = gw_mod.Gateway.init(gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = CHAOS_BANK_PORT }, io), 0);
    sign_gw.mac_engine = &mac_engine;
    var stan: u32 = 0;
    for (0..5) |attempt| {
        sign_gw.signOn(&stan, alloc) catch {
            if (attempt < 4) {
                _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 150_000_000 }, null);
                continue;
            }
            std.log.warn("sign-on failed après 5 tentatives — MAC inactive", .{});
            break;
        };
        break;
    }
    if (mac_engine.active) {
        g_mac_active.store(true, .monotonic);
        std.log.info("Sign-on OK — MAC active (HMAC-SHA256)", .{});
    }

    // ── Startup validation D0–D5 ──────────────────────────────────────────────
    std.debug.print("\n\x1b[1;36m══ VALIDATION PIPELINE D0–D5 ══════════════════════════\x1b[0m\n", .{});

    // D0 — Sign-on (done above)
    g_startup_d0.store(mac_engine.active, .monotonic);
    printStartup("D0  Sign-on 0800/301 → 0810", mac_engine.active);

    // D1 — MoMo → ChaosBank 0200 → 0210
    d1: {
        var d1_id: [16]u8 = undefined;
        std.c.arc4random_buf(&d1_id, 16);
        const d1_req = orusshare.InternalMessage{
            .msg_id = d1_id,
            .schema_id = "gimac",
            .topic = TOPIC,
            .origin = .rest_json,
            .provider = .mtn_momo,
            .mti = "0200".*,
            .fields = &.{},
            .stan = "000001".*,
            .pan_hash = 0,
            .amount = 1000,
            .currency = "XAF".*,
            .received_at = 0,
            .source_ip = [_]u8{0} ** 16,
            .hop_count = 1,
            .external_id = null,
        };
        if (sign_gw.process(&d1_req, alloc)) |d1_resp| {
            defer {
                for (d1_resp.fields) |f| alloc.free(f.value);
                alloc.free(d1_resp.fields);
            }
            g_startup_d1.store(true, .monotonic);
            printStartup("D1  MoMo→Bank 0200 → 0210", true);
        } else |_| {
            printStartup("D1  MoMo→Bank 0200 → 0210", false);
        }
        break :d1;
    }

    // D2 — 0200 → BankServer → ACK 0210
    {
        const d2_ok = sendD2BankServer(io, alloc);
        g_startup_d2.store(d2_ok, .monotonic);
        printStartup("D2  0200→BankServer→Broker", d2_ok);
    }

    // D3 — Reversal 0420 → ChaosBank → 0430
    d3: {
        var d3_id: [16]u8 = undefined;
        std.c.arc4random_buf(&d3_id, 16);
        const d3_orig = orusshare.InternalMessage{
            .msg_id = d3_id,
            .schema_id = "gimac",
            .topic = TOPIC,
            .origin = .rest_json,
            .provider = .mtn_momo,
            .mti = "0200".*,
            .fields = &.{},
            .stan = "001235".*,
            .pan_hash = 0,
            .amount = 5000,
            .currency = "XAF".*,
            .received_at = 0,
            .source_ip = [_]u8{0} ** 16,
            .hop_count = 1,
            .external_id = null,
        };
        if (sign_gw.processReversal(&d3_orig, alloc)) |rev| {
            defer {
                for (rev.fields) |f| alloc.free(f.value);
                alloc.free(rev.fields);
            }
            var rev_rc: []const u8 = "";
            for (rev.fields) |f| if (f.id == 39) {
                rev_rc = f.value;
            };
            const d3_ok = std.mem.eql(u8, &rev.mti, "0430") and std.mem.eql(u8, rev_rc, "00");
            g_startup_d3.store(d3_ok, .monotonic);
            printStartup("D3  Reversal 0420 → 0430", d3_ok);
        } else |_| {
            printStartup("D3  Reversal 0420 → 0430", false);
        }
        break :d3;
    }

    // D4 — Réconciliation 0500 → BankServer → 0510
    d4: {
        const d4_addr = std.Io.net.IpAddress.parse("127.0.0.1", BANKSERVER_PORT) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };
        var d4_stream = d4_addr.connect(io, .{ .mode = .stream }) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };
        defer d4_stream.close(io);

        var d4_iso = gw_mod.iso8583.IsoMessage.init(alloc, "0500".*);
        defer d4_iso.deinit();
        d4_iso.set(11, "000099") catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };
        const d4_bytes = gw_mod.iso8583.serializeWithSchema(&d4_iso, &gw_mod.DEFAULT_SCHEMA, alloc) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };
        defer alloc.free(d4_bytes);

        var d4_wbuf: [4096]u8 = undefined;
        var d4_sw = d4_stream.writer(io, &d4_wbuf);
        d4_sw.interface.writeInt(u16, @intCast(d4_bytes.len), .big) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };
        d4_sw.interface.writeAll(d4_bytes) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };
        d4_sw.interface.flush() catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };

        var d4_rbuf: [4096]u8 = undefined;
        var d4_sr = d4_stream.reader(io, &d4_rbuf);
        var d4_lb: [2]u8 = undefined;
        d4_sr.interface.readSliceAll(&d4_lb) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };
        const d4_rlen = std.mem.readInt(u16, &d4_lb, .big);
        if (d4_rlen == 0 or d4_rlen > 4096) {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        }
        const d4_rdata = alloc.alloc(u8, d4_rlen) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };
        defer alloc.free(d4_rdata);
        d4_sr.interface.readSliceAll(d4_rdata) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };

        var d4_resp = gw_mod.iso8583.parseWithSchema(d4_rdata, &gw_mod.DEFAULT_SCHEMA, alloc) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false);
            break :d4;
        };
        defer d4_resp.deinit();
        const d4_rc = d4_resp.get(39) orelse "";
        const d4_ok = std.mem.eql(u8, &d4_resp.mti, "0510") and std.mem.eql(u8, d4_rc, "00");
        g_startup_d4.store(d4_ok, .monotonic);
        printStartup("D4  Réconcil. 0500 → 0510", d4_ok);
    }

    // D5 — Heartbeat 0800/NMI=801 → 0810
    {
        const d5_ok = if (sign_gw.sendHeartbeat(&stan, alloc)) blk: {
            break :blk true;
        } else |_| false;
        g_startup_d5.store(d5_ok, .monotonic);
        printStartup("D5  Heartbeat 0800/801 → 0810", d5_ok);
    }

    // ── FakeMoMoServer ────────────────────────────────────────────────────────
    const momo_sock: c_int = blk: {
        const s = csock.socket(C_AF_INET, C_SOCK_STREAM, C_IPPROTO_TCP);
        if (s < 0) return error.MoMoSocket;
        const opt: c_int = 1;
        _ = csock.setsockopt(s, C_SOL_SOCKET, C_SO_REUSEADDR, @ptrCast(&opt), @sizeOf(c_int));
        _ = csock.setsockopt(s, C_SOL_SOCKET, C_SO_REUSEPORT, @ptrCast(&opt), @sizeOf(c_int));
        const maddr = SockaddrIn{
            .sin_port = std.mem.nativeToBig(u16, MOMO_PORT),
            .sin_addr = std.mem.nativeToBig(u32, 0x7F000001),
        };
        if (csock.bind(s, &maddr, @sizeOf(SockaddrIn)) < 0) return error.MoMoBind;
        if (csock.listen(s, 256) < 0) return error.MoMoListen;
        break :blk s;
    };
    for (0..8) |_| (try std.Thread.spawn(.{}, runMoMoWorker, .{momo_sock})).detach();
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);

    // D6 — Cashout Bank→MoMo 0200 → 0210
    d6: {
        var d6_threaded = std.Io.Threaded.init(alloc, .{});
        const d6_io = d6_threaded.io();
        var d6_client = gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = MOMO_PORT }, d6_io);
        var d6_iso = gw_mod.iso8583.IsoMessage.init(alloc, "0200".*);
        defer d6_iso.deinit();
        d6_iso.set(11, "000001") catch {
            printStartup("D6  Cashout Bank→MoMo 0200 → 0210", false);
            break :d6;
        };
        d6_iso.set(49, "XAF") catch {
            printStartup("D6  Cashout Bank→MoMo 0200 → 0210", false);
            break :d6;
        };
        if (d6_client.send(&d6_iso, alloc)) |d6_resp| {
            defer @constCast(&d6_resp).deinit();
            const d6_rc = d6_resp.get(39) orelse "";
            const d6_ok = std.mem.eql(u8, &d6_resp.mti, "0210") and std.mem.eql(u8, d6_rc, "00");
            g_startup_d6.store(d6_ok, .monotonic);
            printStartup("D6  Cashout Bank→MoMo 0200 → 0210", d6_ok);
        } else |_| {
            printStartup("D6  Cashout Bank→MoMo 0200 → 0210", false);
        }
        break :d6;
    }

    std.debug.print("\x1b[1;36m═══════════════════════════════════════════════════════\x1b[0m\n\n", .{});

    // ── Ticker ────────────────────────────────────────────────────────────────
    (try std.Thread.spawn(.{}, ticker, .{})).detach();

    // ── Worker sizing — runtime CPU detection (Axe 1) ─────────────────────────
    // 32 I/O-bound gateway workers per logical CPU gives good parallelism while
    // avoiding excessive mutex contention.  D3 cashout needs far fewer workers
    // because it contacts the MoMo server (low latency, no bank round-trip).
    // Bank pool = D1 + D3 + 14 slack (heartbeat, sign-on, D0-D6 startup probes).
    const cpu_count: usize = std.Thread.getCpuCount() catch 8;
    const n_d1: usize = @min(512, cpu_count * 32);
    const n_d3: usize = @max(1, cpu_count / 2);
    std.log.info("battle: cpu={d}  D1_workers={d}  D3_workers={d}", .{ cpu_count, n_d1, n_d3 });

    // ── Gateway workers (each owns its own BankClient + Io event loop) ───────
    const gw_args = GwArgs{ .mac_engine = &mac_engine, .audit = &audit, .broker_port = BROKER_PORT };
    for (0..n_d1) |_| (try std.Thread.spawn(.{}, gatewayWorker, .{gw_args})).detach();

    // ── Drain consumer (battle.inbound — prevents router deliver queue build-up)
    (try std.Thread.spawn(.{}, drainConsumer, .{})).detach();

    // ── Heartbeat thread ──────────────────────────────────────────────────────
    (try std.Thread.spawn(.{}, heartbeatWorker, .{HeartbeatArgs{ .mac_engine = &mac_engine }})).detach();

    // ── D2 injector thread ────────────────────────────────────────────────────
    (try std.Thread.spawn(.{}, d2InjectorWorker, .{})).detach();

    // ── Cashout workers (Bank→MoMo direction) ────────────────────────────────
    for (0..n_d3) |_| (try std.Thread.spawn(.{}, cashoutWorker, .{})).detach();

    // ── HTTP dashboard (blocks until Ctrl+C) ──────────────────────────────────
    httpServer();

    // Wait briefly for in-flight transactions to complete
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 500_000_000 }, null);
    printReport(io);
    std.c._exit(0);
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
    \\    <div class="kpi">
    \\      <div class="kpi-label">D1 · MoMo→Bank</div>
    \\      <div class="kpi-val" id="k-d1">—</div>
    \\      <div class="kpi-unit">tx / s</div>
    \\    </div>
    \\    <div class="kpi">
    \\      <div class="kpi-label">D3 · Bank→MoMo</div>
    \\      <div class="kpi-val" id="k-d3">—</div>
    \\      <div class="kpi-unit">tx / s</div>
    \\    </div>
    \\    <div class="kpi">
    \\      <div class="kpi-label">Peak tx/s</div>
    \\      <div class="kpi-val" id="k-peak">—</div>
    \\      <div class="kpi-unit">maximum historique</div>
    \\    </div>
    \\    <div class="kpi">
    \\      <div class="kpi-label">Total livré</div>
    \\      <div class="kpi-val" id="k-total">—</div>
    \\      <div class="kpi-unit">transactions</div>
    \\    </div>
    \\    <div class="kpi">
    \\      <div class="kpi-label">Latence P50</div>
    \\      <div class="kpi-val ok" id="k-p50">—</div>
    \\      <div class="kpi-unit">médiane</div>
    \\    </div>
    \\    <div class="kpi">
    \\      <div class="kpi-label">Latence P99</div>
    \\      <div class="kpi-val" id="k-p99">—</div>
    \\      <div class="kpi-unit">99e percentile</div>
    \\    </div>
    \\    <div class="kpi">
    \\      <div class="kpi-label">Mémoire RSS</div>
    \\      <div class="kpi-val" id="k-rss">—</div>
    \\      <div class="kpi-unit">empreinte réelle</div>
    \\    </div>
    \\  </div>
    \\
    \\  <div class="panel">
    \\    <div class="panel-header">
    \\      <div class="panel-title">Throughput</div>
    \\      <div class="panel-val" id="tps-now">—</div>
    \\    </div>
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
    \\    <div class="panel-header">
    \\      <div class="panel-title">Chaos control</div>
    \\    </div>
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
    \\        y:{beginAtZero:true,grid:{color:'#1f1f1f',lineWidth:1},border:{color:'#2a2a2a'},
    \\           ticks:{color:'#444',font:{size:10},callback:v=>ff(v)}},
    \\        x:{grid:{color:'#1f1f1f',lineWidth:1},border:{color:'#2a2a2a'},
    \\           ticks:{color:'#444',font:{size:10},maxTicksLimit:8}}
    \\      },
    \\      plugins:{legend:{display:false},tooltip:{
    \\        backgroundColor:'#1a1a1a',borderColor:'#2a2a2a',borderWidth:1,
    \\        titleColor:'#555',bodyColor:'#d4d4d4',padding:8,
    \\        titleFont:{size:10},bodyFont:{size:11}
    \\      }}
    \\    }});
    \\  }
    \\  tick();setInterval(tick,1000);
    \\});
    \\function tick(){
    \\  fetch('/metrics').then(r=>r.json()).then(m=>{
    \\    sv('ts',new Date().toLocaleTimeString('fr-FR'));
    \\    sv('el',ft(m.elapsed_s||0));
    \\    // D0 — session MAC live
    \\    const macOk=m.mac_active===1;
    \\    dot('sd0',macOk);
    \\    sv('sd0-val',macOk?'actif':'inactif');
    \\    // D1 — flux de transactions live (rouge si silence > 10s)
    \\    const txs=m.tx_since_s??-1;
    \\    dot('sd1',txs>=0&&txs<10);
    \\    sv('sd1-val',txs>=0?flo(m.gw_tps||0)+' tx/s':'—');
    \\    // D2 — ACKs Bank→Broker
    \\    const d2s=m.d2_sent||0,d2r=m.d2_received||0;
    \\    dot('sd2',d2s>0?d2r>=d2s:null);
    \\    sv('sd2-val',d2s>0?d2r+'/'+d2s:'—');
    \\    // D3 — reversals (vert=0 échec, jaune<1%, rouge>=1%)
    \\    const revTotal=m.reversals||0,revOk=m.rev_ok||0,revFail=m.rev_fail||0;
    \\    const revRate=revTotal>0?revFail/revTotal:0;
    \\    dot('sd3',revTotal>0?(revFail===0?true:revRate<0.01?'warn':false):null);
    \\    sv('sd3-val',revTotal>0?flo(revOk)+'/'+flo(revTotal):'—');
    \\    // D4 — réconciliation (validation démarrage)
    \\    dot('sd4',m.startup_d4===1);
    \\    sv('sd4-val',m.startup_d4===1?'OK':'—');
    \\    // Pulse — heartbeat live
    \\    const hbs=m.hb_since_s??-1;
    \\    dot('sd5',hbs>=0&&hbs<90);
    \\    sv('sd5-val',hbs>=0?hbs+'s':'—');
    \\    // Cashout — Bank→MoMo (live)
    \\    const cashOk=m.cashout_ok||0,cashFail=m.cashout_fail||0,cashSent=m.cashout_sent||0;
    \\    dot('sd6',cashSent>0?(cashFail===0):(m.startup_d6===1?true:null));
    \\    sv('sd6-val',cashSent>0?flo(cashOk)+'/'+flo(cashSent):'—');
    \\    // MAC
    \\    dot('smac',macOk);
    \\    sv('smac-val',macOk?'actif':'inactif');
    \\    // Dernier Pulse
    \\    dot('shb',hbs>=0&&hbs<90);
    \\    sv('shb-val',hbs>=0?hbs+'s':'—');
    \\    // WAL
    \\    const wk=m.wal_kb||0;
    \\    sv('i-wal',wk>=1024?(wk/1024).toFixed(1)+' MB':wk+' KB');
    \\    const p99=m.p99_us||0;
    \\    sv('k-d1',flo(m.gw_tps||0));
    \\    sv('k-d3',flo(m.tps_d3||0));
    \\    sv('k-peak',flo(m.peak_tps||0));
    \\    sv('k-total',ff((m.ok||0)+(m.declined||0)));
    \\    sv('k-p50',fms(m.p50_us||0));
    \\    sv('k-p99',fms(p99));
    \\    const p99el=document.getElementById('k-p99');
    \\    if(p99el)p99el.className='kpi-val'+(p99>50000?' fail':p99>5000?' warn':' ok');
    \\    sv('k-rss',(m.rss_mb||0)+' MB');
    \\    sv('c-noresp',flo(m.bank_noresp||0));
    \\    sv('c-rc51',flo(m.bank_rc51||0));
    \\    sv('c-rc91',flo(m.bank_rc91||0));
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
