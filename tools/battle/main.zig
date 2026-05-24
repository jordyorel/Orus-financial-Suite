// Battle test — entry point.
// Launch : zig build battle
// Open   : http://localhost:7782
//
// Integrity invariant:
//   sent == ok + declined + err_conn + err_mac
//   audit_log_lines == ok + declined

const std        = @import("std");
const orusshare  = @import("orusshare");
const orusbroker = @import("orusbroker");
const gw_mod     = @import("orusgateway");

const s         = @import("state.zig");
const fake_bank = @import("fake_bank.zig");
const fake_momo = @import("fake_momo.zig");
const wk        = @import("workers.zig");
const tck       = @import("ticker.zig");
const dash      = @import("dashboard.zig");

fn sigHandler(sig: std.c.SIG) callconv(.c) void {
    _ = sig;
    s.g_stopped.store(true, .monotonic);
}

fn runBroker(inner: *s.BrokerInner) void {
    inner.server.serve(std.heap.c_allocator) catch {};
}

fn printStartup(name: []const u8, success: bool) void {
    if (success) {
        std.debug.print("  \x1b[32m✓\x1b[0m {s}\n", .{name});
    } else {
        std.debug.print("  \x1b[31m✗\x1b[0m {s}\n", .{name});
    }
}

fn printReport(_: std.Io) void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const now_ns: i64 = ts.sec *% std.time.ns_per_s + ts.nsec;
    const elapsed: u64 = @intCast(@max(1, @divTrunc(now_ns - s.g_start_ns, std.time.ns_per_s)));

    const sent      = s.g_sent.load(.monotonic);
    const ok        = s.g_ok.load(.monotonic);
    const declined  = s.g_declined.load(.monotonic);
    const err_conn  = s.g_err_conn.load(.monotonic);
    const err_mac   = s.g_err_mac.load(.monotonic);
    const responded = ok + declined;
    const audit_lines = s.g_audit_lines.load(.monotonic);
    const peak_tps  = s.g_peak_tps.load(.monotonic);
    const pct       = tck.computePercentiles();
    const success_pct: u64 = if (sent > 0) responded * 100 / sent else 0;
    const err_pct:     u64 = if (sent > 0) (err_conn + err_mac) * 100 / sent else 0;
    const avg_tps:     u64 = responded / elapsed;
    const chaos_lat  = s.g_latency_ms.load(.monotonic);
    const chaos_nrsp = s.g_noresp_pct.load(.monotonic);
    const chaos_r51  = s.g_rc51_pct.load(.monotonic);
    const chaos_r91  = s.g_rc91_pct.load(.monotonic);
    const pass = (err_mac == 0) and (audit_lines == responded) and
        (err_conn * 100 / @max(sent, 1) < 5);

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

    std.debug.print("  \x1b[1mLatence (last {d} samples)\x1b[0m\n", .{@min(s.g_lat_idx.load(.monotonic), s.LAT_CAP)});
    std.debug.print("    P50              : {d} µs\n", .{pct.p50});
    std.debug.print("    P99              : {d} µs\n\n", .{pct.p99});

    std.debug.print("  \x1b[1mSécurité MAC (GIMAC/BEAC)\x1b[0m\n", .{});
    std.debug.print("    Statut           : {s}\n", .{if (s.g_mac_active.load(.monotonic)) "\x1b[32mACTIF (HMAC-SHA256)\x1b[0m" else "\x1b[33mINACTIF\x1b[0m"});
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
    std.debug.print("    D0 Sign-on  0800/301 : {s}\n", .{if (s.g_startup_d0.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D1 MoMo→Bank 0200    : {s}\n", .{if (s.g_startup_d1.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D2 Bank→Broker       : {s}\n", .{if (s.g_startup_d2.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D3 Reversal 0420     : {s}\n", .{if (s.g_startup_d3.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D4 Réconcil. 0500    : {s}\n", .{if (s.g_startup_d4.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D5 Heartbeat 0800    : {s}\n", .{if (s.g_startup_d5.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});
    std.debug.print("    D6 Cashout Bank→MoMo : {s}\n\n", .{if (s.g_startup_d6.load(.monotonic)) "\x1b[32m✓ OK\x1b[0m" else "\x1b[31m✗ ÉCHEC\x1b[0m"});

    std.debug.print("  \x1b[1mHeartbeat (30 s)\x1b[0m\n", .{});
    std.debug.print("    OK               : \x1b[32m{d}\x1b[0m\n", .{s.g_hb_ok.load(.monotonic)});
    std.debug.print("    Échecs           : \x1b[{s}m{d}\x1b[0m\n\n", .{ if (s.g_hb_fail.load(.monotonic) == 0) "32" else "33", s.g_hb_fail.load(.monotonic) });

    const rev_total = s.g_reversals.load(.monotonic);
    std.debug.print("  \x1b[1mReversals injectés (5%% des RC=00)\x1b[0m\n", .{});
    std.debug.print("    Injectés         : {d}\n", .{rev_total});
    std.debug.print("    Confirmés 0430   : \x1b[32m{d}\x1b[0m\n", .{s.g_rev_ok.load(.monotonic)});
    std.debug.print("    Échecs           : \x1b[{s}m{d}\x1b[0m\n\n", .{ if (s.g_rev_fail.load(.monotonic) == 0) "32" else "33", s.g_rev_fail.load(.monotonic) });

    std.debug.print("  \x1b[1mDirection 2 — BankServer (port {d})\x1b[0m\n", .{s.BANKSERVER_PORT});
    std.debug.print("    Injections 0200  : {d}\n", .{s.g_d2_sent.load(.monotonic)});
    std.debug.print("    ACK reçus 0210   : \x1b[32m{d}\x1b[0m\n\n", .{s.g_d2_received.load(.monotonic)});

    std.debug.print("  \x1b[1mDirection 3 — Cashout Bank→MoMo (port {d})\x1b[0m\n", .{s.MOMO_PORT});
    std.debug.print("    Envoyés          : {d}\n", .{s.g_cashout_sent.load(.monotonic)});
    std.debug.print("    OK               : \x1b[32m{d}\x1b[0m\n", .{s.g_cashout_ok.load(.monotonic)});
    std.debug.print("    Échecs           : \x1b[{s}m{d}\x1b[0m\n\n", .{ if (s.g_cashout_fail.load(.monotonic) == 0) "32" else "33", s.g_cashout_fail.load(.monotonic) });

    std.debug.print("  \x1b[1mChaos bank (config à l'arrêt)\x1b[0m\n", .{});
    std.debug.print("    Latence injectée : {d} ms\n", .{chaos_lat});
    std.debug.print("    Sans réponse     : {d}%\n", .{chaos_nrsp});
    std.debug.print("    RC=51            : {d}%\n", .{chaos_r51});
    std.debug.print("    RC=91            : {d}%\n", .{chaos_r91});
    std.debug.print("    Traitées bank    : rc00={d}  rc51={d}  rc91={d}  noresp={d}\n\n", .{ s.g_bank_rc00.load(.monotonic), s.g_bank_rc51.load(.monotonic), s.g_bank_rc91.load(.monotonic), s.g_bank_noresp.load(.monotonic) });

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

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    const sa = std.posix.Sigaction{
        .handler = .{ .handler = sigHandler },
        .mask    = std.posix.sigemptyset(),
        .flags   = 0,
    };
    std.posix.sigaction(std.posix.SIG.INT,  &sa, null);
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);

    std.Io.Dir.cwd().deleteFile(io, s.WAL_PATH)    catch {};
    std.Io.Dir.cwd().deleteFile(io, s.AUDIT_PATH)  catch {};

    var ts0: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts0);
    s.g_start_ns = ts0.sec *% std.time.ns_per_s + ts0.nsec;

    // ── Broker ────────────────────────────────────────────────────────────────
    const inner = try std.heap.c_allocator.create(s.BrokerInner);
    inner.wal = try orusbroker.Wal.open(io, s.WAL_PATH);
    inner.wal.sync_every = 0;
    inner.dedup  = orusbroker.DedupFilter.init();
    inner.router = orusbroker.TopicRouter.init(std.heap.c_allocator);
    inner.server = orusbroker.BrokerServer{
        .config = .{ .host = "127.0.0.1", .port = s.BROKER_PORT, .wal_path = s.WAL_PATH, .cursor_dir = "/tmp/battle_cursors" },
        .io     = io,
        .wal    = &inner.wal,
        .dedup  = &inner.dedup,
        .router = &inner.router,
        .cursor = orusbroker.Cursor.init("/tmp/battle_cursors", io),
    };
    (try std.Thread.spawn(.{}, runBroker, .{inner})).detach();
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 80_000_000 }, null);

    // ── FakeChaosBank ────────────────────────────────────────────────────────
    s.chaos_mac.setKey(s.SESSION_KEY);
    const bank_sock: c_int = blk: {
        const sock = s.csock.socket(s.C_AF_INET, s.C_SOCK_STREAM, s.C_IPPROTO_TCP);
        if (sock < 0) return error.ChaosBankSocket;
        const opt: c_int = 1;
        _ = s.csock.setsockopt(sock, s.C_SOL_SOCKET, s.C_SO_REUSEADDR, @ptrCast(&opt), @sizeOf(c_int));
        _ = s.csock.setsockopt(sock, s.C_SOL_SOCKET, s.C_SO_REUSEPORT, @ptrCast(&opt), @sizeOf(c_int));
        const baddr = s.SockaddrIn{
            .sin_port = std.mem.nativeToBig(u16, s.CHAOS_BANK_PORT),
            .sin_addr = std.mem.nativeToBig(u32, 0x7F000001),
        };
        if (s.csock.bind(sock, &baddr, @sizeOf(s.SockaddrIn)) < 0) return error.ChaosBankBind;
        if (s.csock.listen(sock, 1024) < 0) return error.ChaosBankListen;
        break :blk sock;
    };
    for (0..s.N_CHAOSBANK_WORKERS) |_|
        (try std.Thread.spawn(.{}, fake_bank.runChaosBankWorker, .{bank_sock})).detach();
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);

    // ── MacEngine + AuditLog ──────────────────────────────────────────────────
    var mac_engine = gw_mod.MacEngine.init();
    var audit = try orusshare.AuditLog.open(io, s.AUDIT_PATH);
    audit.sync_every = 0;
    defer audit.close();

    // ── ReconciliationState ───────────────────────────────────────────────────
    var recon_state = gw_mod.ReconciliationState.init();

    // ── BankServer D2 ─────────────────────────────────────────────────────────
    (try std.Thread.spawn(.{}, wk.runBankServerD2, .{wk.BankServerD2Args{ .recon_state = &recon_state }})).detach();
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 150_000_000 }, null);

    // ── Sign-on (retry up to 5× for startup race) ─────────────────────────────
    var sign_gw = gw_mod.Gateway.init(gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = s.CHAOS_BANK_PORT }, io), 0);
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
        s.g_mac_active.store(true, .monotonic);
        std.log.info("Sign-on OK — MAC active (HMAC-SHA256)", .{});
    }

    // ── Startup validation D0–D6 ──────────────────────────────────────────────
    std.debug.print("\n\x1b[1;36m══ VALIDATION PIPELINE D0–D5 ══════════════════════════\x1b[0m\n", .{});

    s.g_startup_d0.store(mac_engine.active, .monotonic);
    printStartup("D0  Sign-on 0800/301 → 0810", mac_engine.active);

    d1: {
        var d1_id: [16]u8 = undefined;
        std.c.arc4random_buf(&d1_id, 16);
        const d1_req = orusshare.InternalMessage{
            .msg_id = d1_id, .schema_id = "gimac", .topic = s.TOPIC,
            .origin = .rest_json, .provider = .mtn_momo, .mti = "0200".*,
            .fields = &.{}, .stan = "000001".*, .pan_hash = 0, .amount = 1000,
            .currency = "XAF".*, .received_at = 0, .source_ip = [_]u8{0} ** 16,
            .hop_count = 1, .external_id = null,
        };
        if (sign_gw.process(&d1_req, alloc)) |d1_resp| {
            defer { for (d1_resp.fields) |f| alloc.free(f.value); alloc.free(d1_resp.fields); }
            s.g_startup_d1.store(true, .monotonic);
            printStartup("D1  MoMo→Bank 0200 → 0210", true);
        } else |_| { printStartup("D1  MoMo→Bank 0200 → 0210", false); }
        break :d1;
    }

    {
        const d2_ok = wk.sendD2BankServer(io, alloc);
        s.g_startup_d2.store(d2_ok, .monotonic);
        printStartup("D2  0200→BankServer→Broker", d2_ok);
    }

    d3: {
        var d3_id: [16]u8 = undefined;
        std.c.arc4random_buf(&d3_id, 16);
        const d3_orig = orusshare.InternalMessage{
            .msg_id = d3_id, .schema_id = "gimac", .topic = s.TOPIC,
            .origin = .rest_json, .provider = .mtn_momo, .mti = "0200".*,
            .fields = &.{}, .stan = "001235".*, .pan_hash = 0, .amount = 5000,
            .currency = "XAF".*, .received_at = 0, .source_ip = [_]u8{0} ** 16,
            .hop_count = 1, .external_id = null,
        };
        if (sign_gw.processReversal(&d3_orig, alloc)) |rev| {
            defer { for (rev.fields) |f| alloc.free(f.value); alloc.free(rev.fields); }
            var rev_rc: []const u8 = "";
            for (rev.fields) |f| if (f.id == 39) { rev_rc = f.value; };
            const d3_ok = std.mem.eql(u8, &rev.mti, "0430") and std.mem.eql(u8, rev_rc, "00");
            s.g_startup_d3.store(d3_ok, .monotonic);
            printStartup("D3  Reversal 0420 → 0430", d3_ok);
        } else |_| { printStartup("D3  Reversal 0420 → 0430", false); }
        break :d3;
    }

    d4: {
        const d4_addr = std.Io.net.IpAddress.parse("127.0.0.1", s.BANKSERVER_PORT) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false); break :d4;
        };
        var d4_stream = d4_addr.connect(io, .{ .mode = .stream }) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false); break :d4;
        };
        defer d4_stream.close(io);
        var d4_iso = gw_mod.iso8583.IsoMessage.init(alloc, "0500".*);
        defer d4_iso.deinit();
        d4_iso.set(11, "000099") catch { printStartup("D4  Réconcil. 0500 → 0510", false); break :d4; };
        const d4_bytes = gw_mod.iso8583.serializeWithSchema(&d4_iso, &gw_mod.DEFAULT_SCHEMA, alloc) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false); break :d4;
        };
        defer alloc.free(d4_bytes);
        var d4_wbuf: [4096]u8 = undefined;
        var d4_sw = d4_stream.writer(io, &d4_wbuf);
        d4_sw.interface.writeInt(u16, @intCast(d4_bytes.len), .big) catch { printStartup("D4  Réconcil. 0500 → 0510", false); break :d4; };
        d4_sw.interface.writeAll(d4_bytes) catch { printStartup("D4  Réconcil. 0500 → 0510", false); break :d4; };
        d4_sw.interface.flush() catch { printStartup("D4  Réconcil. 0500 → 0510", false); break :d4; };
        var d4_rbuf: [4096]u8 = undefined;
        var d4_sr = d4_stream.reader(io, &d4_rbuf);
        var d4_lb: [2]u8 = undefined;
        d4_sr.interface.readSliceAll(&d4_lb) catch { printStartup("D4  Réconcil. 0500 → 0510", false); break :d4; };
        const d4_rlen = std.mem.readInt(u16, &d4_lb, .big);
        if (d4_rlen == 0 or d4_rlen > 4096) { printStartup("D4  Réconcil. 0500 → 0510", false); break :d4; }
        const d4_rdata = alloc.alloc(u8, d4_rlen) catch { printStartup("D4  Réconcil. 0500 → 0510", false); break :d4; };
        defer alloc.free(d4_rdata);
        d4_sr.interface.readSliceAll(d4_rdata) catch { printStartup("D4  Réconcil. 0500 → 0510", false); break :d4; };
        var d4_resp = gw_mod.iso8583.parseWithSchema(d4_rdata, &gw_mod.DEFAULT_SCHEMA, alloc) catch {
            printStartup("D4  Réconcil. 0500 → 0510", false); break :d4;
        };
        defer d4_resp.deinit();
        const d4_rc = d4_resp.get(39) orelse "";
        const d4_ok = std.mem.eql(u8, &d4_resp.mti, "0510") and std.mem.eql(u8, d4_rc, "00");
        s.g_startup_d4.store(d4_ok, .monotonic);
        printStartup("D4  Réconcil. 0500 → 0510", d4_ok);
    }

    {
        const d5_ok = if (sign_gw.sendHeartbeat(&stan, alloc)) true else |_| false;
        s.g_startup_d5.store(d5_ok, .monotonic);
        printStartup("D5  Heartbeat 0800/801 → 0810", d5_ok);
    }

    // ── FakeMoMoServer ────────────────────────────────────────────────────────
    const momo_sock: c_int = blk: {
        const sock = s.csock.socket(s.C_AF_INET, s.C_SOCK_STREAM, s.C_IPPROTO_TCP);
        if (sock < 0) return error.MoMoSocket;
        const opt: c_int = 1;
        _ = s.csock.setsockopt(sock, s.C_SOL_SOCKET, s.C_SO_REUSEADDR, @ptrCast(&opt), @sizeOf(c_int));
        _ = s.csock.setsockopt(sock, s.C_SOL_SOCKET, s.C_SO_REUSEPORT, @ptrCast(&opt), @sizeOf(c_int));
        const maddr = s.SockaddrIn{
            .sin_port = std.mem.nativeToBig(u16, s.MOMO_PORT),
            .sin_addr = std.mem.nativeToBig(u32, 0x7F000001),
        };
        if (s.csock.bind(sock, &maddr, @sizeOf(s.SockaddrIn)) < 0) return error.MoMoBind;
        if (s.csock.listen(sock, 256) < 0) return error.MoMoListen;
        break :blk sock;
    };
    for (0..s.N_MOMO_WORKERS) |_|
        (try std.Thread.spawn(.{}, fake_momo.runMoMoWorker, .{momo_sock})).detach();
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);

    d6: {
        var d6_threaded = std.Io.Threaded.init(alloc, .{});
        const d6_io = d6_threaded.io();
        var d6_client = gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = s.MOMO_PORT }, d6_io);
        var d6_iso = gw_mod.iso8583.IsoMessage.init(alloc, "0200".*);
        defer d6_iso.deinit();
        d6_iso.set(11, "000001") catch { printStartup("D6  Cashout Bank→MoMo 0200 → 0210", false); break :d6; };
        d6_iso.set(49, "XAF")    catch { printStartup("D6  Cashout Bank→MoMo 0200 → 0210", false); break :d6; };
        if (d6_client.send(&d6_iso, alloc)) |d6_resp| {
            defer @constCast(&d6_resp).deinit();
            const d6_rc = d6_resp.get(39) orelse "";
            const d6_ok = std.mem.eql(u8, &d6_resp.mti, "0210") and std.mem.eql(u8, d6_rc, "00");
            s.g_startup_d6.store(d6_ok, .monotonic);
            printStartup("D6  Cashout Bank→MoMo 0200 → 0210", d6_ok);
        } else |_| { printStartup("D6  Cashout Bank→MoMo 0200 → 0210", false); }
        break :d6;
    }

    std.debug.print("\x1b[1;36m═══════════════════════════════════════════════════════\x1b[0m\n\n", .{});

    // ── Workers ───────────────────────────────────────────────────────────────
    (try std.Thread.spawn(.{}, tck.ticker, .{})).detach();

    const gw_args = wk.GwArgs{ .mac_engine = &mac_engine, .audit = &audit, .broker_port = s.BROKER_PORT };
    for (0..s.N_GATEWAY_WORKERS) |_|
        (try std.Thread.spawn(.{}, wk.gatewayWorker, .{gw_args})).detach();

    (try std.Thread.spawn(.{}, wk.drainConsumer,   .{})).detach();
    (try std.Thread.spawn(.{}, wk.heartbeatWorker, .{wk.HeartbeatArgs{ .mac_engine = &mac_engine }})).detach();
    (try std.Thread.spawn(.{}, wk.d2InjectorWorker,.{})).detach();

    for (0..s.N_CASHOUT_WORKERS) |_|
        (try std.Thread.spawn(.{}, fake_momo.cashoutWorker, .{})).detach();

    dash.httpServer();

    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 500_000_000 }, null);
    printReport(io);
    std.c._exit(0);
}
