// Battle workers — gateway (D1), heartbeat, drain consumer, D2 injector, BankServerD2.

const std      = @import("std");
const orusshare= @import("orusshare");
const gw_mod   = @import("orusgateway");
const s        = @import("state.zig");

// ── BankServer D2 (Direction 2 : Banque → Broker) ────────────────────────────

pub const BankServerD2Args = struct { recon_state: *gw_mod.ReconciliationState };

pub fn runBankServerD2(args: BankServerD2Args) void {
    var srv_threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const srv_io = srv_threaded.io();
    const d2_broker = gw_mod.GatewayBrokerClient.init(.{ .host = "127.0.0.1", .port = s.BROKER_PORT }, srv_io);
    const bank_srv = std.heap.c_allocator.create(gw_mod.BankServer) catch return;
    bank_srv.* = gw_mod.BankServer.init(.{
        .host           = "127.0.0.1",
        .port           = s.BANKSERVER_PORT,
        .length_prefix  = .two_byte_be,
        .topic          = s.TOPIC_OUTBOUND,
    }, srv_io, &gw_mod.DEFAULT_SCHEMA, d2_broker);
    bank_srv.reconciliation = args.recon_state;
    bank_srv.serve(std.heap.c_allocator) catch {};
}

pub fn sendD2BankServer(d2_io: std.Io, alloc: std.mem.Allocator) bool {
    const addr = std.Io.net.IpAddress.parse("127.0.0.1", s.BANKSERVER_PORT) catch return false;
    var stream = addr.connect(d2_io, .{ .mode = .stream }) catch return false;
    defer stream.close(d2_io);

    var iso = gw_mod.iso8583.IsoMessage.init(alloc, "0200".*);
    defer iso.deinit();
    iso.set(11, "999001") catch return false;
    iso.set(49, "952")    catch return false;

    const bytes = gw_mod.iso8583.serializeWithSchema(&iso, &gw_mod.DEFAULT_SCHEMA, alloc) catch return false;
    defer alloc.free(bytes);

    var wbuf: [4096]u8 = undefined;
    var sw = stream.writer(d2_io, &wbuf);
    sw.interface.writeInt(u16, @intCast(bytes.len), .big) catch return false;
    sw.interface.writeAll(bytes)                          catch return false;
    sw.interface.flush()                                  catch return false;

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

pub fn d2InjectorWorker() void {
    var inj_threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const inj_io = inj_threaded.io();
    _ = std.c.nanosleep(&.{ .sec = 1, .nsec = 0 }, null);
    while (!s.g_stopped.load(.monotonic)) {
        _ = std.c.nanosleep(&.{ .sec = 5, .nsec = 0 }, null);
        if (s.g_stopped.load(.monotonic)) break;
        _ = s.g_d2_sent.fetchAdd(1, .monotonic);
        if (sendD2BankServer(inj_io, std.heap.c_allocator))
            _ = s.g_d2_received.fetchAdd(1, .monotonic);
        _ = s.g_d2_total.fetchAdd(1, .monotonic);
    }
}

// ── Drain consumer — prevents router deliver queue from accumulating ──────────
//
// Anonymous subscription (no sub_id) → no WAL replay on connect.
// The router delivers under its mutex, so this consumer must drain fast.
// Loopback ACK roundtrip <20 µs; 20 000 ACK/s well within budget.

fn drainCb(_: *const orusshare.InternalMessage, _: ?*anyopaque) void {}

pub fn drainConsumer() void {
    const alloc = std.heap.c_allocator;
    var dr_threaded = std.Io.Threaded.init(alloc, .{});
    const dr_io = dr_threaded.io();
    var client = gw_mod.GatewayBrokerClient.init(.{ .host = "127.0.0.1", .port = s.BROKER_PORT }, dr_io);
    while (!s.g_stopped.load(.monotonic)) {
        client.consume(s.TOPIC, alloc, drainCb, null) catch {};
        if (!s.g_stopped.load(.monotonic))
            _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 10_000_000 }, null);
    }
}

// ── Heartbeat thread (0800/NMI=801 every 30 s) ───────────────────────────────

pub const HeartbeatArgs = struct { mac_engine: *gw_mod.MacEngine };

pub fn heartbeatWorker(args: HeartbeatArgs) void {
    var hb_threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const hb_io = hb_threaded.io();
    var hb_gw = gw_mod.Gateway.init(
        gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = s.CHAOS_BANK_PORT }, hb_io), 0);
    hb_gw.mac_engine = args.mac_engine;
    var hb_stan: u32 = 9000;
    var hb_buf: [65536]u8 = undefined;
    var hb_fba = std.heap.FixedBufferAllocator.init(&hb_buf);

    while (!s.g_stopped.load(.monotonic)) {
        _ = std.c.nanosleep(&.{ .sec = 30, .nsec = 0 }, null);
        if (s.g_stopped.load(.monotonic)) break;
        hb_fba.reset();
        if (hb_gw.sendHeartbeat(&hb_stan, hb_fba.allocator())) {
            _ = s.g_hb_ok.fetchAdd(1, .monotonic);
            var ts: std.c.timespec = undefined;
            _ = std.c.clock_gettime(.MONOTONIC, &ts);
            s.g_hb_last_ns.store(ts.sec *% std.time.ns_per_s + ts.nsec, .monotonic);
        } else |_| {
            _ = s.g_hb_fail.fetchAdd(1, .monotonic);
        }
    }
}

// ── Gateway worker (Direction 1 : MoMo→Bank) ─────────────────────────────────

pub const GwArgs = struct {
    mac_engine:  *gw_mod.MacEngine,
    audit:       *orusshare.AuditLog,
    broker_port: u16,
};

pub fn gatewayWorker(args: GwArgs) void {
    var tx_buf: [131072]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&tx_buf);

    var worker_threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const worker_io = worker_threaded.io();
    var gw = gw_mod.Gateway.init(
        gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = s.CHAOS_BANK_PORT }, worker_io), 0);
    gw.mac_engine = args.mac_engine;
    var broker = gw_mod.GatewayBrokerClient.init(.{ .host = "127.0.0.1", .port = args.broker_port }, worker_io);
    defer broker.deinit();
    var prng: u64 = undefined;
    std.c.arc4random_buf(@ptrCast(&prng), 8);
    var ts_pub: std.c.timespec = undefined;

    while (!s.g_stopped.load(.monotonic)) {
        fba.reset();
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
            .msg_id     = id,
            .schema_id  = "gimac",
            .topic      = s.TOPIC,
            .origin     = .rest_json,
            .provider   = .mtn_momo,
            .mti        = "0200".*,
            .fields     = &.{},
            .stan       = stan_buf,
            .pan_hash   = prng,
            .amount     = @intCast(1_000 + prng % 99_000),
            .currency   = "XAF".*,
            .received_at= 0,
            .source_ip  = [_]u8{0} ** 16,
            .hop_count  = 1,
            .external_id= null,
        };

        _ = s.g_sent.fetchAdd(1, .monotonic);

        const resp = gw.process(&req, alloc) catch |err| {
            switch (err) {
                error.MacMismatch => _ = s.g_err_mac.fetchAdd(1, .monotonic),
                else              => _ = s.g_err_conn.fetchAdd(1, .monotonic),
            }
            continue;
        };
        defer {
            for (resp.fields) |f| alloc.free(f.value);
            alloc.free(resp.fields);
        }

        _ = std.c.clock_gettime(.MONOTONIC, &ts_pub);
        const t1: i64 = ts_pub.sec *% std.time.ns_per_s + ts_pub.nsec;
        if (t1 > t0) {
            const lat: u64 = @intCast(t1 - t0);
            const idx = s.g_lat_idx.fetchAdd(1, .monotonic) % s.LAT_CAP;
            s.g_lat[idx] = lat;
        }

        var rc: []const u8 = "";
        for (resp.fields) |f| if (f.id == 39) { rc = f.value; };
        const is_ok = std.mem.eql(u8, rc, "00");

        s.g_last_tx_ns.store(t1, .monotonic);

        args.audit.record(&resp.mti, &resp.stan, resp.amount, &resp.currency, rc, @tagName(resp.provider), resp.msg_id);
        _ = s.g_d1_total.fetchAdd(1, .monotonic);
        _ = s.g_audit_lines.fetchAdd(1, .monotonic);

        if (is_ok) {
            _ = s.g_ok.fetchAdd(1, .monotonic);
            prng ^= prng << 13;
            prng ^= prng >> 7;
            prng ^= prng << 17;
            if (prng % 100 < 5) {
                _ = s.g_reversals.fetchAdd(1, .monotonic);
                if (gw.processReversal(&req, alloc)) |rev| {
                    defer {
                        for (rev.fields) |f2| alloc.free(f2.value);
                        alloc.free(rev.fields);
                    }
                    var rev_rc: []const u8 = "";
                    for (rev.fields) |f2| if (f2.id == 39) { rev_rc = f2.value; };
                    if (std.mem.eql(u8, rev_rc, "00")) _ = s.g_rev_ok.fetchAdd(1, .monotonic)
                    else _ = s.g_rev_fail.fetchAdd(1, .monotonic);
                } else |_| {
                    _ = s.g_rev_fail.fetchAdd(1, .monotonic);
                }
            }
        } else {
            _ = s.g_declined.fetchAdd(1, .monotonic);
        }

        broker.publish(&resp, alloc) catch {};
    }
}
