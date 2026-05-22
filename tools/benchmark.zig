// Benchmark de performance OrusBroker — deux directions simultanées.
//
// Mesure :
//   • Throughput publish (TX/s)
//   • Throughput deliver (TX/s)
//   • Latence broker P50 / P95 / P99 / P99.9 / max
//   • Taux d'erreur
//   • Mémoire RSS
//   • Impact de la charge simultanée D1 + D2
//
// Protocole de mesure :
//   - received_at du message = timestamp nanoseconde à la publication
//   - Latence = now() dans le callback subscriber - received_at
//   - Ce mesure le vrai aller-retour : publish → WAL → router → deliver
//
// Usage : zig build bench
//   Variables d'environnement optionnelles :
//     BENCH_DURATION=10    (secondes, défaut 10)
//     BENCH_PUB_THREADS=10 (threads publiants par direction, défaut 10)
//     BENCH_DECLINE_RATE=5 (% de refus simulés, défaut 5)

const std        = @import("std");
const orusshare  = @import("orusshare");
const orusbroker = @import("orusbroker");
const orusconnect = @import("orusconnect");

const BROKER_PORT: u16 = 7794;
const TOPIC_D1 = "bench.inbound";   // MoMo → Banque
const TOPIC_D2 = "bench.outbound";  // Banque → MoMo

// Limite du tableau de samples — 5 millions = ~40 MB (u64 × 5M).
const MAX_SAMPLES: usize = 5_000_000;

// ── État global partagé entre threads ────────────────────────────────────────

var g_published  = std.atomic.Value(u64).init(0);
var g_delivered  = std.atomic.Value(u64).init(0);
var g_pub_errors = std.atomic.Value(u64).init(0);
var g_sub_errors = std.atomic.Value(u64).init(0);
var g_running    = std.atomic.Value(bool).init(true);

// Pré-alloué statiquement — évite les allocations dans le hot path.
var g_samples: [MAX_SAMPLES]u64 = undefined;
var g_sample_idx = std.atomic.Value(usize).init(0);

// Snapshot pour le calcul du débit par seconde.
var g_prev_published: u64 = 0;
var g_prev_delivered: u64 = 0;

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

// ── Publisher thread ──────────────────────────────────────────────────────────

const PubArgs = struct {
    io:           std.Io,
    topic:        []const u8,
    decline_rate: u8, // 0-100 : % de messages marqués "refus simulé"
};

fn publishLoop(args: PubArgs) void {
    const client = orusconnect.BrokerClient.init(
        .{ .host = "127.0.0.1", .port = BROKER_PORT },
        args.io,
    );

    var ts_init: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts_init);
    var prng_state: u64 = @bitCast(ts_init.sec *% 1_000_000_000 +% ts_init.nsec);

    while (g_running.load(.monotonic)) {
        prng_state ^= prng_state << 13;
        prng_state ^= prng_state >> 7;
        prng_state ^= prng_state << 17;

        const declined = (prng_state % 100) < args.decline_rate;
        const amount: i64 = @intCast(1_000 + (prng_state % 99_000));

        var msg_id: [16]u8 = undefined;
        std.c.arc4random_buf(&msg_id, msg_id.len);

        var ts_now: std.c.timespec = undefined;
        _ = std.c.clock_gettime(.MONOTONIC, &ts_now);
        const now_ns: i64 = ts_now.sec *% std.time.ns_per_s + ts_now.nsec;

        const msg = orusshare.InternalMessage{
            .msg_id      = msg_id,
            .schema_id   = "gimac",
            .topic       = args.topic,
            .origin      = .rest_json,
            .provider    = .mtn_momo,
            .mti         = "0200".*,
            .fields      = &.{},
            .stan        = "000000".*,
            .pan_hash    = prng_state,
            .amount      = if (declined) -amount else amount,
            .currency    = "XAF".*,
            .received_at = now_ns, // horodatage pour la mesure de latence
            .source_ip   = [_]u8{0} ** 16,
            .hop_count   = 1,
            .external_id = null,
        };

        client.publish(&msg) catch {
            _ = g_pub_errors.fetchAdd(1, .monotonic);
            continue;
        };
        _ = g_published.fetchAdd(1, .monotonic);
    }
}

// ── Consumer / latence ────────────────────────────────────────────────────────

fn onDelivered(msg: *const orusshare.InternalMessage, _: ?*anyopaque) void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts);
    const now_ns: i64 = ts.sec *% std.time.ns_per_s + ts.nsec;
    if (msg.received_at > 0 and now_ns > msg.received_at) {
        const lat_ns: u64 = @intCast(now_ns - msg.received_at);
        const idx = g_sample_idx.fetchAdd(1, .monotonic);
        if (idx < MAX_SAMPLES) g_samples[idx] = lat_ns;
    }
    _ = g_delivered.fetchAdd(1, .monotonic);
}

const SubArgs = struct { io: std.Io, topic: []const u8 };

fn consumeLoop(args: SubArgs) void {
    const client = orusconnect.BrokerClient.init(
        .{ .host = "127.0.0.1", .port = BROKER_PORT },
        args.io,
    );
    while (true) {
        client.consume(args.topic, std.heap.page_allocator, onDelivered, null) catch {
            _ = g_sub_errors.fetchAdd(1, .monotonic);
        };
        if (!g_running.load(.monotonic)) break;
        // Petite pause avant reconnexion.
        const ts = std.c.timespec{ .sec = 0, .nsec = 10_000_000 };
        _ = std.c.nanosleep(&ts, null);
    }
}

// ── Percentiles ───────────────────────────────────────────────────────────────

fn percentile(sorted: []u64, p: f64) u64 {
    if (sorted.len == 0) return 0;
    const idx = @min(
        sorted.len - 1,
        @as(usize, @intFromFloat(@ceil(p / 100.0 * @as(f64, @floatFromInt(sorted.len))) - 1)),
    );
    return sorted[idx];
}

// ── RSS mémoire ───────────────────────────────────────────────────────────────

fn rssBytes() usize {
    var ru: std.c.rusage = undefined;
    _ = std.c.getrusage(0, &ru); // RUSAGE_SELF = 0
    // Zig 0.16: champ nommé `maxrss` (sans préfixe ru_).
    // macOS: en octets. Linux: en kilo-octets.
    const raw: usize = @intCast(ru.maxrss);
    return if (@import("builtin").os.tag == .macos) raw else raw * 1024;
}

// ── Affichage ─────────────────────────────────────────────────────────────────

fn fmtNum(buf: []u8, n: u64) []u8 {
    // Format avec séparateurs milliers : 1_234_567
    var tmp: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&tmp, "{d}", .{n}) catch return buf[0..0];
    var out_pos: usize = 0;
    var digits_left = s.len;
    for (s) |c| {
        if (digits_left > 0 and digits_left % 3 == 0 and out_pos > 0) {
            buf[out_pos] = '_';
            out_pos += 1;
        }
        buf[out_pos] = c;
        out_pos += 1;
        digits_left -= 1;
    }
    return buf[0..out_pos];
}

fn bar(filled: usize, total: usize, width: usize) void {
    const n = if (total == 0) 0 else @min(width, filled * width / total);
    std.debug.print("[", .{});
    for (0..n) |_| std.debug.print("█", .{});
    for (n..width) |_| std.debug.print("░", .{});
    std.debug.print("]", .{});
}

// ── main ──────────────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // Paramètres via env
    const duration_s: u64 = blk: {
        const s = std.c.getenv("BENCH_DURATION") orelse break :blk 10;
        break :blk std.fmt.parseInt(u64, std.mem.span(s), 10) catch 10;
    };
    const pub_threads: usize = blk: {
        const s = std.c.getenv("BENCH_PUB_THREADS") orelse break :blk 10;
        break :blk std.fmt.parseInt(usize, std.mem.span(s), 10) catch 10;
    };
    const decline_rate: u8 = blk: {
        const s = std.c.getenv("BENCH_DECLINE_RATE") orelse break :blk 5;
        break :blk @intCast(std.fmt.parseInt(u8, std.mem.span(s), 10) catch 5);
    };

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    // ── Démarrage du broker ───────────────────────────────────────────────────
    const wal_path = "/tmp/bench_broker.wal";
    std.Io.Dir.cwd().deleteFile(io, wal_path) catch {};

    const inner = try std.heap.page_allocator.create(BrokerInner);
    inner.wal    = try orusbroker.Wal.open(io, wal_path);
    inner.dedup  = orusbroker.DedupFilter.init();
    inner.router = orusbroker.TopicRouter.init(std.heap.page_allocator);

    const cfg = orusbroker.Config{
        .host       = "127.0.0.1",
        .port       = BROKER_PORT,
        .wal_path   = wal_path,
        .cursor_dir = "/tmp/bench_cursors",
    };
    inner.server = orusbroker.BrokerServer{
        .config = cfg,
        .io     = io,
        .wal    = &inner.wal,
        .dedup  = &inner.dedup,
        .router = &inner.router,
        .cursor = orusbroker.Cursor.init(cfg.cursor_dir, io),
    };
    (try std.Thread.spawn(.{}, runBroker, .{inner})).detach();

    // Attendre que le broker soit prêt.
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 100_000_000 }, null);

    // ── Subscribers (D1 + D2) ─────────────────────────────────────────────────
    // 2 subscribers par topic pour simuler la charge réelle.
    for (0..2) |_| {
        (try std.Thread.spawn(.{}, consumeLoop, .{SubArgs{ .io = io, .topic = TOPIC_D1 }})).detach();
        (try std.Thread.spawn(.{}, consumeLoop, .{SubArgs{ .io = io, .topic = TOPIC_D2 }})).detach();
    }
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);

    // ── Publishers D1 + D2 simultanés ────────────────────────────────────────
    const d1_args = PubArgs{ .io = io, .topic = TOPIC_D1, .decline_rate = decline_rate };
    const d2_args = PubArgs{ .io = io, .topic = TOPIC_D2, .decline_rate = 0 };

    for (0..pub_threads) |_| {
        (try std.Thread.spawn(.{}, publishLoop, .{d1_args})).detach();
        (try std.Thread.spawn(.{}, publishLoop, .{d2_args})).detach();
    }

    // ── Boucle de mesure ──────────────────────────────────────────────────────
    std.debug.print("\x1b[2J\x1b[H", .{}); // clear screen
    std.debug.print("\x1b[1;36m", .{});
    std.debug.print("ORUS INFRA — Benchmark de performance\n", .{});
    std.debug.print("══════════════════════════════════════════════════════════════\n", .{});
    std.debug.print("\x1b[0m", .{});
    std.debug.print("  Durée: {d}s  │  Threads pub/dir: {d}×2  │  Refus simulés: {d}%\n",
        .{ duration_s, pub_threads, decline_rate });
    std.debug.print("  Topics: {s} + {s}\n\n", .{ TOPIC_D1, TOPIC_D2 });

    var peak_tps: u64 = 0;
    var num_buf1: [32]u8 = undefined;
    var num_buf2: [32]u8 = undefined;
    var num_buf3: [32]u8 = undefined;

    for (0..duration_s) |sec| {
        _ = std.c.nanosleep(&.{ .sec = 1, .nsec = 0 }, null);

        const pub_now = g_published.load(.monotonic);
        const del_now = g_delivered.load(.monotonic);
        const tps_pub = pub_now - g_prev_published;
        const tps_del = del_now - g_prev_delivered;
        g_prev_published = pub_now;
        g_prev_delivered = del_now;
        if (tps_pub > peak_tps) peak_tps = tps_pub;

        const progress = (sec + 1) * 100 / duration_s;

        std.debug.print("\r\x1b[K", .{}); // effacer la ligne
        std.debug.print("  [{d:>2}s/{d}s] ", .{ sec + 1, duration_s });
        bar(sec + 1, duration_s, 20);
        std.debug.print("  Pub: \x1b[33m{s}\x1b[0m tx/s  Del: \x1b[32m{s}\x1b[0m tx/s  Total: {s}",
            .{
                fmtNum(&num_buf1, tps_pub),
                fmtNum(&num_buf2, tps_del),
                fmtNum(&num_buf3, pub_now),
            });
        _ = progress;
    }

    // Stop publishers.
    g_running.store(false, .monotonic);
    _ = std.c.nanosleep(&.{ .sec = 1, .nsec = 0 }, null);

    // ── Calcul des percentiles ────────────────────────────────────────────────
    const n_samples = @min(g_sample_idx.load(.monotonic), MAX_SAMPLES);
    const samples = g_samples[0..n_samples];
    std.mem.sort(u64, samples, {}, std.sort.asc(u64));

    const total_pub  = g_published.load(.monotonic);
    const total_del  = g_delivered.load(.monotonic);
    const pub_errors = g_pub_errors.load(.monotonic);
    const total_ops  = total_pub + pub_errors;
    const error_rate = if (total_ops > 0) pub_errors * 10000 / total_ops else 0;

    const rss_mb = rssBytes() / (1024 * 1024);

    // ── Rapport final ─────────────────────────────────────────────────────────
    std.debug.print("\n\n", .{});
    std.debug.print("\x1b[1;36m══════════════════════════════════════════════════════════════\x1b[0m\n", .{});
    std.debug.print("\x1b[1mRÉSULTATS FINAUX ({d} secondes, {d} threads × 2 directions)\x1b[0m\n",
        .{ duration_s, pub_threads });
    std.debug.print("\x1b[1;36m══════════════════════════════════════════════════════════════\x1b[0m\n\n", .{});

    std.debug.print("  \x1b[1mDébit\x1b[0m\n", .{});
    std.debug.print("    Publié          : \x1b[33m{d:>10}\x1b[0m messages total\n",    .{total_pub});
    std.debug.print("    Livré           : \x1b[32m{d:>10}\x1b[0m messages total\n",    .{total_del});
    std.debug.print("    Débit moyen pub : \x1b[1;33m{d:>10}\x1b[0m tx/s\n",           .{total_pub / duration_s});
    std.debug.print("    Débit moyen liv : \x1b[1;32m{d:>10}\x1b[0m tx/s\n",           .{total_del / duration_s});
    std.debug.print("    Peak TPS        : \x1b[1;35m{d:>10}\x1b[0m tx/s\n",           .{peak_tps});
    std.debug.print("    Taux d'erreur   : \x1b[{s}m        {d}.{d:02}%\x1b[0m\n\n",
        .{ if (error_rate == 0) "32" else "31", error_rate / 100, error_rate % 100 });

    if (n_samples > 0) {
        const p50   = percentile(samples, 50.0);
        const p95   = percentile(samples, 95.0);
        const p99   = percentile(samples, 99.0);
        const p999  = percentile(samples, 99.9);
        const p_max = samples[samples.len - 1];
        const p_min = samples[0];

        std.debug.print("  \x1b[1mLatence broker (publish → deliver)\x1b[0m  [{d} échantillons]\n",
            .{n_samples});
        std.debug.print("    Min    : \x1b[32m{d:>8} µs  ({d:.2} ms)\x1b[0m\n",
            .{ p_min / 1000, @as(f64, @floatFromInt(p_min)) / 1_000_000.0 });
        std.debug.print("    P50    : \x1b[32m{d:>8} µs  ({d:.2} ms)\x1b[0m\n",
            .{ p50 / 1000,   @as(f64, @floatFromInt(p50))   / 1_000_000.0 });
        std.debug.print("    P95    : \x1b[33m{d:>8} µs  ({d:.2} ms)\x1b[0m\n",
            .{ p95 / 1000,   @as(f64, @floatFromInt(p95))   / 1_000_000.0 });
        std.debug.print("    P99    : \x1b[33m{d:>8} µs  ({d:.2} ms)\x1b[0m\n",
            .{ p99 / 1000,   @as(f64, @floatFromInt(p99))   / 1_000_000.0 });
        std.debug.print("    P99.9  : \x1b[31m{d:>8} µs  ({d:.2} ms)\x1b[0m\n",
            .{ p999 / 1000,  @as(f64, @floatFromInt(p999))  / 1_000_000.0 });
        std.debug.print("    Max    : \x1b[31m{d:>8} µs  ({d:.2} ms)\x1b[0m\n\n",
            .{ p_max / 1000, @as(f64, @floatFromInt(p_max)) / 1_000_000.0 });
    } else {
        std.debug.print("  Latence : pas assez d'échantillons\n\n", .{});
    }

    // WAL size — Zig 0.16 utilise std.Io.Dir
    const wal_stat = std.Io.Dir.cwd().statFile(io, wal_path, .{}) catch null;
    const wal_mb: f64 = if (wal_stat) |st|
        @as(f64, @floatFromInt(st.size)) / (1024.0 * 1024.0)
    else 0;

    std.debug.print("  \x1b[1mRessources\x1b[0m\n", .{});
    std.debug.print("    Mémoire RSS     : {d} MB\n",   .{rss_mb});
    std.debug.print("    WAL écrit       : {d:.1} MB\n", .{wal_mb});
    std.debug.print("    Threads pub     : {d} × 2 directions = {d} total\n",
        .{ pub_threads, pub_threads * 2 });
    std.debug.print("    Threads sub     : 2 × 2 directions = 4 total\n\n", .{});

    std.debug.print("\x1b[1;36m══════════════════════════════════════════════════════════════\x1b[0m\n", .{});

    std.c._exit(0);
}
