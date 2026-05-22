// Démonstration en temps réel du broker OrusBroker.
//
// Ce binaire :
//   1. Démarre un BrokerServer in-process sur le port 7799.
//   2. Ouvre un subscriber sur "demo.transactions" dans un thread dédié.
//   3. Publie N=5 messages via BrokerClient.publish() avec un délai entre chaque.
//   4. Attend que le subscriber ait reçu tous les messages, puis quitte.
//
// Lancer avec : zig build demo

const std       = @import("std");
const orusshare = @import("orusshare");
const orusbroker = @import("orusbroker");
const orusconnect = @import("orusconnect");

const DEMO_PORT: u16 = 7799;
const DEMO_TOPIC = "demo.transactions";
const N_MESSAGES: usize = 5;

// ── Broker in-process ─────────────────────────────────────────────────────────

const BrokerInner = struct {
    wal:    orusbroker.Wal,
    dedup:  orusbroker.DedupFilter,
    router: orusbroker.TopicRouter,
    server: orusbroker.BrokerServer,
};

fn runBroker(inner: *BrokerInner) void {
    inner.server.serve(std.heap.page_allocator) catch |err| {
        std.log.err("broker: {}", .{err});
    };
}

// ── Subscriber ────────────────────────────────────────────────────────────────

const SubArgs = struct {
    io:       std.Io,
    received: *std.atomic.Value(usize),
};

fn onMessage(msg: *const orusshare.InternalMessage, raw_ctx: ?*anyopaque) void {
    const args: *SubArgs = @ptrCast(@alignCast(raw_ctx.?));

    const n = args.received.fetchAdd(1, .monotonic) + 1;
    var id_hex = std.fmt.bytesToHex(&msg.msg_id, .lower);
    std.debug.print(
        "  [SUB #{d}] topic={s}  mti={s}  amount={d} {s}  id={s}\n",
        .{
            n,
            msg.topic,
            &msg.mti,
            msg.amount,
            &msg.currency,
            id_hex[0..8],
        },
    );
}

fn runSubscriber(args: *SubArgs) void {
    const client = orusconnect.BrokerClient.init(
        .{ .host = "127.0.0.1", .port = DEMO_PORT },
        args.io,
    );
    client.consume(DEMO_TOPIC, std.heap.page_allocator, onMessage, args) catch {};
}

// ── Publisher ─────────────────────────────────────────────────────────────────

fn buildMsg(i: usize) orusshare.InternalMessage {
    var id: [16]u8 = undefined;
    std.c.arc4random_buf(&id, id.len);

    var stan: [6]u8 = undefined;
    const n = i % 1_000_000;
    stan[0] = '0' + @as(u8, @intCast(n / 100_000));
    stan[1] = '0' + @as(u8, @intCast((n / 10_000) % 10));
    stan[2] = '0' + @as(u8, @intCast((n / 1_000) % 10));
    stan[3] = '0' + @as(u8, @intCast((n / 100) % 10));
    stan[4] = '0' + @as(u8, @intCast((n / 10) % 10));
    stan[5] = '0' + @as(u8, @intCast(n % 10));

    return .{
        .msg_id      = id,
        .schema_id   = "gimac",
        .topic       = DEMO_TOPIC,
        .origin      = .rest_json,
        .provider    = .mtn_momo,
        .mti         = "0200".*,
        .fields      = &.{},
        .stan        = stan,
        .pan_hash    = @as(u64, i) * 0xdeadbeef,
        .amount      = @intCast((i + 1) * 1000),
        .currency    = "XAF".*,
        .received_at = 0,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,
    };
}

// ── main ──────────────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    const wal_path = "/tmp/demo_broker.wal";
    std.Io.Dir.cwd().deleteFile(io, wal_path) catch {};

    // ── Démarrage du broker ───────────────────────────────────────────────────
    const inner = try std.heap.page_allocator.create(BrokerInner);
    inner.wal    = try orusbroker.Wal.open(io, wal_path);
    inner.dedup  = orusbroker.DedupFilter.init();
    inner.router = orusbroker.TopicRouter.init(std.heap.page_allocator);

    const cfg = orusbroker.Config{
        .host       = "127.0.0.1",
        .port       = DEMO_PORT,
        .wal_path   = wal_path,
        .cursor_dir = "/tmp/demo_cursors",
    };
    inner.server = orusbroker.BrokerServer{
        .config = cfg,
        .io     = io,
        .wal    = &inner.wal,
        .dedup  = &inner.dedup,
        .router = &inner.router,
        .cursor = orusbroker.Cursor.init(cfg.cursor_dir, io),
    };

    const broker_thread = try std.Thread.spawn(.{}, runBroker, .{inner});
    broker_thread.detach();

    // Laisser le broker démarrer.
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 80_000_000 }, null);

    // ── Subscriber ────────────────────────────────────────────────────────────
    var received = std.atomic.Value(usize).init(0);
    var sub_args = SubArgs{ .io = io, .received = &received };

    const sub_thread = try std.Thread.spawn(.{}, runSubscriber, .{&sub_args});
    sub_thread.detach();

    // Laisser le subscriber s'abonner avant les publications.
    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 50_000_000 }, null);

    // ── Publication des messages ───────────────────────────────────────────────
    std.debug.print("\nOrusBroker — démonstration en temps réel (port {d})\n", .{DEMO_PORT});
    std.debug.print("──────────────────────────────────────────────────\n", .{});

    const pub_client = orusconnect.BrokerClient.init(
        .{ .host = "127.0.0.1", .port = DEMO_PORT },
        io,
    );

    for (0..N_MESSAGES) |i| {
        const msg = buildMsg(i);
        pub_client.publish(&msg) catch |err| {
            std.debug.print("  [PUB #{d}] ERREUR: {}\n", .{ i + 1, err });
            continue;
        };
        std.debug.print("  [PUB #{d}] amount={d} XAF  stan={s}\n", .{
            i + 1, msg.amount, &msg.stan,
        });
        _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 200_000_000 }, null);
    }

    // Attendre que tous les messages soient reçus (timeout 3 s).
    var waited_ns: u64 = 0;
    while (received.load(.monotonic) < N_MESSAGES and waited_ns < 3_000_000_000) {
        _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 10_000_000 }, null);
        waited_ns += 10_000_000;
    }

    const total = received.load(.monotonic);
    std.debug.print("──────────────────────────────────────────────────\n", .{});
    if (total == N_MESSAGES) {
        std.debug.print("✓ {d}/{d} messages publiés et reçus avec succès.\n\n", .{ total, N_MESSAGES });
    } else {
        std.debug.print("✗ Seulement {d}/{d} messages reçus.\n\n", .{ total, N_MESSAGES });
        std.process.exit(1);
    }
}
