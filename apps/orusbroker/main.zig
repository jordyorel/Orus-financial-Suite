const std = @import("std");
const orusbroker = @import("orusbroker");

fn getenv(name: [:0]const u8) ?[]const u8 {
    const raw = std.c.getenv(name.ptr) orelse return null;
    return std.mem.span(raw);
}

fn envU16(name: [:0]const u8, default: u16) u16 {
    const s = getenv(name) orelse return default;
    return std.fmt.parseInt(u16, s, 10) catch default;
}

// ── Signal handling ───────────────────────────────────────────────────────────

fn handleSignal(sig: std.posix.SIG) callconv(.c) void {
    _ = sig;
    std.process.exit(0);
}

fn installSignalHandlers() void {
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
    std.posix.sigaction(std.posix.SIG.INT,  &sa, null);
}

// ── Health check ──────────────────────────────────────────────────────────────

const HealthArgs = struct { port: u16, io: std.Io };

fn serveHealth(args: *HealthArgs) void {
    const addr = std.Io.net.IpAddress.parse("0.0.0.0", args.port) catch return;
    var srv = addr.listen(args.io, .{ .reuse_address = true }) catch return;
    defer srv.deinit(args.io);
    const resp =
        "HTTP/1.1 200 OK\r\n" ++
        "Content-Type: text/plain\r\n" ++
        "Content-Length: 2\r\n" ++
        "Connection: close\r\n\r\nOK";
    while (true) {
        const stream = srv.accept(args.io) catch continue;
        var wbuf: [256]u8 = undefined;
        var sw = stream.writer(args.io, &wbuf);
        sw.interface.writeAll(resp) catch {};
        sw.interface.flush() catch {};
        stream.close(args.io);
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    installSignalHandlers();

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    const host         = getenv("BROKER_HOST")        orelse "0.0.0.0";
    const port         = envU16("BROKER_PORT",         7770);
    const health_port  = envU16("BROKER_HEALTH_PORT",  7771);
    const wal_path     = getenv("BROKER_WAL_FILE")    orelse "orus_broker.wal";
    const cursor_dir   = getenv("BROKER_CURSOR_DIR")  orelse "/tmp/orus_cursors";

    var wal = try orusbroker.Wal.open(io, wal_path);
    defer wal.close();

    var dedup  = orusbroker.DedupFilter.init();
    var router = orusbroker.TopicRouter.init(alloc);
    defer router.deinit();

    const config = orusbroker.Config{
        .host       = host,
        .port       = port,
        .wal_path   = wal_path,
        .cursor_dir = cursor_dir,
    };

    const server = orusbroker.BrokerServer{
        .config = config,
        .io     = io,
        .wal    = &wal,
        .dedup  = &dedup,
        .router = &router,
        .cursor = orusbroker.Cursor.init(cursor_dir, io),
    };

    var health_args = HealthArgs{ .port = health_port, .io = io };
    const health_thread = try std.Thread.spawn(.{}, serveHealth, .{&health_args});
    health_thread.detach();

    std.log.info("OrusBroker listening on {s}:{d}  health=:{d}  wal={s}", .{
        host, port, health_port, wal_path,
    });

    try server.serve(alloc);
}
