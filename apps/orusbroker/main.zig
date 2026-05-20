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

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    const host     = getenv("BROKER_HOST")     orelse "0.0.0.0";
    const port     = envU16("BROKER_PORT",     7770);
    const wal_path = getenv("BROKER_WAL_FILE") orelse "orus_broker.wal";

    var wal = try orusbroker.Wal.open(io, wal_path);
    defer wal.close();

    var dedup  = orusbroker.DedupFilter.init();
    var router = orusbroker.TopicRouter.init(alloc);
    defer router.deinit();

    const server = orusbroker.BrokerServer{
        .config = .{ .host = host, .port = port, .wal_path = wal_path },
        .io     = io,
        .wal    = &wal,
        .dedup  = &dedup,
        .router = &router,
    };

    try server.serve(alloc);
}
