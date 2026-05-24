// FakeMoMoServer — Direction 3 (Bank→MoMo cashout).
// Always responds 0210/RC=00. No MAC, no chaos. Keep-alive per connection.

const std    = @import("std");
const gw_mod = @import("orusgateway");
const s      = @import("state.zig");

fn readAllFd(fd: c_int, buf: []u8) bool {
    var off: usize = 0;
    while (off < buf.len) {
        const n = s.csock.read(fd, buf[off..].ptr, buf.len - off);
        if (n <= 0) return false;
        off += @intCast(n);
    }
    return true;
}

fn writeAllFd(fd: c_int, buf: []const u8) bool {
    var off: usize = 0;
    while (off < buf.len) {
        const n = s.csock.write(fd, buf[off..].ptr, buf.len - off);
        if (n <= 0) return false;
        off += @intCast(n);
    }
    return true;
}

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

pub fn runMoMoWorker(momo_sock: c_int) void {
    while (true) {
        const client = s.csock.accept(momo_sock, null, null);
        if (client < 0) {
            if (s.g_stopped.load(.monotonic)) return;
            continue;
        }
        const tv = s.CTimeval{ .tv_sec = 10 };
        _ = s.csock.setsockopt(client, s.C_SOL_SOCKET, s.C_SO_RCVTIMEO, @ptrCast(&tv), @sizeOf(s.CTimeval));
        handleMoMoFd(client);
        _ = s.csock.close(client);
    }
}

pub fn cashoutWorker() void {
    var threaded = std.Io.Threaded.init(std.heap.c_allocator, .{});
    const io = threaded.io();
    var client = gw_mod.BankClient.init(.{ .host = "127.0.0.1", .port = s.MOMO_PORT }, io);
    const alloc = std.heap.c_allocator;
    var ts_co: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts_co);
    var prng: u64 = @as(u64, @intCast(ts_co.sec)) *% 1_000_000_000 + @as(u64, @intCast(ts_co.nsec));

    var ts_slot: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.MONOTONIC, &ts_slot);

    while (!s.g_stopped.load(.monotonic)) {
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

        _ = s.g_cashout_sent.fetchAdd(1, .monotonic);
        if (client.send(&iso, alloc)) |resp| {
            defer @constCast(&resp).deinit();
            const rc = resp.get(39) orelse "";
            if (std.mem.eql(u8, rc, "00")) {
                _ = s.g_cashout_ok.fetchAdd(1, .monotonic);
            } else {
                _ = s.g_cashout_fail.fetchAdd(1, .monotonic);
            }
        } else |_| {
            _ = s.g_cashout_fail.fetchAdd(1, .monotonic);
        }

        var ts_now: std.c.timespec = undefined;
        _ = std.c.clock_gettime(.MONOTONIC, &ts_now);
        const elapsed_ns: i64 = (ts_now.sec - ts_slot.sec) * std.time.ns_per_s +
            (ts_now.nsec - ts_slot.nsec);
        if (elapsed_ns < s.CASHOUT_SLOT_NS) {
            const sleep_ns: i64 = s.CASHOUT_SLOT_NS - elapsed_ns;
            _ = std.c.nanosleep(&.{ .sec = 0, .nsec = @intCast(sleep_ns) }, null);
        }
        _ = std.c.clock_gettime(.MONOTONIC, &ts_slot);
    }
}
