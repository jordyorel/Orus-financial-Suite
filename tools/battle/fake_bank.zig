// FakeChaosBank — libc TCP server, pre-spawned accept() pool.
// Keep-alive: one connection per gateway worker, multiple requests per connection.
// Chaos config (latency, noresp%, rc51%, rc91%) is read atomically each request.

const std    = @import("std");
const gw_mod = @import("orusgateway");
const s      = @import("state.zig");

fn chaosMacReply(alloc: std.mem.Allocator, mti: [4]u8, stan: []const u8, rc: []const u8) ![]u8 {
    var resp = gw_mod.iso8583.IsoMessage.init(alloc, mti);
    defer resp.deinit();
    try resp.set(39, rc);
    try resp.set(11, stan);
    try resp.set(49, "XAF");
    const scope = try gw_mod.iso8583.serialize(&resp, alloc);
    defer alloc.free(scope);
    const mac_val = s.chaos_mac.compute(scope);
    try resp.set(64, &mac_val);
    return gw_mod.iso8583.serializeWithSchema(&resp, &gw_mod.DEFAULT_SCHEMA, alloc);
}

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

fn handleChaosFd(fd: c_int) void {
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

        if (std.mem.eql(u8, &iso_req.mti, "0800")) {
            const nmi = iso_req.get(70) orelse "";
            var resp_iso = gw_mod.buildNetworkResponse(&iso_req, alloc, if (std.mem.eql(u8, nmi, "301")) s.SESSION_KEY else "") catch return;
            defer resp_iso.deinit();
            const bytes = gw_mod.iso8583.serializeWithSchema(&resp_iso, &gw_mod.DEFAULT_SCHEMA, alloc) catch return;
            defer alloc.free(bytes);
            var lout: [2]u8 = undefined;
            std.mem.writeInt(u16, &lout, @intCast(bytes.len), .big);
            if (!writeAllFd(fd, &lout)) return;
            if (!writeAllFd(fd, bytes)) return;
            continue;
        }

        var prng: u64 = undefined;
        std.c.arc4random_buf(@ptrCast(&prng), 8);

        if (prng % 100 < s.g_noresp_pct.load(.monotonic)) {
            _ = s.g_bank_noresp.fetchAdd(1, .monotonic);
            return;
        }

        const lat_ms = s.g_latency_ms.load(.monotonic);
        if (lat_ms > 0) {
            const ns: u64 = @as(u64, lat_ms) * 1_000_000;
            _ = std.c.nanosleep(&.{ .sec = 0, .nsec = @intCast(@min(ns, 999_999_999)) }, null);
        }

        const roll = (prng >> 8) % 100;
        const rc51 = s.g_rc51_pct.load(.monotonic);
        const rc91 = s.g_rc91_pct.load(.monotonic);
        const rc: []const u8 = if (roll < rc51) blk: {
            _ = s.g_bank_rc51.fetchAdd(1, .monotonic);
            break :blk "51";
        } else if (roll < rc51 + rc91) blk: {
            _ = s.g_bank_rc91.fetchAdd(1, .monotonic);
            break :blk "91";
        } else blk: {
            _ = s.g_bank_rc00.fetchAdd(1, .monotonic);
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

pub fn runChaosBankWorker(bank_sock: c_int) void {
    while (true) {
        const client = s.csock.accept(bank_sock, null, null);
        if (client < 0) {
            if (s.g_stopped.load(.monotonic)) return;
            continue;
        }
        const tv = s.CTimeval{ .tv_sec = 10 };
        _ = s.csock.setsockopt(client, s.C_SOL_SOCKET, s.C_SO_RCVTIMEO, @ptrCast(&tv), @sizeOf(s.CTimeval));
        handleChaosFd(client);
        _ = s.csock.close(client);
    }
}
