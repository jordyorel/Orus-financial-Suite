// Ticker — samples metrics every second and updates ring buffers.
// computePercentiles — lock-free snapshot sort on the latency ring.

const std = @import("std");
const s   = @import("state.zig");

pub fn ticker() void {
    while (true) {
        _ = std.c.nanosleep(&.{ .sec = 1, .nsec = 0 }, null);

        const cur_gw = s.g_ok.load(.monotonic) + s.g_declined.load(.monotonic);
        const tps_gw = cur_gw - s.g_prev_gw;
        s.g_prev_gw = cur_gw;

        _ = s.g_hist_gen.fetchAdd(1, .release);
        const head = s.g_hist_head.load(.monotonic);
        s.g_hist_gw[head] = tps_gw;
        s.g_hist_head.store((head + 1) % 60, .monotonic);
        const cur_n = s.g_hist_n.load(.monotonic);
        if (cur_n < 60) _ = s.g_hist_n.fetchAdd(1, .monotonic);
        _ = s.g_hist_gen.fetchAdd(1, .release);

        const prev_peak = s.g_peak_tps.load(.monotonic);
        if (tps_gw > prev_peak) s.g_peak_tps.store(tps_gw, .monotonic);

        // D1
        const cur_d1 = s.g_ok.load(.monotonic);
        const tps_d1 = cur_d1 - s.g_prev_d1;
        s.g_prev_d1 = cur_d1;
        const head1 = s.g_hist_d1_head.load(.monotonic);
        s.g_hist_d1[head1] = tps_d1;
        s.g_hist_d1_head.store((head1 + 1) % 60, .monotonic);
        const nd1 = s.g_hist_d1_n.load(.monotonic);
        if (nd1 < 60) _ = s.g_hist_d1_n.fetchAdd(1, .monotonic);

        // D2
        const cur_d2 = s.g_d2_received.load(.monotonic);
        const tps_d2 = cur_d2 - s.g_prev_d2;
        s.g_prev_d2 = cur_d2;
        const head2 = s.g_hist_d2_head.load(.monotonic);
        s.g_hist_d2[head2] = tps_d2;
        s.g_hist_d2_head.store((head2 + 1) % 60, .monotonic);
        const nd2 = s.g_hist_d2_n.load(.monotonic);
        if (nd2 < 60) _ = s.g_hist_d2_n.fetchAdd(1, .monotonic);

        // D3
        const cur_d3 = s.g_cashout_ok.load(.monotonic);
        const tps_d3 = cur_d3 - s.g_prev_d3;
        s.g_prev_d3 = cur_d3;
        const head3 = s.g_hist_d3_head.load(.monotonic);
        s.g_hist_d3[head3] = tps_d3;
        s.g_hist_d3_head.store((head3 + 1) % 60, .monotonic);
        const nd3 = s.g_hist_d3_n.load(.monotonic);
        if (nd3 < 60) _ = s.g_hist_d3_n.fetchAdd(1, .monotonic);

        // WAL size (O_RDONLY=0, SEEK_END=2)
        const wfd = s.csock.open(s.WAL_PATH, 0);
        if (wfd >= 0) {
            const wsz = s.csock.lseek(wfd, 0, 2);
            if (wsz >= 0) s.g_wal_bytes.store(@intCast(wsz), .monotonic);
            _ = s.csock.close(wfd);
        }
    }
}

pub fn computePercentiles() struct { p50: u64, p99: u64 } {
    const n_raw = s.g_lat_idx.load(.monotonic);
    const n = @min(n_raw, s.LAT_CAP);
    if (n <= 1) return .{ .p50 = 0, .p99 = 0 };
    var tmp: [s.LAT_CAP]u64 = undefined;
    @memcpy(tmp[0..n], s.g_lat[0..n]);
    std.mem.sort(u64, tmp[0..n], {}, std.sort.asc(u64));
    const raw50 = tmp[n / 2];
    const raw99 = tmp[n * 99 / 100];
    return .{
        .p50 = @min(raw50, raw99) / 1000,
        .p99 = @max(raw50, raw99) / 1000,
    };
}
