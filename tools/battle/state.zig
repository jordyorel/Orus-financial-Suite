// Battle test — shared constants, global atomics, C socket bindings.
// Every other battle/* file imports this module as `const s = @import("state.zig")`.

const std      = @import("std");
const orusbroker = @import("orusbroker");
const gw_mod   = @import("orusgateway");

// ── Ports / paths ─────────────────────────────────────────────────────────────
pub const BROKER_PORT:     u16 = 7799;
pub const CHAOS_BANK_PORT: u16 = 5099;
pub const BANKSERVER_PORT: u16 = 7797;
pub const MOMO_PORT:       u16 = 7796;
pub const HTTP_PORT:       u16 = 7782;
pub const TOPIC          = "battle.inbound";
pub const TOPIC_OUTBOUND = "battle.outbound";
pub const AUDIT_PATH     = "/tmp/battle_audit.log";
pub const WAL_PATH       = "/tmp/battle.wal";
pub const SESSION_KEY    = "orus_battle_key_32bytes_padxxxxx";

// Cashout rate cap — prevents CPU starvation of gateway workers when bank
// latency is high.
pub const CASHOUT_MAX_TPS:  i64 = 5_000;
pub const CASHOUT_SLOT_NS:  i64 = @divTrunc(std.time.ns_per_s, CASHOUT_MAX_TPS);

// Worker counts
pub const N_GATEWAY_WORKERS:    usize = 256;
pub const N_CHAOSBANK_WORKERS:  usize = 270; // 256 for gw keep-alive + 14 slack
pub const N_MOMO_WORKERS:       usize = 8;
pub const N_CASHOUT_WORKERS:    usize = 8;

// ── Chaos config (live-adjustable via POST /chaos) ────────────────────────────
pub var g_latency_ms = std.atomic.Value(u32).init(0);
pub var g_noresp_pct = std.atomic.Value(u32).init(0);
pub var g_rc51_pct   = std.atomic.Value(u32).init(0);
pub var g_rc91_pct   = std.atomic.Value(u32).init(0);

// ── Chaos bank events ─────────────────────────────────────────────────────────
pub var g_bank_noresp = std.atomic.Value(u64).init(0);
pub var g_bank_rc00   = std.atomic.Value(u64).init(0);
pub var g_bank_rc51   = std.atomic.Value(u64).init(0);
pub var g_bank_rc91   = std.atomic.Value(u64).init(0);

// ── Integrity counters ────────────────────────────────────────────────────────
pub var g_sent    = std.atomic.Value(u64).init(0);
pub var g_ok      = std.atomic.Value(u64).init(0);
pub var g_declined= std.atomic.Value(u64).init(0);
pub var g_err_conn= std.atomic.Value(u64).init(0);
pub var g_err_mac = std.atomic.Value(u64).init(0);

// ── Throughput history (seqlock-protected) ────────────────────────────────────
pub var g_hist_gen  = std.atomic.Value(usize).init(0);
pub var g_hist_gw:  [60]u64 = [_]u64{0} ** 60;
pub var g_hist_head = std.atomic.Value(usize).init(0);
pub var g_hist_n    = std.atomic.Value(usize).init(0);
pub var g_prev_gw:  u64 = 0;
pub var g_peak_tps  = std.atomic.Value(u64).init(0);
pub var g_last_tx_ns= std.atomic.Value(i64).init(0);

// D1
pub var g_hist_d1:      [60]u64 = [_]u64{0} ** 60;
pub var g_prev_d1:      u64 = 0;
pub var g_hist_d1_head  = std.atomic.Value(usize).init(0);
pub var g_hist_d1_n     = std.atomic.Value(usize).init(0);

// D2
pub var g_hist_d2:      [60]u64 = [_]u64{0} ** 60;
pub var g_prev_d2:      u64 = 0;
pub var g_hist_d2_head  = std.atomic.Value(usize).init(0);
pub var g_hist_d2_n     = std.atomic.Value(usize).init(0);

// D3
pub var g_hist_d3:      [60]u64 = [_]u64{0} ** 60;
pub var g_prev_d3:      u64 = 0;
pub var g_hist_d3_head  = std.atomic.Value(usize).init(0);
pub var g_hist_d3_n     = std.atomic.Value(usize).init(0);

pub var g_d1_total = std.atomic.Value(u64).init(0);
pub var g_d2_total = std.atomic.Value(u64).init(0);

// ── Latency ring buffer ───────────────────────────────────────────────────────
pub const LAT_CAP = 2000;
pub var g_lat:     [LAT_CAP]u64 = [_]u64{0} ** LAT_CAP;
pub var g_lat_idx  = std.atomic.Value(usize).init(0);

// ── Lifecycle ─────────────────────────────────────────────────────────────────
pub var g_stopped    = std.atomic.Value(bool).init(false);
pub var g_mac_active = std.atomic.Value(bool).init(false);
pub var g_start_ns:  i64 = 0;

// ── Pipeline health ───────────────────────────────────────────────────────────
pub var g_hb_ok       = std.atomic.Value(u64).init(0);
pub var g_hb_fail     = std.atomic.Value(u64).init(0);
pub var g_hb_last_ns  = std.atomic.Value(i64).init(0);
pub var g_reversals   = std.atomic.Value(u64).init(0);
pub var g_rev_ok      = std.atomic.Value(u64).init(0);
pub var g_rev_fail    = std.atomic.Value(u64).init(0);
pub var g_d2_sent     = std.atomic.Value(u64).init(0);
pub var g_d2_received = std.atomic.Value(u64).init(0);
pub var g_audit_lines = std.atomic.Value(u64).init(0);
pub var g_wal_bytes   = std.atomic.Value(u64).init(0);

// ── Startup validation flags (D0–D6) ─────────────────────────────────────────
pub var g_startup_d0 = std.atomic.Value(bool).init(false);
pub var g_startup_d1 = std.atomic.Value(bool).init(false);
pub var g_startup_d2 = std.atomic.Value(bool).init(false);
pub var g_startup_d3 = std.atomic.Value(bool).init(false);
pub var g_startup_d4 = std.atomic.Value(bool).init(false);
pub var g_startup_d5 = std.atomic.Value(bool).init(false);
pub var g_startup_d6 = std.atomic.Value(bool).init(false);

// ── Cashout counters ──────────────────────────────────────────────────────────
pub var g_cashout_sent = std.atomic.Value(u64).init(0);
pub var g_cashout_ok   = std.atomic.Value(u64).init(0);
pub var g_cashout_fail = std.atomic.Value(u64).init(0);

// ── Chaos bank MAC engine ─────────────────────────────────────────────────────
pub var chaos_mac = gw_mod.MacEngine.init();

// ── Broker in-process ─────────────────────────────────────────────────────────
pub const BrokerInner = struct {
    wal:    orusbroker.Wal,
    dedup:  orusbroker.DedupFilter,
    router: orusbroker.TopicRouter,
    server: orusbroker.BrokerServer,
};

// ── libc socket API (std.posix was gutted in Zig 0.16) ───────────────────────
pub const csock = struct {
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

// macOS constants
pub const C_AF_INET:    c_int = 2;
pub const C_SOCK_STREAM:c_int = 1;
pub const C_IPPROTO_TCP:c_int = 6;
pub const C_SOL_SOCKET: c_int = 0xffff;
pub const C_SO_REUSEADDR:c_int= 0x0004;
pub const C_SO_REUSEPORT:c_int= 0x0200;
pub const C_SO_RCVTIMEO: c_int= 0x1006;

pub const CTimeval = extern struct { tv_sec: i64 = 0, tv_usec: i32 = 0, _pad: i32 = 0 };
pub const SockaddrIn = extern struct {
    sin_len:    u8  = @sizeOf(SockaddrIn),
    sin_family: u8  = 2,
    sin_port:   u16 = 0,
    sin_addr:   u32 = 0,
    sin_zero:   [8]u8 = .{0} ** 8,
};

pub const HttpFd  = c_int;
pub const HttpCtx = struct { fd: HttpFd };
