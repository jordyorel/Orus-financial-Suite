// Audit log — non-repudiable transaction journal (BEAC/BCEAO requirement).
//
// One line per financial event, flushed to disk periodically.
// Format (tab-separated):
//   {unix_sec}.{ns}  {MTI}  {STAN}  {amount}  {currency}  {RC}  {provider}  {msg_id_hex}
//
// The Unix timestamp is unambiguous and convertible to any timezone by auditors.
// sync_every controls durability vs. throughput:
//   1  = sync every write (full compliance, ~5 000 tx/s bounded by fsync)
//   N  = sync every N writes (at most N-1 records lost on crash)
//   0  = no sync (bench/test only — not suitable for production)
// The newline acts as a commit marker; an incomplete last line is ignored by parsers.
//
// Performance: record() formats the line on the caller's stack, then acquires the
// mutex only for a memcpy into the internal 64 KB write buffer.  The mutex hold
// time is O(line length) — nanoseconds — instead of a write() syscall.  The
// buffer is flushed to disk only when full or at a sync boundary (sync_every > 0).

const std = @import("std");

pub const AuditLog = struct {
    file:        std.Io.File,
    io:          std.Io,
    mu:          std.c.pthread_mutex_t,
    file_offset: u64,
    write_count: u64,
    // 1 = compliance default; set higher for benchmarks.
    sync_every:  u64,
    buf:         [65536]u8,
    buf_used:    usize,

    pub fn open(io: std.Io, path: []const u8) !AuditLog {
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = false });
        const size = std.Io.File.length(file, io) catch 0;
        return .{
            .file = file, .io = io,
            .mu = .{},
            .file_offset = size, .write_count = 0, .sync_every = 1,
            .buf = undefined, .buf_used = 0,
        };
    }

    pub fn close(self: *AuditLog) void {
        _ = std.c.pthread_mutex_lock(&self.mu);
        self.flushNoLock();
        std.Io.File.sync(self.file, self.io) catch {};
        _ = std.c.pthread_mutex_unlock(&self.mu);
        std.Io.File.close(self.file, self.io);
    }

    // Caller must hold self.mu.
    fn flushNoLock(self: *AuditLog) void {
        if (self.buf_used == 0) return;
        std.Io.File.writePositionalAll(self.file, self.io, self.buf[0..self.buf_used], self.file_offset) catch return;
        self.file_offset += self.buf_used;
        self.buf_used = 0;
    }

    // Record one financial event. Thread-safe. Best-effort: errors are silently
    // dropped so audit log failures never abort the payment processing path.
    pub fn record(
        self:     *AuditLog,
        mti:      []const u8,
        stan:     []const u8,
        amount:   i64,
        currency: []const u8,
        rc:       []const u8,
        provider: []const u8,
        msg_id:   [16]u8,
    ) void {
        // Format the line on the caller's stack — no lock needed.
        var ts: std.c.timespec = undefined;
        _ = std.c.clock_gettime(.REALTIME, &ts);

        var hex_id: [32]u8 = undefined;
        const hex_chars = "0123456789abcdef";
        for (msg_id, 0..) |b, i| {
            hex_id[i * 2]     = hex_chars[b >> 4];
            hex_id[i * 2 + 1] = hex_chars[b & 0xf];
        }

        var line_buf: [512]u8 = undefined;
        const nsec: u64 = @intCast(ts.nsec);
        const line = std.fmt.bufPrint(&line_buf,
            "{d}.{d:0>9}\t{s}\t{s}\t{d}\t{s}\t{s}\t{s}\t{s}\n",
            .{
                ts.sec, nsec,
                mti, stan, amount, currency, rc, provider,
                &hex_id,
            },
        ) catch return;

        // Acquire lock — hold only for memcpy (nanoseconds in the common case).
        _ = std.c.pthread_mutex_lock(&self.mu);
        defer _ = std.c.pthread_mutex_unlock(&self.mu);

        // Flush buffer to disk if the new line would overflow it.
        if (self.buf_used + line.len > self.buf.len) self.flushNoLock();

        @memcpy(self.buf[self.buf_used..][0..line.len], line);
        self.buf_used    += line.len;
        self.write_count += 1;

        if (self.sync_every > 0 and self.write_count % self.sync_every == 0) {
            self.flushNoLock();
            std.Io.File.sync(self.file, self.io) catch {};
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "AuditLog: open, record, close" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    const io = threaded.io();

    const path = "/tmp/orus_audit_test.log";
    std.Io.Dir.cwd().deleteFile(io, path) catch {};

    var log = try AuditLog.open(io, path);
    log.record("0200", "001234", 10_000, "XAF", "00", "mtn_momo", [_]u8{0xAB} ** 16);
    log.record("0420", "001235", 5_000,  "XAF", "00", "orange_money", [_]u8{0xCD} ** 16);
    log.close();

    const file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer std.Io.File.close(file, io);
    const size = try std.Io.File.length(file, io);
    try std.testing.expect(size > 0);

    std.Io.Dir.cwd().deleteFile(io, path) catch {};
}
