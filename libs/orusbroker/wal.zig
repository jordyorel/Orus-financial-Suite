// Write-Ahead Log — append-only, CRC32-guarded, lock-free.
//
// Entry layout on disk:
//   [magic:   u32 BE = 0xDEADBEEF]
//   [crc32:   u32 BE]               -- CRC32 of the payload bytes
//   [length:  u32 BE]               -- payload length in bytes
//   [payload: length bytes]         -- serialized InternalMessage
//
// Positional I/O is used throughout (pread/pwrite) so concurrent writes to
// disjoint regions are POSIX-safe without any mutex on the write path.
//
// Each append atomically reserves a slot via fetchAdd on `offset`, then
// does two pwrite calls (header, payload) at the reserved position.
// No mutex is held during I/O — 512 concurrent writers contend only on
// a single atomic increment.
//
// sync_every controls durability vs. throughput:
//   1 = sync every write (full compliance — default)
//   N = sync every N writes (at most N-1 entries lost on crash)
//   0 = no sync (bench/test only)
//
// Replay note: `offset` reflects reserved space, which may include entries
// whose pwrite calls are still in flight. Replay is only called during
// crash recovery (no concurrent appenders), so CRC checks catch any
// partial writes cleanly.

const std       = @import("std");

const MAGIC: u32 = 0xDEADBEEF;
const HDR_LEN: u64 = 12; // 4 (magic) + 4 (crc32) + 4 (length)

pub const WalError = error{ Corrupt, OutOfMemory };

pub const Wal = struct {
    file:        std.Io.File,
    io:          std.Io,
    offset:      std.atomic.Value(u64), // logical end of WAL (reserved bytes)
    write_count: std.atomic.Value(u64),
    sync_every:  u64,

    pub fn open(io: std.Io, path: []const u8) !Wal {
        const file = try std.Io.Dir.cwd().createFile(io, path, .{
            .read     = true,
            .truncate = false,
        });
        const size = std.Io.File.length(file, io) catch 0;
        return .{
            .file        = file,
            .io          = io,
            .offset      = std.atomic.Value(u64).init(size),
            .write_count = std.atomic.Value(u64).init(0),
            .sync_every  = 1,
        };
    }

    pub fn close(self: *Wal) void {
        if (self.sync_every > 0)
            std.Io.File.sync(self.file, self.io) catch {};
        std.Io.File.close(self.file, self.io);
    }

    // Append payload; returns the byte offset of the entry start.
    pub fn appendGetOffset(self: *Wal, payload: []const u8) !u64 {
        const crc: u32 = std.hash.Crc32.hash(payload);
        const len: u32 = @intCast(payload.len);

        var hdr: [12]u8 = undefined;
        std.mem.writeInt(u32, hdr[0..4],  MAGIC, .big);
        std.mem.writeInt(u32, hdr[4..8],  crc,   .big);
        std.mem.writeInt(u32, hdr[8..12], len,   .big);

        const entry_size: u64 = HDR_LEN + @as(u64, payload.len);

        // Reserve our slot — the only contended operation, and it's a single
        // atomic RMW with no lock, no cache-line write-back loop.
        const start = self.offset.fetchAdd(entry_size, .monotonic);

        // Write header then payload at the reserved position.
        // Concurrent pwrites to disjoint file regions are safe on POSIX.
        try std.Io.File.writePositionalAll(self.file, self.io, &hdr, start);
        try std.Io.File.writePositionalAll(self.file, self.io, payload, start + HDR_LEN);

        // Fsync on boundary — multiple concurrent fsyncs are harmless (idempotent).
        const wc = self.write_count.fetchAdd(1, .monotonic) + 1;
        if (self.sync_every > 0 and wc % self.sync_every == 0)
            std.Io.File.sync(self.file, self.io) catch {};

        return start;
    }

    pub fn append(self: *Wal, payload: []const u8) !void {
        _ = try self.appendGetOffset(payload);
    }

    // Current logical write position (reserved bytes, may include in-flight pwrites).
    pub fn currentOffset(self: *Wal) u64 {
        return self.offset.load(.monotonic);
    }

    // Replay every valid entry in [start, end).
    // Calls cb(payload, next_offset, ctx) for each entry.
    // Safe only when no concurrent appenders are active.
    pub fn replayFrom(
        self:  *Wal,
        start: u64,
        end:   u64,
        alloc: std.mem.Allocator,
        cb:    *const fn (payload: []const u8, next_offset: u64, ctx: ?*anyopaque) void,
        ctx:   ?*anyopaque,
    ) WalError!void {
        var pos: u64 = start;

        while (pos + HDR_LEN <= end) {
            var hdr: [12]u8 = undefined;
            const hn = std.Io.File.readPositionalAll(self.file, self.io, &hdr, pos) catch
                return error.Corrupt;
            if (hn == 0) break;
            if (hn < HDR_LEN) return error.Corrupt;

            const magic        = std.mem.readInt(u32, hdr[0..4],  .big);
            const expected_crc = std.mem.readInt(u32, hdr[4..8],  .big);
            const length       = std.mem.readInt(u32, hdr[8..12], .big);

            if (magic != MAGIC) return error.Corrupt;
            if (pos + HDR_LEN + length > end) return error.Corrupt;

            const payload = alloc.alloc(u8, length) catch return error.OutOfMemory;
            defer alloc.free(payload);

            const pn = std.Io.File.readPositionalAll(self.file, self.io, payload, pos + HDR_LEN) catch
                return error.Corrupt;
            if (pn < length) return error.Corrupt;

            const actual_crc: u32 = std.hash.Crc32.hash(payload);
            if (actual_crc != expected_crc) return error.Corrupt;

            const next_offset = pos + HDR_LEN + length;
            cb(payload, next_offset, ctx);
            pos = next_offset;
        }
    }

    // Replay every valid entry from byte offset 0.
    pub fn replay(
        self:  *Wal,
        alloc: std.mem.Allocator,
        cb:    *const fn (payload: []const u8, ctx: ?*anyopaque) void,
        ctx:   ?*anyopaque,
    ) !void {
        var pos: u64 = 0;
        const end = self.offset.load(.monotonic);

        while (pos + HDR_LEN <= end) {
            var hdr: [12]u8 = undefined;
            const hn = try std.Io.File.readPositionalAll(self.file, self.io, &hdr, pos);
            if (hn == 0) break;
            if (hn < HDR_LEN) return error.Corrupt;
            pos += HDR_LEN;

            const magic        = std.mem.readInt(u32, hdr[0..4],  .big);
            const expected_crc = std.mem.readInt(u32, hdr[4..8],  .big);
            const length       = std.mem.readInt(u32, hdr[8..12], .big);

            if (magic != MAGIC) return error.Corrupt;
            if (pos + length > end) return error.Corrupt;

            const payload = try alloc.alloc(u8, length);
            defer alloc.free(payload);

            const pn = try std.Io.File.readPositionalAll(self.file, self.io, payload, pos);
            if (pn < length) return error.Corrupt;
            pos += length;

            const actual_crc: u32 = std.hash.Crc32.hash(payload);
            if (actual_crc != expected_crc) return error.Corrupt;

            cb(payload, ctx);
        }
    }
};
