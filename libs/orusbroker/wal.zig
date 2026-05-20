// Write-Ahead Log — append-only, CRC32-guarded, fsync on every write.
//
// Entry layout on disk:
//   [magic:   u32 BE = 0xDEADBEEF]
//   [crc32:   u32 BE]               -- CRC32 of the payload bytes
//   [length:  u32 BE]               -- payload length in bytes
//   [payload: length bytes]         -- serialized InternalMessage
//
// Positional I/O is used throughout (pread/pwrite) so the Wal keeps an
// explicit `offset` cursor rather than relying on seek state.
// Thread-safe: all mutations are serialised by `mutex`.

const std   = @import("std");
const mutex_mod = @import("mutex.zig");

const MAGIC: u32 = 0xDEADBEEF;
const HDR_LEN: u64 = 12; // 4 (magic) + 4 (crc32) + 4 (length)

pub const WalError = error{ Corrupt, OutOfMemory };

pub const Wal = struct {
    file:   std.Io.File,
    io:     std.Io,
    offset: u64,             // byte offset of the next write (= current file size)
    mutex:  mutex_mod.SpinMutex,

    pub fn open(io: std.Io, path: []const u8) !Wal {
        const file = try std.Io.Dir.cwd().createFile(io, path, .{
            .read     = true,
            .truncate = false,
        });
        const size = std.Io.File.length(file, io) catch 0;
        return .{ .file = file, .io = io, .offset = size, .mutex = .{} };
    }

    pub fn close(self: *Wal) void {
        std.Io.File.close(self.file, self.io);
    }

    pub fn append(self: *Wal, payload: []const u8) !void {
        const crc: u32 = std.hash.Crc32.hash(payload);
        const len: u32 = @intCast(payload.len);

        var hdr: [12]u8 = undefined;
        std.mem.writeInt(u32, hdr[0..4],  MAGIC, .big);
        std.mem.writeInt(u32, hdr[4..8],  crc,   .big);
        std.mem.writeInt(u32, hdr[8..12], len,   .big);

        self.mutex.lock();
        defer self.mutex.unlock();

        try std.Io.File.writePositionalAll(self.file, self.io, &hdr, self.offset);
        self.offset += HDR_LEN;
        try std.Io.File.writePositionalAll(self.file, self.io, payload, self.offset);
        self.offset += payload.len;
        try std.Io.File.sync(self.file, self.io);
    }

    // Replay every valid entry from byte offset 0.
    // Calls `cb` for each payload; the slice is valid only for the call duration.
    // Stops cleanly at EOF; returns WalError.Corrupt on any integrity failure.
    pub fn replay(
        self:  *Wal,
        alloc: std.mem.Allocator,
        cb:    *const fn (payload: []const u8, ctx: ?*anyopaque) void,
        ctx:   ?*anyopaque,
    ) !void {
        var pos: u64 = 0;

        while (pos + HDR_LEN <= self.offset) {
            var hdr: [12]u8 = undefined;
            const hn = try std.Io.File.readPositionalAll(self.file, self.io, &hdr, pos);
            if (hn == 0) break;
            if (hn < HDR_LEN) return error.Corrupt;
            pos += HDR_LEN;

            const magic        = std.mem.readInt(u32, hdr[0..4],  .big);
            const expected_crc = std.mem.readInt(u32, hdr[4..8],  .big);
            const length       = std.mem.readInt(u32, hdr[8..12], .big);

            if (magic != MAGIC) return error.Corrupt;
            if (pos + length > self.offset) return error.Corrupt;

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
