// Per-subscriber WAL cursor — persists the next-to-deliver WAL byte offset.
//
// File layout: {dir}/{sub_id}.cursor — 8 bytes, little-endian u64.
// A missing file is treated as 0 (fresh subscriber, start from WAL tip).
// All I/O errors in load() return 0; store() errors are silently ignored.

const std   = @import("std");
const proto = @import("protocol.zig");

pub const Cursor = struct {
    dir: []const u8,
    io:  std.Io,

    pub fn init(dir: []const u8, io: std.Io) Cursor {
        return .{ .dir = dir, .io = io };
    }

    // Returns 0 if the cursor file does not exist or cannot be read.
    pub fn load(self: *const Cursor, sub_id: []const u8) u64 {
        var path_buf: [proto.MAX_SUB_ID_LEN + 300]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}.cursor", .{ self.dir, sub_id }) catch return 0;
        const file = std.Io.Dir.cwd().openFile(self.io, path, .{}) catch return 0;
        defer std.Io.File.close(file, self.io);
        var buf: [8]u8 = undefined;
        const n = std.Io.File.readPositionalAll(file, self.io, &buf, 0) catch return 0;
        if (n < 8) return 0;
        return std.mem.readInt(u64, &buf, .little);
    }

    // Writes the cursor atomically (create + truncate).
    // Silently ignores all errors to avoid disrupting the ACK loop.
    pub fn store(self: *const Cursor, sub_id: []const u8, offset: u64) void {
        var path_buf: [proto.MAX_SUB_ID_LEN + 300]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}.cursor", .{ self.dir, sub_id }) catch return;
        std.Io.Dir.cwd().createDirPath(self.io, self.dir) catch {};
        const file = std.Io.Dir.cwd().createFile(self.io, path, .{ .truncate = true }) catch return;
        defer std.Io.File.close(file, self.io);
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, offset, .little);
        std.Io.File.writePositionalAll(file, self.io, &buf, 0) catch {};
    }
};
