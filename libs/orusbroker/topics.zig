// TopicRouter — maintains the subscriber registry and drives fan-out delivery.
//
// Design (V1):
//   • `router.mutex` (std.Thread.Mutex) protects subscribe/unsubscribe/snapshot.
//     deliver() snapshots matching subs under the lock, then writes outside —
//     so TCP I/O never blocks other publishers.
//   • Subs are heap-allocated by the subscriber connection handler.
//     They are freed by that same handler AFTER `unsubscribe` returns, which
//     guarantees no concurrent access by any publisher thread.
//   • `alive = false` is set on write failure; dead subs are skipped on the
//     next deliver call and cleaned up by the next unsubscribe.

const std       = @import("std");
const mutex_mod = @import("mutex.zig");
const proto     = @import("protocol.zig");

pub const Sub = struct {
    topic:  []const u8,       // alloc-owned by the subscriber handler
    stream: std.Io.net.Stream,
    io:     std.Io,
    alive:  bool,
};

pub const TopicRouter = struct {
    mutex: mutex_mod.BlockingMutex,
    subs:  std.ArrayList(*Sub),
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) TopicRouter {
        return .{ .mutex = .{}, .subs = .empty, .alloc = alloc };
    }

    pub fn deinit(self: *TopicRouter) void {
        self.subs.deinit(self.alloc);
    }

    pub fn subscribe(self: *TopicRouter, sub: *Sub) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.subs.append(self.alloc, sub);
    }

    // Marks the sub as dead and removes it from the list.
    // After this returns it is safe for the caller to free `sub`.
    pub fn unsubscribe(self: *TopicRouter, sub: *Sub) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        sub.alive = false;
        for (self.subs.items, 0..) |s, i| {
            if (s == sub) {
                _ = self.subs.swapRemove(i);
                return;
            }
        }
    }

    // Deliver `payload` to every alive subscriber whose topic matches.
    // Lock is held only during the snapshot — TCP writes happen outside
    // so that 256 concurrent publishers don't spin waiting on I/O.
    pub fn deliver(self: *TopicRouter, topic: []const u8, payload: []const u8, wal_offset: u64) void {
        const topic_len: u16 = @intCast(topic.len);
        // body = frame_type(1) + topic_len(2) + topic + wal_offset(8) + payload
        const body_len: u32 = @intCast(1 + 2 + topic.len + 8 + payload.len);

        // Snapshot matching subs under lock — O(n), n ≈ 1 in production.
        // We copy stream+io by value so the write below is safe even if the
        // sub is removed from the list (unsubscribe) between snapshot and write.
        const Snap = struct { stream: std.Io.net.Stream, io: std.Io, sub: *Sub };
        var snaps: [32]Snap = undefined;
        var n: usize = 0;
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            for (self.subs.items) |sub| {
                if (!sub.alive) continue;
                if (!std.mem.eql(u8, sub.topic, topic)) continue;
                if (n < snaps.len) {
                    snaps[n] = .{ .stream = sub.stream, .io = sub.io, .sub = sub };
                    n += 1;
                }
            }
        }

        // Write to each sub outside the lock — parallel delivers are now possible.
        for (snaps[0..n]) |snap| {
            var wbuf: [8192]u8 = undefined;
            var sw = snap.stream.writer(snap.io, &wbuf);
            const w = &sw.interface;
            const ok: bool = blk: {
                w.writeInt(u32, body_len,        .big) catch break :blk false;
                w.writeByte(proto.FRAME_DELIVER)       catch break :blk false;
                w.writeInt(u16, topic_len,       .big) catch break :blk false;
                w.writeAll(topic)                      catch break :blk false;
                w.writeInt(u64, wal_offset,      .big) catch break :blk false;
                w.writeAll(payload)                    catch break :blk false;
                w.flush()                              catch break :blk false;
                break :blk true;
            };
            if (!ok) {
                self.mutex.lock();
                snap.sub.alive = false;
                self.mutex.unlock();
            }
        }
    }
};
