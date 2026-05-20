// TopicRouter — maintains the subscriber registry and drives fan-out delivery.
//
// Design (V1):
//   • `router.mutex` serializes all subscribe / unsubscribe / deliver operations.
//     This means only one delivery can proceed at a time across all topics, which
//     is correct and simple. Parallel delivery per-topic is a V2 optimization.
//   • Subs are heap-allocated by the subscriber connection handler.
//     They are freed by that same handler AFTER `unsubscribe` returns, which
//     guarantees no concurrent access by any publisher thread.
//   • `alive = false` is set on write failure; dead subs are skipped on the
//     next deliver call and cleaned up by the next unsubscribe.

const std       = @import("std");
const mutex_mod = @import("mutex.zig");
const proto  = @import("protocol.zig");

pub const Sub = struct {
    topic: []const u8,       // alloc-owned by the subscriber handler
    stream: std.Io.net.Stream,
    io:    std.Io,
    alive: bool,
};

pub const TopicRouter = struct {
    mutex: mutex_mod.SpinMutex,
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

    // Deliver `payload` (pre-serialized InternalMessage) to every alive
    // subscriber whose topic matches. Write failures mark the sub dead.
    // `payload` must be valid for the duration of this call.
    pub fn deliver(self: *TopicRouter, topic: []const u8, payload: []const u8) void {
        const topic_len: u16 = @intCast(topic.len);
        const body_len:  u32 = @intCast(1 + 2 + topic.len + payload.len);

        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.subs.items) |sub| {
            if (!sub.alive) continue;
            if (!std.mem.eql(u8, sub.topic, topic)) continue;

            var wbuf: [8192]u8 = undefined;
            var sw = sub.stream.writer(sub.io, &wbuf);
            const w = &sw.interface;
            const ok: bool = blk: {
                w.writeInt(u32, body_len,           .big) catch break :blk false;
                w.writeByte(proto.FRAME_DELIVER)         catch break :blk false;
                w.writeInt(u16, topic_len,           .big) catch break :blk false;
                w.writeAll(topic)                        catch break :blk false;
                w.writeAll(payload)                      catch break :blk false;
                w.flush()                                catch break :blk false;
                break :blk true;
            };
            if (!ok) sub.alive = false;
        }
    }
};
