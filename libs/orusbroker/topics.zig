// TopicRouter — subscriber registry + fan-out delivery.
//
// Delivery pipeline (post-MPSC refactor):
//   1. deliver() holds the mutex only for: list scan + frame allocation +
//      ring push.  No TCP I/O under the lock.  Lock hold ≈ 1–5 µs.
//   2. Each Sub owns an MPSC ring and a dedicated deliver thread.
//      The deliver thread drains the ring and writes directly to the
//      subscriber's TCP socket fd (std.posix.write) — zero io event-loop
//      overhead, zero cross-thread kqueue sharing.
//   3. Publishers never block on subscriber I/O — the convoy effect is gone.
//
// Sub lifetime:
//   subscribe()   → called by handleSubscribe after spawning the deliver thread
//   unsubscribe() → sets stopped flag, joins the deliver thread, then removes
//                   the sub from the list.  The caller may free the sub after
//                   unsubscribe() returns.

const std       = @import("std");
const mpsc_mod  = @import("mpsc.zig");
const mutex_mod = @import("mutex.zig");
const proto     = @import("protocol.zig");

pub const Sub = struct {
    topic:          []const u8,
    fd:             std.posix.fd_t,  // raw socket fd — deliver thread writes here
    alive:          bool,
    ring:           mpsc_mod.Ring,
    stopped:        std.atomic.Value(bool),
    deliver_thread: std.Thread,

    // Deliver loop — runs in a dedicated thread per Sub.
    // Drains the ring and writes pre-built wire frames directly to the TCP fd.
    // No io event-loop involved: avoids kqueue round-trip overhead (~500 µs
    // per frame when using std.Io.Threaded), keeping drain rate > 100k msg/s.
    // Exits when stopped is set AND the ring is empty.
    pub fn deliverLoop(sub: *Sub) void {
        var idle: u32 = 0;
        while (true) {
            if (sub.ring.pop()) |frame| {
                defer std.heap.c_allocator.free(frame);
                idle = 0;
                // Direct C write — retry on EAGAIN (non-blocking socket).
                const ok: bool = blk: {
                    var rem = frame;
                    while (rem.len > 0) {
                        const n = std.c.write(sub.fd, rem.ptr, rem.len);
                        if (n < 0) {
                            if (std.c._errno().* == @intFromEnum(std.c.E.AGAIN)) {
                                std.atomic.spinLoopHint();
                                continue;
                            }
                            break :blk false;
                        }
                        rem = rem[@intCast(n)..];
                    }
                    break :blk true;
                };
                if (!ok) sub.alive = false;
            } else {
                if (sub.stopped.load(.acquire)) break;
                // Brief adaptive idle: spin a few times then sleep 100 µs.
                if (idle < 64) {
                    std.atomic.spinLoopHint();
                    idle += 1;
                } else {
                    _ = std.c.nanosleep(&.{ .sec = 0, .nsec = 100_000 }, null);
                    idle = 0;
                }
            }
        }
        // Drain any frames pushed between the last pop() and stopped being read.
        while (sub.ring.pop()) |frame| std.heap.c_allocator.free(frame);
    }
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

    // Removes the sub from the list, stops its deliver thread, and waits for
    // it to finish draining.  The caller may free the sub after this returns.
    pub fn unsubscribe(self: *TopicRouter, sub: *Sub) void {
        self.mutex.lock();
        sub.alive = false;
        for (self.subs.items, 0..) |s, i| {
            if (s == sub) {
                _ = self.subs.swapRemove(i);
                break;
            }
        }
        self.mutex.unlock();

        // Signal deliver thread and wait for it to flush remaining frames.
        sub.stopped.store(true, .release);
        sub.deliver_thread.join();
    }

    // Deliver payload to every alive subscriber matching topic.
    // Builds the complete wire frame in memory and pushes it to the sub's
    // MPSC ring — no TCP I/O while the lock is held.
    pub fn deliver(self: *TopicRouter, topic: []const u8, payload: []const u8, wal_offset: u64) void {
        const topic_len: u16 = @intCast(topic.len);
        // body  = FRAME_DELIVER(1) + topic_len(2) + topic + wal_offset(8) + payload
        const body_len: u32 = @intCast(1 + 2 + topic.len + 8 + payload.len);
        // wire  = body_len header(4) + body
        const wire_len: usize = 4 + @as(usize, body_len);

        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.subs.items) |sub| {
            if (!sub.alive) continue;
            if (!std.mem.eql(u8, sub.topic, topic)) continue;

            // Allocate the complete wire frame; deliver thread frees it.
            const frame = std.heap.c_allocator.alloc(u8, wire_len) catch continue;
            var pos: usize = 0;
            std.mem.writeInt(u32, frame[pos..][0..4], body_len,        .big); pos += 4;
            frame[pos] = proto.FRAME_DELIVER;                                  pos += 1;
            std.mem.writeInt(u16, frame[pos..][0..2], topic_len,        .big); pos += 2;
            @memcpy(frame[pos..][0..topic.len], topic);                        pos += topic.len;
            std.mem.writeInt(u64, frame[pos..][0..8], wal_offset,       .big); pos += 8;
            @memcpy(frame[pos..][0..payload.len], payload);

            // Lock-free push — never blocks on TCP I/O.
            sub.ring.push(frame);
        }
    }
};
