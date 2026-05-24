// BrokerServer — TCP accept loop + per-connection handler.
//
// Connection lifecycle:
//   1. Accept TCP connection.
//   2. Read the first frame header → determine PUBLISH or SUBSCRIBE.
//   3a. PUBLISH: dedup → WAL append → fan-out DELIVER → send PUBLISH_ACK.
//       Loops reading additional PUBLISH frames on the same connection until
//       the publisher closes (EOF). One-shot publishers (close after ACK) exit
//       the loop naturally on EOF — fully backward-compatible.
//   3b. SUBSCRIBE: read cursor, replay WAL, register Sub, loop reading ACK
//       frames until disconnect. On each ACK, persist cursor.

const std        = @import("std");
const orusshare  = @import("orusshare");
const serialize  = orusshare.serialize;
const proto      = @import("protocol.zig");
const wal_mod    = @import("wal.zig");
const dedup_mod  = @import("dedup.zig");
const topics_mod = @import("topics.zig");
const cursor_mod = @import("cursor.zig");

pub const Config = struct {
    host:       []const u8 = "0.0.0.0",
    port:       u16        = 7770,
    wal_path:   []const u8 = "orus_broker.wal",
    cursor_dir: []const u8 = "/tmp/orus_cursors",
};

pub const BrokerServer = struct {
    config: Config,
    io:     std.Io,
    wal:    *wal_mod.Wal,
    dedup:  *dedup_mod.DedupFilter,
    router: *topics_mod.TopicRouter,
    cursor: cursor_mod.Cursor,

    // Blocking accept loop — runs until the listener is closed.
    pub fn serve(self: *const BrokerServer, alloc: std.mem.Allocator) !void {
        const addr = try std.Io.net.IpAddress.parse(self.config.host, self.config.port);
        var server = try addr.listen(self.io, .{ .reuse_address = true });
        defer server.deinit(self.io);

        std.log.info("OrusBroker listening on {s}:{d}  wal={s}  cursors={s}", .{
            self.config.host, self.config.port,
            self.config.wal_path, self.config.cursor_dir,
        });

        while (true) {
            const stream = server.accept(self.io) catch |err| {
                if (err == error.SocketNotListening) break;
                std.log.warn("broker: accept error: {}", .{err});
                continue;
            };

            const ctx = alloc.create(ConnCtx) catch {
                stream.close(self.io);
                continue;
            };
            ctx.* = .{ .stream = stream, .server = self, .alloc = alloc };

            const t = std.Thread.spawn(.{}, handleConn, .{ctx}) catch |err| {
                std.log.err("broker: thread spawn failed: {}", .{err});
                stream.close(self.io);
                alloc.destroy(ctx);
                continue;
            };
            t.detach();
        }
    }
};

const ConnCtx = struct {
    stream: std.Io.net.Stream,
    server: *const BrokerServer,
    alloc:  std.mem.Allocator,
};

fn handleConn(ctx: *ConnCtx) void {
    defer ctx.alloc.destroy(ctx);
    defer ctx.stream.close(ctx.server.io);

    var rbuf: [16384]u8 = undefined;
    var sr = ctx.stream.reader(ctx.server.io, &rbuf);
    const r = &sr.interface;

    var len_bytes: [4]u8 = undefined;
    r.readSliceAll(&len_bytes) catch return;
    const body_len = std.mem.readInt(u32, &len_bytes, .big);
    if (body_len == 0 or body_len > proto.MAX_PAYLOAD) return;

    const frame_type = r.takeByte() catch return;
    const remaining: u32 = body_len - 1;

    switch (frame_type) {
        proto.FRAME_PUBLISH => {
            handlePublish(ctx, r, remaining);
            // Persistent publish loop: keep reading PUBLISH frames until EOF.
            while (true) {
                var next_len: [4]u8 = undefined;
                r.readSliceAll(&next_len) catch return; // EOF = publisher closed normally
                const next_body = std.mem.readInt(u32, &next_len, .big);
                if (next_body == 0 or next_body > proto.MAX_PAYLOAD) return;
                const next_type = r.takeByte() catch return;
                if (next_type != proto.FRAME_PUBLISH) return;
                handlePublish(ctx, r, next_body - 1);
            }
        },
        proto.FRAME_SUBSCRIBE => handleSubscribe(ctx, r, remaining),
        else => {},
    }
}

fn handlePublish(ctx: *ConnCtx, r: *std.Io.Reader, remaining: u32) void {
    const alloc = ctx.alloc;

    var tl: [2]u8 = undefined;
    r.readSliceAll(&tl) catch return;
    const topic_len = std.mem.readInt(u16, &tl, .big);
    if (topic_len > proto.MAX_TOPIC_LEN) return;

    const topic = alloc.alloc(u8, topic_len) catch return;
    defer alloc.free(topic);
    r.readSliceAll(topic) catch return;

    const hdr: u32 = 2 + topic_len;
    if (remaining < hdr) return;
    const payload_len: usize = remaining - hdr;
    const payload = alloc.alloc(u8, payload_len) catch return;
    defer alloc.free(payload);
    r.readSliceAll(payload) catch return;

    var fixed_r = std.Io.Reader.fixed(payload);
    const msg = serialize.deserialize(&fixed_r, alloc) catch {
        std.log.warn("broker: publish: deserialize failed", .{});
        sendPublishAck(ctx);
        return;
    };
    defer serialize.free(&msg, alloc);

    if (ctx.server.dedup.checkAndSet(msg.msg_id)) {
        std.log.debug("broker: duplicate msg_id discarded", .{});
        sendPublishAck(ctx);
        return;
    }

    const start_offset = ctx.server.wal.appendGetOffset(payload) catch |err| {
        std.log.warn("broker: WAL append failed: {} (delivering anyway)", .{err});
        ctx.server.router.deliver(topic, payload, 0);
        sendPublishAck(ctx);
        return;
    };
    // after_offset = offset of the next entry after this one.
    const after_offset: u64 = start_offset + 12 + payload.len;
    ctx.server.router.deliver(topic, payload, after_offset);

    sendPublishAck(ctx);
}

fn sendPublishAck(ctx: *ConnCtx) void {
    var wbuf: [8]u8 = undefined;
    var sw = ctx.stream.writer(ctx.server.io, &wbuf);
    sw.interface.writeByte(proto.FRAME_PUBLISH_ACK) catch return;
    sw.interface.flush() catch {};
}

// Context passed to replayCb during WAL replay for a reconnecting subscriber.
const ReplayCtx = struct {
    w:     *std.Io.Writer,
    topic: []const u8,
    fail:  bool,
};

fn replayCb(payload: []const u8, next_offset: u64, ctx_ptr: ?*anyopaque) void {
    const rc: *ReplayCtx = @ptrCast(@alignCast(ctx_ptr.?));
    if (rc.fail) return;

    const topic_len: u16 = @intCast(rc.topic.len);
    const body_len:  u32 = @intCast(1 + 2 + rc.topic.len + 8 + payload.len);

    const ok: bool = blk: {
        rc.w.writeInt(u32, body_len,           .big) catch break :blk false;
        rc.w.writeByte(proto.FRAME_DELIVER)         catch break :blk false;
        rc.w.writeInt(u16, topic_len,           .big) catch break :blk false;
        rc.w.writeAll(rc.topic)                     catch break :blk false;
        rc.w.writeInt(u64, next_offset,         .big) catch break :blk false;
        rc.w.writeAll(payload)                      catch break :blk false;
        rc.w.flush()                                catch break :blk false;
        break :blk true;
    };
    if (!ok) rc.fail = true;
}

fn handleSubscribe(ctx: *ConnCtx, r: *std.Io.Reader, remaining: u32) void {
    const alloc = ctx.alloc;
    _ = remaining;

    // Parse topic.
    var tl: [2]u8 = undefined;
    r.readSliceAll(&tl) catch return;
    const topic_len = std.mem.readInt(u16, &tl, .big);
    if (topic_len > proto.MAX_TOPIC_LEN) return;

    const topic = alloc.alloc(u8, topic_len) catch return;
    r.readSliceAll(topic) catch {
        alloc.free(topic);
        return;
    };

    // Parse sub_id (may be empty for anonymous subscribers).
    var sl: [2]u8 = undefined;
    r.readSliceAll(&sl) catch {
        alloc.free(topic);
        return;
    };
    const sub_id_len = std.mem.readInt(u16, &sl, .big);
    if (sub_id_len > proto.MAX_SUB_ID_LEN) {
        alloc.free(topic);
        return;
    }

    var sub_id_buf: [proto.MAX_SUB_ID_LEN]u8 = undefined;
    const sub_id = sub_id_buf[0..sub_id_len];
    if (sub_id_len > 0) {
        r.readSliceAll(sub_id) catch {
            alloc.free(topic);
            return;
        };
    }

    const has_cursor = proto.isValidSubId(sub_id);

    // Snapshot current WAL tip; replay entries between cursor and tip.
    const tip = ctx.server.wal.currentOffset();
    const cursor_start: u64 = if (has_cursor) ctx.server.cursor.load(sub_id) else 0;

    var write_buf: [16384]u8 = undefined;
    var sw = ctx.stream.writer(ctx.server.io, &write_buf);
    const w = &sw.interface;

    if (has_cursor and cursor_start > 0 and cursor_start < tip) {
        var rc = ReplayCtx{ .w = w, .topic = topic, .fail = false };
        ctx.server.wal.replayFrom(cursor_start, tip, alloc, replayCb, &rc) catch |err| {
            std.log.warn("broker: WAL replay failed for sub={s}: {}", .{ sub_id, err });
        };
        if (rc.fail) {
            alloc.free(topic);
            return;
        }
    }

    // Register with router — new publishes will flow here from this point.
    const sub = alloc.create(topics_mod.Sub) catch {
        alloc.free(topic);
        return;
    };
    sub.* = .{
        .topic          = topic,
        .fd             = ctx.stream.socket.handle,
        .alive          = true,
        .ring           = @import("mpsc.zig").Ring.init(),
        .stopped        = .init(false),
        .deliver_thread = undefined,
    };

    // Spawn the deliver thread BEFORE subscribing so it is ready to drain
    // immediately when the first publish arrives.
    sub.deliver_thread = std.Thread.spawn(.{}, topics_mod.Sub.deliverLoop, .{sub}) catch {
        alloc.free(topic);
        alloc.destroy(sub);
        return;
    };

    ctx.server.router.subscribe(sub) catch {
        sub.stopped.store(true, .release);
        sub.deliver_thread.join();
        alloc.free(topic);
        alloc.destroy(sub);
        return;
    };

    std.log.debug("broker: subscriber registered topic={s} sub_id={s} cursor={d}", .{
        topic, sub_id, cursor_start,
    });

    // Loop: read ACK frames until the client disconnects.
    while (true) {
        var len_bytes: [4]u8 = undefined;
        r.readSliceAll(&len_bytes) catch break;
        const body_len = std.mem.readInt(u32, &len_bytes, .big);
        const frame_type = r.takeByte() catch break;

        if (frame_type == proto.FRAME_ACK) {
            // body: [u64 wal_offset BE] — the after_offset sent in DELIVER
            var offset_bytes: [8]u8 = undefined;
            r.readSliceAll(&offset_bytes) catch break;
            const wal_offset = std.mem.readInt(u64, &offset_bytes, .big);
            if (has_cursor) {
                ctx.server.cursor.store(sub_id, wal_offset);
            }
            std.log.debug("broker: ACK sub={s} wal_offset={d}", .{ sub_id, wal_offset });
        } else {
            const skip_len: usize = if (body_len > 1) body_len - 1 else 0;
            const skip = alloc.alloc(u8, skip_len) catch break;
            defer alloc.free(skip);
            r.readSliceAll(skip) catch break;
        }
    }

    ctx.server.router.unsubscribe(sub);
    alloc.free(topic);
    alloc.destroy(sub);
}
