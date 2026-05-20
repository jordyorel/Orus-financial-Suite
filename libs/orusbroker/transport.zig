// BrokerServer — TCP accept loop + per-connection handler.
//
// Connection lifecycle:
//   1. Accept TCP connection.
//   2. Read the first frame header → determine PUBLISH or SUBSCRIBE.
//   3a. PUBLISH: dedup → WAL append → fan-out DELIVER → send PUBLISH_ACK.
//       Connection closes after the single exchange.
//   3b. SUBSCRIBE: register Sub → loop reading ACK frames until disconnect.
//       Publisher threads push DELIVER frames to the stream concurrently.

const std      = @import("std");
const orusshare = @import("orusshare");
const serialize = orusshare.serialize;
const proto    = @import("protocol.zig");
const wal_mod  = @import("wal.zig");
const dedup_mod = @import("dedup.zig");
const topics_mod = @import("topics.zig");

pub const Config = struct {
    host:     []const u8 = "0.0.0.0",
    port:     u16        = 7770,
    wal_path: []const u8 = "orus_broker.wal",
};

pub const BrokerServer = struct {
    config: Config,
    io:     std.Io,
    wal:    *wal_mod.Wal,
    dedup:  *dedup_mod.DedupFilter,
    router: *topics_mod.TopicRouter,

    // Blocking accept loop — runs until the listener is closed.
    pub fn serve(self: *const BrokerServer, alloc: std.mem.Allocator) !void {
        const addr = try std.Io.net.IpAddress.parse(self.config.host, self.config.port);
        var server = try addr.listen(self.io, .{ .reuse_address = true });
        defer server.deinit(self.io);

        std.log.info("OrusBroker listening on {s}:{d}  wal={s}", .{
            self.config.host, self.config.port, self.config.wal_path,
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
    // Defers execute LIFO: destroy(ctx) must come AFTER stream.close to avoid use-after-free.
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
        proto.FRAME_PUBLISH   => handlePublish(ctx, r, remaining),
        proto.FRAME_SUBSCRIBE => handleSubscribe(ctx, r, remaining),
        else => {},
    }
}

fn handlePublish(ctx: *ConnCtx, r: *std.Io.Reader, remaining: u32) void {
    const alloc = ctx.alloc;

    // [u16 topic_len][topic][IM payload]
    var tl: [2]u8 = undefined;
    r.readSliceAll(&tl) catch return;
    const topic_len = std.mem.readInt(u16, &tl, .big);
    if (topic_len > proto.MAX_TOPIC_LEN) return;

    const topic = alloc.alloc(u8, topic_len) catch return;
    defer alloc.free(topic);
    r.readSliceAll(topic) catch return;

    const payload_len: usize = remaining -| (2 + topic_len);
    const payload = alloc.alloc(u8, payload_len) catch return;
    defer alloc.free(payload);
    r.readSliceAll(payload) catch return;

    // Deserialize only to extract msg_id for dedup.
    var fixed_r = std.Io.Reader.fixed(payload);
    const msg = serialize.deserialize(&fixed_r, alloc) catch {
        std.log.warn("broker: publish: deserialize failed", .{});
        sendPublishAck(ctx);
        return;
    };
    defer serialize.free(&msg, alloc);

    if (ctx.server.dedup.checkAndSet(msg.msg_id)) {
        std.log.debug("broker: duplicate msg_id discarded", .{});
    } else {
        ctx.server.wal.append(payload) catch |err| {
            std.log.warn("broker: WAL append failed: {} (delivering anyway)", .{err});
        };
        ctx.server.router.deliver(topic, payload);
    }

    sendPublishAck(ctx);
}

fn sendPublishAck(ctx: *ConnCtx) void {
    var wbuf: [8]u8 = undefined;
    var sw = ctx.stream.writer(ctx.server.io, &wbuf);
    sw.interface.writeByte(proto.FRAME_PUBLISH_ACK) catch return;
    sw.interface.flush() catch {};
}

fn handleSubscribe(ctx: *ConnCtx, r: *std.Io.Reader, remaining: u32) void {
    const alloc = ctx.alloc;

    // [u16 topic_len][topic]
    var tl: [2]u8 = undefined;
    r.readSliceAll(&tl) catch return;
    const topic_len = std.mem.readInt(u16, &tl, .big);
    if (topic_len > proto.MAX_TOPIC_LEN) return;
    _ = remaining;

    const topic = alloc.alloc(u8, topic_len) catch return;
    r.readSliceAll(topic) catch {
        alloc.free(topic);
        return;
    };

    const sub = alloc.create(topics_mod.Sub) catch {
        alloc.free(topic);
        return;
    };
    sub.* = .{
        .topic  = topic,
        .stream = ctx.stream,
        .io     = ctx.server.io,
        .alive  = true,
    };

    ctx.server.router.subscribe(sub) catch {
        alloc.free(topic);
        alloc.destroy(sub);
        return;
    };

    std.log.debug("broker: subscriber registered topic={s}", .{topic});

    // Loop: read ACK frames until the client disconnects.
    // DELIVER frames are pushed to this stream by publisher threads
    // (via router.deliver) concurrently — TCP full-duplex keeps reads
    // and writes independent at the OS level.
    while (true) {
        var len_bytes: [4]u8 = undefined;
        r.readSliceAll(&len_bytes) catch break;
        const body_len = std.mem.readInt(u32, &len_bytes, .big);
        const frame_type = r.takeByte() catch break;

        if (frame_type == proto.FRAME_ACK) {
            var msg_id: [16]u8 = undefined;
            r.readSliceAll(&msg_id) catch break;
            // V1: ACKs are logged; WAL compaction / exactly-once cursor
            // tracking is deferred to the M4 milestone.
            std.log.debug("broker: ACK msg_id={s}", .{
                std.fmt.bytesToHex(msg_id, .lower),
            });
        } else {
            // Drain unknown frame body (minus the byte already consumed).
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
