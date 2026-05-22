const std = @import("std");
const orusshare = @import("orusshare");
const serialize = orusshare.serialize;

pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 7770,
    connect_timeout_ms: u32 = 5_000,
};

// Wire protocol — all frames:
//   [u32 BE] body_len  (excludes this 4-byte header)
//   [u8]     frame_type
//   ... frame-specific fields ...
//
// Client → Broker:
//   PUBLISH   (0x01): [u16 topic_len][topic][serialized InternalMessage]
//   SUBSCRIBE (0x02): [u16 topic_len][topic][u16 sub_id_len][sub_id]
//   ACK       (0x04): [u64 wal_offset BE]
//
// Broker → Client:
//   DELIVER     (0x03): [u16 topic_len][topic][u64 wal_offset BE][serialized InternalMessage]
//   PUBLISH_ACK (0x06): single byte (no body_len prefix — legacy)
const FRAME_PUBLISH: u8 = 0x01;
const FRAME_SUBSCRIBE: u8 = 0x02;
const FRAME_DELIVER: u8 = 0x03;
const FRAME_ACK: u8 = 0x04;

pub const ConsumeCallback = *const fn (
    msg: *const orusshare.InternalMessage,
    ctx: ?*anyopaque,
) void;

pub const BrokerClient = struct {
    config: Config,
    io: std.Io,

    pub fn init(config: Config, io: std.Io) BrokerClient {
        return .{ .config = config, .io = io };
    }

    pub fn publish(self: *const BrokerClient, msg: *const orusshare.InternalMessage) !void {
        const addr = std.Io.net.IpAddress.parse(self.config.host, self.config.port) catch
            return error.BrokerUnavailable;

        var stream = addr.connect(self.io, .{ .mode = .stream }) catch return error.BrokerUnavailable;
        defer stream.close(self.io);

        var write_buf: [8192]u8 = undefined;
        var sw = stream.writer(self.io, &write_buf);
        var w = &sw.interface;

        var payload_list: std.ArrayList(u8) = .empty;
        try payload_list.ensureTotalCapacity(std.heap.page_allocator, 4096);
        var pw = std.Io.Writer.fromArrayList(&payload_list);
        serialize.serialize(msg, &pw) catch return error.BrokerUnavailable;
        var payload_al = std.Io.Writer.toArrayList(&pw);
        defer payload_al.deinit(std.heap.page_allocator);
        const payload = payload_al.items;

        const topic_len: u16 = @intCast(msg.topic.len);
        const frame_len: u32 = @intCast(1 + 2 + msg.topic.len + payload.len);

        try w.writeInt(u32, frame_len, .big);
        try w.writeByte(FRAME_PUBLISH);
        try w.writeInt(u16, topic_len, .big);
        try w.writeAll(msg.topic);
        try w.writeAll(payload);
        try sw.interface.flush();

        var read_buf: [16]u8 = undefined;
        var sr = stream.reader(self.io, &read_buf);
        const ack = sr.interface.takeByte() catch return error.BrokerUnavailable;
        if (ack != 0x06) return error.BrokerNack;
    }

    // Blocking consume loop. Opens a persistent connection, subscribes to
    // `topic`, and calls `callback` for every delivered message.
    // Sends ACK after each callback. Returns on any connection error.
    // Caller runs this in a dedicated thread; reconnect logic lives outside.
    pub fn consume(
        self: *const BrokerClient,
        topic: []const u8,
        alloc: std.mem.Allocator,
        callback: ConsumeCallback,
        ctx: ?*anyopaque,
    ) BrokerClientError!void {
        const addr = std.Io.net.IpAddress.parse(self.config.host, self.config.port) catch
            return error.BrokerUnavailable;
        var stream = addr.connect(self.io, .{ .mode = .stream }) catch
            return error.BrokerUnavailable;
        defer stream.close(self.io);

        var write_buf: [256]u8 = undefined;
        var sw = stream.writer(self.io, &write_buf);

        // SUBSCRIBE frame — anonymous (sub_id_len = 0, no cursor replay)
        const sub_len: u32 = @intCast(1 + 2 + topic.len + 2);
        sw.interface.writeInt(u32, sub_len, .big) catch return error.BrokerUnavailable;
        sw.interface.writeByte(FRAME_SUBSCRIBE) catch return error.BrokerUnavailable;
        sw.interface.writeInt(u16, @intCast(topic.len), .big) catch return error.BrokerUnavailable;
        sw.interface.writeAll(topic) catch return error.BrokerUnavailable;
        sw.interface.writeInt(u16, 0, .big) catch return error.BrokerUnavailable; // sub_id_len = 0
        sw.interface.flush() catch return error.BrokerUnavailable;

        var read_buf: [16384]u8 = undefined;
        var sr = stream.reader(self.io, &read_buf);

        while (true) {
            var len_bytes: [4]u8 = undefined;
            sr.interface.readSliceAll(&len_bytes) catch return error.BrokerUnavailable;
            const body_len = std.mem.readInt(u32, &len_bytes, .big);

            const frame_type = sr.interface.takeByte() catch return error.BrokerUnavailable;

            if (frame_type != FRAME_DELIVER) {
                // Drain unknown frame body (body_len includes the frame_type byte already read)
                const remaining: usize = if (body_len > 1) body_len - 1 else 0;
                const skip = alloc.alloc(u8, remaining) catch return error.BrokerUnavailable;
                defer alloc.free(skip);
                sr.interface.readSliceAll(skip) catch return error.BrokerUnavailable;
                continue;
            }

            // Parse DELIVER body: [u16 topic_len][topic][u64 wal_offset][serialized InternalMessage]
            var tl_bytes: [2]u8 = undefined;
            sr.interface.readSliceAll(&tl_bytes) catch return error.BrokerUnavailable;
            const topic_len_val = std.mem.readInt(u16, &tl_bytes, .big);

            const topic_buf = alloc.alloc(u8, topic_len_val) catch return error.BrokerUnavailable;
            defer alloc.free(topic_buf);
            sr.interface.readSliceAll(topic_buf) catch return error.BrokerUnavailable;

            var offset_bytes: [8]u8 = undefined;
            sr.interface.readSliceAll(&offset_bytes) catch return error.BrokerUnavailable;
            const wal_offset = std.mem.readInt(u64, &offset_bytes, .big);

            const hdr: u32 = 1 + 2 + topic_len_val + 8;
            if (body_len < hdr) return error.BrokerUnavailable;
            const payload_len: usize = body_len - hdr;
            const payload = alloc.alloc(u8, payload_len) catch return error.BrokerUnavailable;
            defer alloc.free(payload);
            sr.interface.readSliceAll(payload) catch return error.BrokerUnavailable;

            var fixed_r = std.Io.Reader.fixed(payload);
            const msg = serialize.deserialize(&fixed_r, alloc) catch return error.BrokerUnavailable;
            defer serialize.free(&msg, alloc);

            callback(&msg, ctx);

            // ACK frame: body = [u8 0x04][u64 wal_offset BE], body_len = 9
            sw.interface.writeInt(u32, 9, .big) catch return error.BrokerUnavailable;
            sw.interface.writeByte(FRAME_ACK) catch return error.BrokerUnavailable;
            sw.interface.writeInt(u64, wal_offset, .big) catch return error.BrokerUnavailable;
            sw.interface.flush() catch return error.BrokerUnavailable;
        }
    }
};

// Publisher — connexion TCP persistante réutilisée pour tous les messages.
// Une instance par thread publisher. Reconnexion automatique sur erreur.
pub const Publisher = struct {
    config: Config,
    io:     std.Io,
    stream: ?std.Io.net.Stream,

    pub fn init(config: Config, io: std.Io) Publisher {
        return .{ .config = config, .io = io, .stream = null };
    }

    pub fn deinit(self: *Publisher) void {
        if (self.stream) |s| s.close(self.io);
        self.stream = null;
    }

    fn connect(self: *Publisher) !void {
        const addr = std.Io.net.IpAddress.parse(self.config.host, self.config.port) catch
            return error.BrokerUnavailable;
        self.stream = addr.connect(self.io, .{ .mode = .stream }) catch
            return error.BrokerUnavailable;
    }

    pub fn publish(self: *Publisher, msg: *const orusshare.InternalMessage) !void {
        if (self.stream == null) try self.connect();
        self.doPublish(msg) catch {
            // Connexion périmée : fermer, reconnecter, réessayer une fois.
            if (self.stream) |s| { s.close(self.io); self.stream = null; }
            try self.connect();
            try self.doPublish(msg);
        };
    }

    fn doPublish(self: *Publisher, msg: *const orusshare.InternalMessage) !void {
        const stream = self.stream orelse return error.BrokerUnavailable;

        var payload_list: std.ArrayList(u8) = .empty;
        try payload_list.ensureTotalCapacity(std.heap.page_allocator, 4096);
        var pw = std.Io.Writer.fromArrayList(&payload_list);
        serialize.serialize(msg, &pw) catch return error.BrokerUnavailable;
        var payload_al = std.Io.Writer.toArrayList(&pw);
        defer payload_al.deinit(std.heap.page_allocator);
        const payload = payload_al.items;

        const topic_len: u16 = @intCast(msg.topic.len);
        const frame_len: u32 = @intCast(1 + 2 + msg.topic.len + payload.len);

        var write_buf: [8192]u8 = undefined;
        var sw = stream.writer(self.io, &write_buf);
        const w = &sw.interface;
        w.writeInt(u32, frame_len, .big) catch return error.BrokerUnavailable;
        w.writeByte(FRAME_PUBLISH)       catch return error.BrokerUnavailable;
        w.writeInt(u16, topic_len, .big) catch return error.BrokerUnavailable;
        w.writeAll(msg.topic)            catch return error.BrokerUnavailable;
        w.writeAll(payload)              catch return error.BrokerUnavailable;
        w.flush()                        catch return error.BrokerUnavailable;

        var read_buf: [16]u8 = undefined;
        var sr = stream.reader(self.io, &read_buf);
        const ack = sr.interface.takeByte() catch return error.BrokerUnavailable;
        if (ack != 0x06) return error.BrokerNack;
    }
};

pub const BrokerClientError = error{
    BrokerUnavailable,
    BrokerNack,
};
