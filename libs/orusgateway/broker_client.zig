const std = @import("std");
const orusshare = @import("orusshare");
const serialize = orusshare.serialize;

// Thin spin-mutex — same approach as WAL/orusbroker, avoids std.Io.Mutex
// which requires being called from within the Io coroutine scheduler.
const SpinMutex = struct {
    state: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),
    pub fn lock(self: *SpinMutex) void {
        while (self.state.cmpxchgWeak(0, 1, .acquire, .monotonic) != null) {}
    }
    pub fn unlock(self: *SpinMutex) void {
        self.state.store(0, .release);
    }
};

// Broker client for OrusGateway — Direction 2 (publish) and Direction 1 (consume).
//
// publish() uses a persistent TCP connection (reuse + reconnect-once on failure),
// the same pattern as BankClient. This eliminates connect-per-publish overhead
// and allows 9 000+ publish/s on loopback without saturating the TCP stack.

pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 7770,
};

const FRAME_PUBLISH: u8 = 0x01;
const FRAME_SUBSCRIBE: u8 = 0x02;
const FRAME_DELIVER: u8 = 0x03;
const FRAME_ACK: u8 = 0x04;

pub const ConsumeCallback = *const fn (
    msg: *const orusshare.InternalMessage,
    ctx: ?*anyopaque,
) void;

pub const BrokerClientError = error{ BrokerUnavailable, BrokerNack, OutOfMemory };

pub const BrokerClient = struct {
    config: Config,
    io:     std.Io,
    stream: ?std.Io.net.Stream = null,
    mu:     SpinMutex = .{},

    pub fn init(config: Config, io: std.Io) BrokerClient {
        return .{ .config = config, .io = io };
    }

    pub fn deinit(self: *BrokerClient) void {
        self.mu.lock();
        defer self.mu.unlock();
        if (self.stream) |s| {
            s.close(self.io);
            self.stream = null;
        }
    }

    // Publish one InternalMessage to OrusBroker.
    // Reuses the TCP connection across calls; reconnects once on failure.
    // Thread-safe: mu serialises the request+response exchange on the shared stream.
    pub fn publish(
        self:  *BrokerClient,
        msg:   *const orusshare.InternalMessage,
        alloc: std.mem.Allocator,
    ) BrokerClientError!void {
        // Serialize outside the connection retry loop — no need to redo on reconnect.
        var payload_list: std.ArrayList(u8) = .empty;
        payload_list.ensureTotalCapacity(alloc, 4096) catch return error.OutOfMemory;
        var pw = std.Io.Writer.fromArrayList(&payload_list);
        serialize.serialize(msg, &pw) catch {
            var leftover = std.Io.Writer.toArrayList(&pw);
            leftover.deinit(alloc);
            return error.BrokerUnavailable;
        };
        var payload_al = std.Io.Writer.toArrayList(&pw);
        defer payload_al.deinit(alloc);
        const payload = payload_al.items;

        self.mu.lock();
        defer self.mu.unlock();

        var attempt: u2 = 0;
        while (attempt < 2) : (attempt += 1) {
            if (self.stream == null) {
                const addr = std.Io.net.IpAddress.parse(self.config.host, self.config.port) catch
                    return error.BrokerUnavailable;
                self.stream = addr.connect(self.io, .{ .mode = .stream }) catch
                    return error.BrokerUnavailable;
            }
            if (self.doPublish(self.stream.?, msg, payload)) {
                return;
            } else |_| {
                self.stream.?.close(self.io);
                self.stream = null;
            }
        }
        return error.BrokerUnavailable;
    }

    fn doPublish(
        self:    *const BrokerClient,
        stream:  std.Io.net.Stream,
        msg:     *const orusshare.InternalMessage,
        payload: []const u8,
    ) BrokerClientError!void {
        const topic_len: u16 = @intCast(msg.topic.len);
        const frame_body: u32 = @intCast(1 + 2 + msg.topic.len + payload.len);

        var write_buf: [8192]u8 = undefined;
        var sw = stream.writer(self.io, &write_buf);
        var w = &sw.interface;

        w.writeInt(u32, frame_body, .big) catch return error.BrokerUnavailable;
        w.writeByte(FRAME_PUBLISH) catch return error.BrokerUnavailable;
        w.writeInt(u16, topic_len, .big) catch return error.BrokerUnavailable;
        w.writeAll(msg.topic) catch return error.BrokerUnavailable;
        w.writeAll(payload) catch return error.BrokerUnavailable;
        w.flush() catch return error.BrokerUnavailable;

        var read_buf: [16]u8 = undefined;
        var sr = stream.reader(self.io, &read_buf);
        const ack = sr.interface.takeByte() catch return error.BrokerUnavailable;
        if (ack != 0x06) return error.BrokerNack;
    }

    // Blocking consume loop. Subscribes to `topic` and calls `callback` for every
    // delivered message, then ACKs. Returns on any connection error; caller retries.
    pub fn consume(
        self:     *const BrokerClient,
        topic:    []const u8,
        alloc:    std.mem.Allocator,
        callback: ConsumeCallback,
        user_ctx: ?*anyopaque,
    ) BrokerClientError!void {
        const addr = std.Io.net.IpAddress.parse(self.config.host, self.config.port) catch
            return error.BrokerUnavailable;
        var stream = addr.connect(self.io, .{ .mode = .stream }) catch
            return error.BrokerUnavailable;
        defer stream.close(self.io);

        var write_buf: [256]u8 = undefined;
        var sw = stream.writer(self.io, &write_buf);

        const sub_body: u32 = @intCast(1 + 2 + topic.len + 2);
        sw.interface.writeInt(u32, sub_body, .big) catch return error.BrokerUnavailable;
        sw.interface.writeByte(FRAME_SUBSCRIBE) catch return error.BrokerUnavailable;
        sw.interface.writeInt(u16, @intCast(topic.len), .big) catch return error.BrokerUnavailable;
        sw.interface.writeAll(topic) catch return error.BrokerUnavailable;
        sw.interface.writeInt(u16, 0, .big) catch return error.BrokerUnavailable;
        sw.interface.flush() catch return error.BrokerUnavailable;

        var read_buf: [16384]u8 = undefined;
        var sr = stream.reader(self.io, &read_buf);

        while (true) {
            var len_bytes: [4]u8 = undefined;
            sr.interface.readSliceAll(&len_bytes) catch return error.BrokerUnavailable;
            const body_len = std.mem.readInt(u32, &len_bytes, .big);

            const frame_type = sr.interface.takeByte() catch return error.BrokerUnavailable;

            if (frame_type != FRAME_DELIVER) {
                const remaining: usize = if (body_len > 1) body_len - 1 else 0;
                const skip = alloc.alloc(u8, remaining) catch return error.BrokerUnavailable;
                defer alloc.free(skip);
                sr.interface.readSliceAll(skip) catch return error.BrokerUnavailable;
                continue;
            }

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

            callback(&msg, user_ctx);

            sw.interface.writeInt(u32, 9, .big) catch return error.BrokerUnavailable;
            sw.interface.writeByte(FRAME_ACK) catch return error.BrokerUnavailable;
            sw.interface.writeInt(u64, wal_offset, .big) catch return error.BrokerUnavailable;
            sw.interface.flush() catch return error.BrokerUnavailable;
        }
    }
};
