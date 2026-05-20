const std = @import("std");
const orusshare = @import("orusshare");
const serialize = orusshare.serialize;

// Minimal broker client for OrusGateway Direction 2.
// Only publish is needed: gateway translates ISO 8583 → InternalMessage → broker.

pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 7770,
};

const FRAME_PUBLISH: u8 = 0x01;

pub const BrokerClientError = error{ BrokerUnavailable, BrokerNack, OutOfMemory };

pub const BrokerClient = struct {
    config: Config,
    io: std.Io,

    pub fn init(config: Config, io: std.Io) BrokerClient {
        return .{ .config = config, .io = io };
    }

    // Publish one InternalMessage to OrusBroker.
    // Connect-per-publish (V1 — connection pooling deferred to broker milestone).
    pub fn publish(
        self: *const BrokerClient,
        msg:  *const orusshare.InternalMessage,
        alloc: std.mem.Allocator,
    ) BrokerClientError!void {
        const addr = std.Io.net.IpAddress.parse(self.config.host, self.config.port) catch
            return error.BrokerUnavailable;
        var stream = addr.connect(self.io, .{ .mode = .stream }) catch
            return error.BrokerUnavailable;
        defer stream.close(self.io);

        // Serialize InternalMessage into a temporary buffer.
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

        // Wait for PUBLISH_ACK (single byte 0x06)
        var read_buf: [16]u8 = undefined;
        var sr = stream.reader(self.io, &read_buf);
        const ack = sr.interface.takeByte() catch return error.BrokerUnavailable;
        if (ack != 0x06) return error.BrokerNack;
    }
};
