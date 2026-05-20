const std = @import("std");
const orusshare = @import("orusshare");
const parser = @import("iso8583/parser.zig");
const translator = @import("translator.zig");
const schema_mod = @import("iso8583/schema.zig");
const bank_client_mod = @import("bank_client.zig");
const broker_mod = @import("broker_client.zig");

// BankServer — Direction 2 (Banque → MoMo).
//
// Listens for incoming ISO 8583 connections from bank / switch.
// For each message:
//   1. Parse ISO 8583 with the configured schema.
//   2. Translate to InternalMessage via toInternal().
//   3. Publish to OrusBroker (best-effort — see note below).
//   4. Send a provisional 0210 response (RC=00) immediately.
//
// Note: the provisional ACK means OrusGateway acknowledges receipt without
// waiting for the full MoMo API round-trip. The actual outcome arrives via
// OrusBroker → OrusConnect → MoMo API → back through broker. This is the
// async store-and-forward pattern from CDC §2.2 / §5.5.

pub const Config = struct {
    host: []const u8,
    port: u16,
    length_prefix: bank_client_mod.LengthPrefix = .two_byte_be,
    // OrusBroker topic to publish received transactions on.
    topic: []const u8 = "transactions.financial",
};

pub const BankServer = struct {
    config: Config,
    io: std.Io,
    schema: *const schema_mod.IsoSchema,
    broker: broker_mod.BrokerClient,

    pub fn init(
        config: Config,
        io: std.Io,
        schema: *const schema_mod.IsoSchema,
        broker: broker_mod.BrokerClient,
    ) BankServer {
        return .{ .config = config, .io = io, .schema = schema, .broker = broker };
    }

    // Blocking accept loop — run in a dedicated thread.
    pub fn serve(self: *const BankServer, alloc: std.mem.Allocator) !void {
        const addr = try std.Io.net.IpAddress.parse(self.config.host, self.config.port);
        var server = try addr.listen(self.io, .{ .reuse_address = true });
        defer server.deinit(self.io);

        std.log.info("BankServer listening on {s}:{d} (schema={s})", .{
            self.config.host, self.config.port, self.schema.id,
        });

        while (true) {
            const stream = server.accept(self.io) catch |err| {
                if (err == error.SocketNotListening) break;
                std.log.warn("bank_server: accept error: {}", .{err});
                continue;
            };

            const ctx = alloc.create(ConnCtx) catch {
                stream.close(self.io);
                continue;
            };
            ctx.* = .{ .stream = stream, .server = self, .alloc = alloc };

            const t = std.Thread.spawn(.{}, handleConn, .{ctx}) catch |err| {
                std.log.err("bank_server: thread spawn failed: {}", .{err});
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
    server: *const BankServer,
    alloc: std.mem.Allocator,
};

fn handleConn(ctx: *ConnCtx) void {
    defer ctx.stream.close(ctx.server.io);
    defer ctx.alloc.destroy(ctx);

    const s = ctx.server;
    const alloc = ctx.alloc;
    const io = s.io;

    var rbuf: [8192]u8 = undefined;
    var sr = ctx.stream.reader(io, &rbuf);
    var r = &sr.interface;

    var wbuf: [8192]u8 = undefined;
    var sw = ctx.stream.writer(io, &wbuf);
    var w = &sw.interface;

    // ── Read ISO 8583 frame ────────────────────────────────────────────────────
    const iso_len = readLenPrefix(r, s.config.length_prefix) catch return;
    if (iso_len == 0 or iso_len > 65_535) return;

    const iso_buf = alloc.alloc(u8, iso_len) catch return;
    defer alloc.free(iso_buf);
    r.readSliceAll(iso_buf) catch return;

    // ── Parse ─────────────────────────────────────────────────────────────────
    var iso_msg = parser.parseWithSchema(iso_buf, s.schema, alloc) catch |err| {
        std.log.warn("bank_server: ISO 8583 parse failed: {}", .{err});
        return;
    };
    defer iso_msg.deinit();

    // ── toInternal ────────────────────────────────────────────────────────────
    var msg_id: [16]u8 = undefined;
    std.c.arc4random_buf(&msg_id, msg_id.len);
    const now_ns: i64 = @intCast(std.Io.Clock.real.now(io).nanoseconds);

    const base = orusshare.InternalMessage{
        .msg_id      = msg_id,
        .schema_id   = s.schema.id,
        .topic       = s.config.topic,
        .origin      = .iso8583,
        .provider    = .none,
        .mti         = iso_msg.mti,
        .fields      = &.{},
        .stan        = [_]u8{0} ** 6,
        .pan_hash    = 0,
        .amount      = 0,
        .currency    = [_]u8{0} ** 3,
        .received_at = now_ns,
        .source_ip   = [_]u8{0} ** 16,
        .hop_count   = 1,
        .external_id = null,
    };

    const im = translator.toInternal(&iso_msg, base, alloc) catch |err| {
        std.log.warn("bank_server: toInternal failed: {}", .{err});
        return;
    };
    defer {
        for (im.fields) |f| alloc.free(f.value);
        alloc.free(im.fields);
    }

    // ── Publish to broker ──────────────────────────────────────────────────────
    s.broker.publish(&im, alloc) catch |err| {
        std.log.warn("bank_server: broker publish failed: {} (sending provisional ACK anyway)", .{err});
    };

    // ── Provisional 0210 response (RC=00 = received) ──────────────────────────
    var resp = parser.IsoMessage.init(alloc, "0210".*);
    defer resp.deinit();
    resp.set(39, "00") catch return;
    if (iso_msg.get(11)) |stan| resp.set(11, stan) catch {};

    const resp_bytes = parser.serializeWithSchema(&resp, s.schema, alloc) catch return;
    defer alloc.free(resp_bytes);

    writeLenPrefix(w, resp_bytes.len, s.config.length_prefix) catch return;
    w.writeAll(resp_bytes) catch return;
    w.flush() catch return;
}

fn readLenPrefix(r: *std.Io.Reader, prefix: bank_client_mod.LengthPrefix) !usize {
    switch (prefix) {
        .two_byte_be => {
            var b: [2]u8 = undefined;
            try r.readSliceAll(&b);
            return std.mem.readInt(u16, &b, .big);
        },
        .four_byte_be => {
            var b: [4]u8 = undefined;
            try r.readSliceAll(&b);
            return std.mem.readInt(u32, &b, .big);
        },
    }
}

fn writeLenPrefix(w: *std.Io.Writer, len: usize, prefix: bank_client_mod.LengthPrefix) !void {
    switch (prefix) {
        .two_byte_be  => try w.writeInt(u16, @intCast(len), .big),
        .four_byte_be => try w.writeInt(u32, @intCast(len), .big),
    }
}
