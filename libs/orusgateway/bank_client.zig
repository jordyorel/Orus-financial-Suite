const std = @import("std");
const parser = @import("iso8583/parser.zig");

pub const IsoMessage = parser.IsoMessage;

// Most African bank networks (GIMAC, BEAC/BCEAO) use a 2-byte big-endian
// Message Length Indicator (MLI). Some acquiring hosts use 4 bytes.
pub const LengthPrefix = enum { two_byte_be, four_byte_be };

pub const TlsMode = enum {
    none,
    // TLS with self-signed certificate verification — for private bank networks
    // (GIMAC, BEAC) that issue their own CA. Does not accept public CA chains.
    self_signed,
};

pub const Config = struct {
    host: []const u8,
    port: u16,
    length_prefix: LengthPrefix = .two_byte_be,
    // Hard ceiling on inbound message size (prevents memory exhaustion).
    max_response_bytes: usize = 8192,
    tls: TlsMode = .none,
};

pub const BankClientError = error{
    BankUnreachable,
    ResponseTooLarge,
    MalformedResponse,
};

// Persistent-connection client — reuses the TCP stream across requests and
// reconnects automatically on failure (up to 2 attempts per call).
// TLS mode keeps connect-per-request because a TLS session is connection-bound.
pub const BankClient = struct {
    config: Config,
    io: std.Io,
    stream: ?std.Io.net.Stream = null,

    pub fn init(config: Config, io: std.Io) BankClient {
        return .{ .config = config, .io = io };
    }

    pub fn deinit(self: *BankClient) void {
        if (self.stream) |s| {
            s.close(self.io);
            self.stream = null;
        }
    }

    // Send msg to the bank and return the response IsoMessage.
    // The returned IsoMessage owns its arena; caller must call .deinit().
    pub fn send(
        self: *BankClient,
        msg: *const IsoMessage,
        alloc: std.mem.Allocator,
    ) BankClientError!IsoMessage {
        const payload = parser.serialize(msg, alloc) catch return error.BankUnreachable;
        defer alloc.free(payload);
        return self.sendBytes(payload, alloc);
    }

    // Like send() but accepts pre-serialized wire bytes — avoids re-serialization
    // when the caller has already built the bytes (e.g. after MAC computation).
    pub fn sendBytes(
        self:      *BankClient,
        req_bytes: []const u8,
        alloc:     std.mem.Allocator,
    ) BankClientError!IsoMessage {
        switch (self.config.tls) {
            // TLS is connection-bound: keep connect-per-request.
            .self_signed => {
                const addr = std.Io.net.IpAddress.parse(self.config.host, self.config.port) catch
                    return error.BankUnreachable;
                var stream = addr.connect(self.io, .{ .mode = .stream }) catch return error.BankUnreachable;
                defer stream.close(self.io);

                var read_buf: [8192]u8 = undefined;
                var write_buf: [8192]u8 = undefined;
                var sr = stream.reader(self.io, &read_buf);
                var sw = stream.writer(self.io, &write_buf);

                var tls_read: [std.crypto.tls.Client.min_buffer_len]u8 = undefined;
                var tls_write: [std.crypto.tls.Client.min_buffer_len]u8 = undefined;
                var entropy: [std.crypto.tls.Client.Options.entropy_len]u8 = undefined;
                std.c.arc4random_buf(&entropy, entropy.len);
                var tls = std.crypto.tls.Client.init(&sr.interface, &sw.interface, .{
                    .host     = .{ .explicit = self.config.host },
                    .ca       = .self_signed,
                    .write_buffer  = &tls_write,
                    .read_buffer   = &tls_read,
                    .entropy       = &entropy,
                    .realtime_now  = std.Io.Clock.real.now(self.io),
                }) catch return error.BankUnreachable;
                defer tls.end() catch {};
                return self.doExchangeRaw(&tls.reader, &tls.writer, req_bytes, alloc);
            },
            // Persistent TCP: reuse stream, reconnect once on error.
            .none => {
                var attempt: u2 = 0;
                while (attempt < 2) : (attempt += 1) {
                    if (self.stream == null) {
                        const addr = std.Io.net.IpAddress.parse(self.config.host, self.config.port) catch
                            return error.BankUnreachable;
                        self.stream = addr.connect(self.io, .{ .mode = .stream }) catch
                            return error.BankUnreachable;
                    }
                    var read_buf: [8192]u8 = undefined;
                    var write_buf: [8192]u8 = undefined;
                    var sr = self.stream.?.reader(self.io, &read_buf);
                    var sw = self.stream.?.writer(self.io, &write_buf);
                    if (self.doExchangeRaw(&sr.interface, &sw.interface, req_bytes, alloc)) |result| {
                        return result;
                    } else |_| {
                        self.stream.?.close(self.io);
                        self.stream = null;
                    }
                }
                return error.BankUnreachable;
            },
        }
    }

    fn doExchangeRaw(
        self:      *const BankClient,
        r:         *std.Io.Reader,
        w:         *std.Io.Writer,
        req_bytes: []const u8,
        alloc:     std.mem.Allocator,
    ) BankClientError!IsoMessage {
        writeLengthPrefix(w, req_bytes.len, self.config.length_prefix) catch
            return error.BankUnreachable;
        w.writeAll(req_bytes) catch return error.BankUnreachable;
        w.flush() catch return error.BankUnreachable;

        const resp_len = readLengthPrefix(r, self.config.length_prefix) catch
            return error.MalformedResponse;
        if (resp_len > self.config.max_response_bytes) return error.ResponseTooLarge;
        if (resp_len < 4) return error.MalformedResponse;

        const resp_buf = alloc.alloc(u8, resp_len) catch return error.BankUnreachable;
        defer alloc.free(resp_buf);
        r.readSliceAll(resp_buf) catch return error.MalformedResponse;

        return parser.parse(resp_buf, alloc) catch return error.MalformedResponse;
    }
};

fn writeLengthPrefix(
    w: *std.Io.Writer,
    len: usize,
    prefix: LengthPrefix,
) std.Io.Writer.Error!void {
    switch (prefix) {
        .two_byte_be => try w.writeInt(u16, @intCast(len), .big),
        .four_byte_be => try w.writeInt(u32, @intCast(len), .big),
    }
}

fn readLengthPrefix(
    r: *std.Io.Reader,
    prefix: LengthPrefix,
) (std.Io.Reader.Error || error{EndOfStream})!usize {
    switch (prefix) {
        .two_byte_be => {
            var buf: [2]u8 = undefined;
            try r.readSliceAll(&buf);
            return std.mem.readInt(u16, &buf, .big);
        },
        .four_byte_be => {
            var buf: [4]u8 = undefined;
            try r.readSliceAll(&buf);
            return std.mem.readInt(u32, &buf, .big);
        },
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// Tests use a fake TCP server that accepts one connection, reads the framed
// request, and writes back a framed ISO 8583 response.

const testing = std.testing;

fn serveOnce(
    server: *std.Io.net.Server,
    io: std.Io,
    response_bytes: []const u8,
    prefix: LengthPrefix,
    alloc: std.mem.Allocator,
) void {
    var stream = server.accept(io) catch return;
    defer stream.close(io);

    // Drain the inbound request (length-prefixed)
    var rbuf: [8192]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    var r = &sr.interface;
    const req_len = readLengthPrefix(r, prefix) catch return;
    if (req_len > rbuf.len) return;
    const tmp = alloc.alloc(u8, req_len) catch return;
    defer alloc.free(tmp);
    r.readSliceAll(tmp) catch return;

    // Send back the framed response
    var wbuf: [8192]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    var w = &sw.interface;
    writeLengthPrefix(w, response_bytes.len, prefix) catch return;
    w.writeAll(response_bytes) catch return;
    w.flush() catch return;
}

fn buildResponse0210(alloc: std.mem.Allocator) ![]u8 {
    var resp = IsoMessage.init(alloc, "0210".*);
    defer resp.deinit();
    try resp.set(39, "00"); // response code: approved
    try resp.set(11, "000001"); // STAN
    try resp.set(49, "XAF"); // currency
    return parser.serialize(&resp, alloc);
}

// Fixed loopback ports for integration tests. Distinct to avoid cross-test
// interference if the OS keeps sockets in TIME_WAIT briefly.
const TEST_PORT_2BYTE: u16 = 19230;
const TEST_PORT_4BYTE: u16 = 19231;
const TEST_PORT_TOOLARGE: u16 = 19232;

test "BankClient: 2-byte prefix round-trip" {
    const alloc = testing.allocator;

    const resp_bytes = try buildResponse0210(alloc);
    defer alloc.free(resp_bytes);

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", TEST_PORT_2BYTE);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    const thread = try std.Thread.spawn(.{}, serveOnce, .{
        &server, io, resp_bytes, .two_byte_be, alloc,
    });
    defer thread.join();

    var req = IsoMessage.init(alloc, "0200".*);
    defer req.deinit();
    try req.set(11, "000001");
    try req.set(4, "000000010000");
    try req.set(49, "XAF");

    var client = BankClient.init(.{
        .host = "127.0.0.1",
        .port = TEST_PORT_2BYTE,
        .length_prefix = .two_byte_be,
    }, io);

    var resp = try client.send(&req, alloc);
    defer resp.deinit();

    try testing.expectEqualSlices(u8, "0210", &resp.mti);
    try testing.expectEqualStrings("00", resp.get(39).?);
    try testing.expectEqualStrings("000001", resp.get(11).?);
    try testing.expectEqualStrings("XAF", resp.get(49).?);
}

test "BankClient: 4-byte prefix round-trip" {
    const alloc = testing.allocator;

    const resp_bytes = try buildResponse0210(alloc);
    defer alloc.free(resp_bytes);

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", TEST_PORT_4BYTE);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    const thread = try std.Thread.spawn(.{}, serveOnce, .{
        &server, io, resp_bytes, .four_byte_be, alloc,
    });
    defer thread.join();

    var req = IsoMessage.init(alloc, "0200".*);
    defer req.deinit();
    try req.set(11, "000002");
    try req.set(4, "000000005000");
    try req.set(49, "XOF");

    var client = BankClient.init(.{
        .host = "127.0.0.1",
        .port = TEST_PORT_4BYTE,
        .length_prefix = .four_byte_be,
    }, io);

    var resp = try client.send(&req, alloc);
    defer resp.deinit();

    try testing.expectEqualSlices(u8, "0210", &resp.mti);
    try testing.expectEqualStrings("00", resp.get(39).?);
}

test "BankClient: ResponseTooLarge" {
    const alloc = testing.allocator;

    const resp_bytes = try buildResponse0210(alloc);
    defer alloc.free(resp_bytes);

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", TEST_PORT_TOOLARGE);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    const thread = try std.Thread.spawn(.{}, serveOnce, .{
        &server, io, resp_bytes, .two_byte_be, alloc,
    });
    defer thread.join();

    var req = IsoMessage.init(alloc, "0200".*);
    defer req.deinit();

    var client = BankClient.init(.{
        .host = "127.0.0.1",
        .port = TEST_PORT_TOOLARGE,
        .max_response_bytes = 10, // smaller than any valid ISO 8583 message
    }, io);

    const result = client.send(&req, alloc);
    try testing.expectError(error.ResponseTooLarge, result);
}
