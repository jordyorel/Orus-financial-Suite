const std = @import("std");

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Response = struct {
    status: u16,
    body: []u8,
    alloc: std.mem.Allocator,

    pub fn deinit(self: *Response) void {
        self.alloc.free(self.body);
    }

    pub fn is2xx(self: Response) bool {
        return self.status >= 200 and self.status < 300;
    }
};

pub const ApiError = error{
    ConnectionFailed,
    ResponseTooLarge,
    MalformedResponse,
    OutOfMemory,
};

// Stateless HTTP/1.1 client — connect-per-request, V1.
// Supports Content-Length responses only (no chunked transfer encoding).
//
// TLS modes:
//   init()    → plain TCP (for tests, internal services)
//   initTls() → HTTPS with system CA bundle (for MoMo public APIs)
pub const ApiClient = struct {
    host: []const u8,
    port: u16,
    io:   std.Io,
    // Non-null when TLS is enabled. Holds a loaded CA bundle + associated lock.
    tls_state: ?TlsState,

    const TlsState = struct {
        bundle: std.crypto.Certificate.Bundle,
        lock:   std.Io.RwLock,
        gpa:    std.mem.Allocator,
    };

    pub fn init(host: []const u8, port: u16, io: std.Io) ApiClient {
        return .{ .host = host, .port = port, .io = io, .tls_state = null };
    }

    // Load the system CA bundle once and store it in the client.
    // Use for outbound HTTPS to public MoMo APIs (MTN, Airtel, Wave…).
    // Caller must call deinit() when done.
    pub fn initTls(host: []const u8, port: u16, io: std.Io, gpa: std.mem.Allocator) !ApiClient {
        var bundle: std.crypto.Certificate.Bundle = .empty;
        try bundle.rescan(gpa, io, std.Io.Clock.real.now(io));
        return .{
            .host = host,
            .port = port,
            .io   = io,
            .tls_state = .{ .bundle = bundle, .lock = std.Io.RwLock.init, .gpa = gpa },
        };
    }

    pub fn deinit(self: *ApiClient) void {
        if (self.tls_state) |*s| s.bundle.deinit(s.gpa);
    }

    pub fn post(
        self: *ApiClient,
        path: []const u8,
        headers: []const Header,
        body: []const u8,
        alloc: std.mem.Allocator,
    ) ApiError!Response {
        const addr = std.Io.net.IpAddress.parse(self.host, self.port) catch
            return error.ConnectionFailed;
        var stream = addr.connect(self.io, .{ .mode = .stream }) catch
            return error.ConnectionFailed;
        defer stream.close(self.io);

        var raw_read: [8192]u8 = undefined;
        var raw_write: [8192]u8 = undefined;
        var sr = stream.reader(self.io, &raw_read);
        var sw = stream.writer(self.io, &raw_write);

        if (self.tls_state) |*s| {
            var tls_read: [std.crypto.tls.Client.min_buffer_len]u8 = undefined;
            var tls_write: [std.crypto.tls.Client.min_buffer_len]u8 = undefined;
            var entropy: [std.crypto.tls.Client.Options.entropy_len]u8 = undefined;
            std.c.arc4random_buf(&entropy, entropy.len);
            var tls = std.crypto.tls.Client.init(&sr.interface, &sw.interface, .{
                .host = .{ .explicit = self.host },
                .ca   = .{ .bundle = .{
                    .gpa    = s.gpa,
                    .io     = self.io,
                    .lock   = &s.lock,
                    .bundle = &s.bundle,
                }},
                .write_buffer = &tls_write,
                .read_buffer  = &tls_read,
                .entropy      = &entropy,
                .realtime_now = std.Io.Clock.real.now(self.io),
            }) catch return error.ConnectionFailed;
            defer tls.end() catch {};
            return doHttp(&tls.reader, &tls.writer, self.host, path, headers, body, alloc);
        } else {
            return doHttp(&sr.interface, &sw.interface, self.host, path, headers, body, alloc);
        }
    }
};

fn doHttp(
    r: *std.Io.Reader,
    w: *std.Io.Writer,
    host: []const u8,
    path: []const u8,
    headers: []const Header,
    body: []const u8,
    alloc: std.mem.Allocator,
) ApiError!Response {
    // Request line
    w.writeAll("POST ") catch return error.ConnectionFailed;
    w.writeAll(path) catch return error.ConnectionFailed;
    w.writeAll(" HTTP/1.1\r\nHost: ") catch return error.ConnectionFailed;
    w.writeAll(host) catch return error.ConnectionFailed;
    w.writeAll("\r\n") catch return error.ConnectionFailed;

    for (headers) |h| {
        w.writeAll(h.name) catch return error.ConnectionFailed;
        w.writeAll(": ") catch return error.ConnectionFailed;
        w.writeAll(h.value) catch return error.ConnectionFailed;
        w.writeAll("\r\n") catch return error.ConnectionFailed;
    }

    // Content-Length — digit-by-digit, no format buffer.
    w.writeAll("Content-Length: ") catch return error.ConnectionFailed;
    var cl_buf: [20]u8 = undefined;
    var cl_pos: usize = 20;
    var cl_rem = body.len;
    if (cl_rem == 0) {
        cl_pos -= 1;
        cl_buf[cl_pos] = '0';
    } else {
        while (cl_rem > 0) {
            cl_pos -= 1;
            cl_buf[cl_pos] = '0' + @as(u8, @intCast(cl_rem % 10));
            cl_rem /= 10;
        }
    }
    w.writeAll(cl_buf[cl_pos..]) catch return error.ConnectionFailed;
    w.writeAll("\r\nConnection: close\r\n\r\n") catch return error.ConnectionFailed;
    if (body.len > 0) w.writeAll(body) catch return error.ConnectionFailed;
    w.flush() catch return error.ConnectionFailed;

    return parseResponse(r, alloc);
}

fn parseResponse(r: *std.Io.Reader, alloc: std.mem.Allocator) ApiError!Response {
    // Read headers section (ends at \r\n\r\n)
    var hbuf: [4096]u8 = undefined;
    var pos: usize = 0;
    while (pos < hbuf.len) {
        hbuf[pos] = r.takeByte() catch return error.MalformedResponse;
        pos += 1;
        if (pos >= 4 and
            hbuf[pos - 4] == '\r' and hbuf[pos - 3] == '\n' and
            hbuf[pos - 2] == '\r' and hbuf[pos - 1] == '\n') break;
    } else return error.ResponseTooLarge;

    const hdata = hbuf[0..pos];
    var lines = std.mem.splitSequence(u8, hdata, "\r\n");

    // Status line: "HTTP/1.1 200 OK"
    const status_line = lines.next() orelse return error.MalformedResponse;
    var parts = std.mem.splitScalar(u8, status_line, ' ');
    _ = parts.next(); // HTTP version
    const code_str = parts.next() orelse return error.MalformedResponse;
    const status = std.fmt.parseInt(u16, code_str, 10) catch return error.MalformedResponse;

    var content_length: usize = 0;
    while (lines.next()) |line| {
        if (line.len == 0) break;
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = std.mem.trim(u8, line[0..colon], " \t");
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(name, "content-length")) {
            content_length = std.fmt.parseInt(usize, value, 10) catch 0;
        }
    }

    if (content_length > 1024 * 1024) return error.ResponseTooLarge;

    const body = alloc.alloc(u8, content_length) catch return error.OutOfMemory;
    errdefer alloc.free(body);
    if (content_length > 0) r.readSliceAll(body) catch return error.MalformedResponse;

    return .{ .status = status, .body = body, .alloc = alloc };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const testing = std.testing;
const TEST_PORT: u16 = 19240;

fn serveOnce(server: *std.Io.net.Server, io: std.Io, status: u16, body: []const u8) void {
    var stream = server.accept(io) catch return;
    defer stream.close(io);

    // Drain the request
    var rbuf: [8192]u8 = undefined;
    var sr = stream.reader(io, &rbuf);
    var r = &sr.interface;
    var pos: usize = 0;
    while (pos < rbuf.len) {
        rbuf[pos] = r.takeByte() catch return;
        pos += 1;
        if (pos >= 4 and
            rbuf[pos - 4] == '\r' and rbuf[pos - 3] == '\n' and
            rbuf[pos - 2] == '\r' and rbuf[pos - 1] == '\n') break;
    }

    var wbuf: [4096]u8 = undefined;
    var sw = stream.writer(io, &wbuf);
    var w = &sw.interface;
    var line_buf: [64]u8 = undefined;
    const status_line = std.fmt.bufPrint(&line_buf, "HTTP/1.1 {d} OK\r\n", .{status}) catch return;
    w.writeAll(status_line) catch return;
    w.writeAll("Content-Type: application/json\r\n") catch return;
    var cl_buf: [32]u8 = undefined;
    const cl = std.fmt.bufPrint(&cl_buf, "Content-Length: {d}\r\n\r\n", .{body.len}) catch return;
    w.writeAll(cl) catch return;
    w.writeAll(body) catch return;
    sw.interface.flush() catch return;
}

test "ApiClient: 200 response with body" {
    const alloc = testing.allocator;

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", TEST_PORT);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    const body_str = "{\"status\":\"SUCCESSFUL\"}";
    const t = try std.Thread.spawn(.{}, serveOnce, .{ &server, io, 200, body_str });
    defer t.join();

    var client = ApiClient.init("127.0.0.1", TEST_PORT, io);
    var resp = try client.post("/v2/transfer", &.{
        .{ .name = "Content-Type", .value = "application/json" },
        .{ .name = "Authorization", .value = "Bearer tok" },
    }, "{\"msisdn\":\"242064000001\"}", alloc);
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expect(resp.is2xx());
    try testing.expectEqualStrings(body_str, resp.body);
}

test "ApiClient: 422 response" {
    const alloc = testing.allocator;

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", TEST_PORT + 1);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    const t = try std.Thread.spawn(.{}, serveOnce, .{ &server, io, 422, "{\"error\":\"invalid\"}" });
    defer t.join();

    var client = ApiClient.init("127.0.0.1", TEST_PORT + 1, io);
    var resp = try client.post("/v2/transfer", &.{}, "", alloc);
    defer resp.deinit();

    try testing.expectEqual(@as(u16, 422), resp.status);
    try testing.expect(!resp.is2xx());
}
