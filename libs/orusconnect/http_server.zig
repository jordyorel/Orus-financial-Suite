const std = @import("std");

pub const Config = struct {
    host: []const u8 = "0.0.0.0",
    port: u16 = 8443,
    max_connections: u32 = 500,
    read_buffer_size: usize = 8192,
    write_buffer_size: usize = 8192,
    header_max_bytes: usize = 8192,
    body_max_bytes: usize = 1024 * 1024, // 1 MB

    // TLS server-side termination.
    // Zig 0.16 stdlib provides only a TLS client (std.crypto.tls.Client).
    // For inbound TLS, run OrusConnect behind a TLS-terminating reverse proxy
    // (nginx, caddy, envoy) and set these fields for documentation/validation.
    // When set, serve() logs a startup reminder about the proxy requirement.
    tls_cert_path: ?[]const u8 = null,
    tls_key_path:  ?[]const u8 = null,
};

pub const Method = enum { GET, POST, PUT, DELETE, PATCH, OPTIONS };

pub const HttpRequest = struct {
    arena: std.heap.ArenaAllocator,
    method: Method,
    path: []const u8,
    headers: std.StringHashMapUnmanaged([]const u8),
    body: []const u8,

    pub fn deinit(self: *HttpRequest) void {
        self.arena.deinit();
    }

    pub fn header(self: *const HttpRequest, name: []const u8) ?[]const u8 {
        return self.headers.get(name);
    }
};

pub const HttpResponse = struct {
    status: u16,
    status_text: []const u8,
    body: []const u8,
    content_type: []const u8 = "application/json",

    pub fn ok(body: []const u8) HttpResponse {
        return .{ .status = 200, .status_text = "OK", .body = body };
    }
    pub fn accepted(body: []const u8) HttpResponse {
        return .{ .status = 202, .status_text = "Accepted", .body = body };
    }
    pub fn badRequest(body: []const u8) HttpResponse {
        return .{ .status = 400, .status_text = "Bad Request", .body = body };
    }
    pub fn unauthorized() HttpResponse {
        return .{ .status = 401, .status_text = "Unauthorized", .body = "{\"error\":\"Unauthorized\"}" };
    }
    pub fn notFound() HttpResponse {
        return .{ .status = 404, .status_text = "Not Found", .body = "{\"error\":\"Not Found\"}" };
    }
    pub fn unprocessable(body: []const u8) HttpResponse {
        return .{ .status = 422, .status_text = "Unprocessable Entity", .body = body };
    }
    pub fn serviceUnavailable() HttpResponse {
        return .{ .status = 503, .status_text = "Service Unavailable", .body = "{\"error\":\"Service Unavailable\"}" };
    }
    pub fn tooManyRequests() HttpResponse {
        return .{ .status = 429, .status_text = "Too Many Requests", .body = "{\"error\":\"Too Many Requests\"}" };
    }
};

// ── Listener abstraction (CDC section 9.2) ────────────────────────────────────

pub const Connection = struct {
    stream: std.Io.net.Stream,
};

pub const Listener = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        accept: *const fn (*anyopaque) anyerror!Connection,
        close: *const fn (*anyopaque) void,
    };

    pub fn accept(self: *Listener) anyerror!Connection {
        return self.vtable.accept(self.ptr);
    }
    pub fn close(self: *Listener) void {
        self.vtable.close(self.ptr);
    }
};

// V1: thread-per-connection TCP listener
pub const ThreadListener = struct {
    server: std.Io.net.Server,
    io: std.Io,

    pub fn init(host: []const u8, port: u16, io: std.Io) !ThreadListener {
        const addr = try std.Io.net.IpAddress.parse(host, port);
        const server = try addr.listen(io, .{});
        return .{ .server = server, .io = io };
    }

    pub fn deinit(self: *ThreadListener) void {
        self.server.deinit(self.io);
    }

    pub fn listener(self: *ThreadListener) Listener {
        return .{
            .ptr = self,
            .vtable = &.{ .accept = acceptFn, .close = closeFn },
        };
    }

    fn acceptFn(ptr: *anyopaque) anyerror!Connection {
        const self: *ThreadListener = @ptrCast(@alignCast(ptr));
        return .{ .stream = try self.server.accept(self.io) };
    }
    fn closeFn(ptr: *anyopaque) void {
        const self: *ThreadListener = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

// ── HTTP/1.1 request parser ───────────────────────────────────────────────────

pub const ParseError = error{
    HeadersTooLarge,
    MalformedRequestLine,
    UnknownMethod,
    BodyTooLarge,
    OutOfMemory,
    EndOfStream,
};

// Reads raw bytes until \r\n\r\n. Returns number of bytes consumed.
fn readHeaderSection(r: *std.Io.Reader, buf: []u8) (std.Io.Reader.Error || ParseError)!usize {
    var pos: usize = 0;
    while (pos < buf.len) {
        buf[pos] = try r.takeByte();
        pos += 1;
        if (pos >= 4 and
            buf[pos - 4] == '\r' and buf[pos - 3] == '\n' and
            buf[pos - 2] == '\r' and buf[pos - 1] == '\n')
        {
            return pos;
        }
    }
    return error.HeadersTooLarge;
}

pub fn parseRequest(
    r: *std.Io.Reader,
    alloc: std.mem.Allocator,
    cfg: Config,
) (std.Io.Reader.Error || ParseError)!HttpRequest {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();

    // Read header section into heap buffer
    const hbuf = try a.alloc(u8, cfg.header_max_bytes);
    const hlen = try readHeaderSection(r, hbuf);
    const hdata = hbuf[0..hlen];

    // Parse request line
    var lines = std.mem.splitSequence(u8, hdata, "\r\n");
    const req_line = lines.next() orelse return error.MalformedRequestLine;

    var parts = std.mem.splitScalar(u8, req_line, ' ');
    const method_str = parts.next() orelse return error.MalformedRequestLine;
    const path_raw = parts.next() orelse return error.MalformedRequestLine;

    const method = parseMethod(method_str) orelse return error.UnknownMethod;
    const path = try a.dupe(u8, path_raw);

    // Parse headers
    var headers = std.StringHashMapUnmanaged([]const u8){};
    while (lines.next()) |line| {
        if (line.len == 0) break;
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = std.mem.trim(u8, line[0..colon], " \t");
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        const lower = try a.alloc(u8, name.len);
        _ = std.ascii.lowerString(lower, name);
        try headers.put(a, lower, try a.dupe(u8, value));
    }

    // Read body
    const cl_str = headers.get("content-length");
    const body_len: usize = if (cl_str) |s| std.fmt.parseInt(usize, s, 10) catch 0 else 0;
    if (body_len > cfg.body_max_bytes) return error.BodyTooLarge;

    const body = try a.alloc(u8, body_len);
    if (body_len > 0) try r.readSliceAll(body);

    return .{
        .arena = arena,
        .method = method,
        .path = path,
        .headers = headers,
        .body = body,
    };
}

fn parseMethod(s: []const u8) ?Method {
    if (std.mem.eql(u8, s, "GET")) return .GET;
    if (std.mem.eql(u8, s, "POST")) return .POST;
    if (std.mem.eql(u8, s, "PUT")) return .PUT;
    if (std.mem.eql(u8, s, "DELETE")) return .DELETE;
    if (std.mem.eql(u8, s, "PATCH")) return .PATCH;
    if (std.mem.eql(u8, s, "OPTIONS")) return .OPTIONS;
    return null;
}

// ── HTTP/1.1 response writer ──────────────────────────────────────────────────

pub fn writeResponse(w: *std.Io.Writer, resp: HttpResponse) std.Io.Writer.Error!void {
    // Status line: write the 3-digit code digit-by-digit — HTTP codes are always 100-599.
    const s = resp.status;
    try w.writeAll("HTTP/1.1 ");
    try w.writeByte('0' + @as(u8, @intCast(s / 100)));
    try w.writeByte('0' + @as(u8, @intCast((s / 10) % 10)));
    try w.writeByte('0' + @as(u8, @intCast(s % 10)));
    try w.writeByte(' ');
    try w.writeAll(resp.status_text);
    try w.writeAll("\r\nContent-Type: ");
    try w.writeAll(resp.content_type);
    try w.writeAll("\r\nContent-Length: ");
    // body.len ≤ body_max_bytes (1 MB = 7 digits) — [8]u8 always sufficient.
    var len_buf: [8]u8 = undefined;
    var len_pos: usize = 8;
    var remaining = resp.body.len;
    if (remaining == 0) {
        len_pos -= 1;
        len_buf[len_pos] = '0';
    } else {
        while (remaining > 0) {
            len_pos -= 1;
            len_buf[len_pos] = '0' + @as(u8, @intCast(remaining % 10));
            remaining /= 10;
        }
    }
    try w.writeAll(len_buf[len_pos..]);
    try w.writeAll("\r\nConnection: close\r\n\r\n");
    try w.writeAll(resp.body);
}

// ── Server ────────────────────────────────────────────────────────────────────

pub const Handler = struct {
    ptr: *anyopaque,
    vtable: *const HandlerVTable,

    pub const HandlerVTable = struct {
        handle: *const fn (*anyopaque, *HttpRequest) HttpResponse,
    };

    pub fn handle(self: *const Handler, req: *HttpRequest) HttpResponse {
        return self.vtable.handle(self.ptr, req);
    }
};

const ConnContext = struct {
    stream: std.Io.net.Stream,
    io: std.Io,
    handler: *const Handler,
    alloc: std.mem.Allocator,
    config: Config,
};

fn handleConn(ctx: *ConnContext) void {
    defer ctx.stream.close(ctx.io);

    const read_buf = ctx.alloc.alloc(u8, ctx.config.read_buffer_size) catch return;
    defer ctx.alloc.free(read_buf);
    const write_buf = ctx.alloc.alloc(u8, ctx.config.write_buffer_size) catch return;
    defer ctx.alloc.free(write_buf);

    var sr = ctx.stream.reader(ctx.io, read_buf);
    var sw = ctx.stream.writer(ctx.io, write_buf);

    var req = parseRequest(&sr.interface, ctx.alloc, ctx.config) catch |err| {
        const resp = switch (err) {
            error.HeadersTooLarge, error.BodyTooLarge => HttpResponse.badRequest("{\"error\":\"Request too large\"}"),
            error.UnknownMethod => HttpResponse.badRequest("{\"error\":\"Method not allowed\"}"),
            else => HttpResponse.badRequest("{\"error\":\"Bad request\"}"),
        };
        writeResponse(&sw.interface, resp) catch {};
        sw.interface.flush() catch {};
        return;
    };
    defer req.deinit();

    const resp = ctx.handler.handle(&req);
    writeResponse(&sw.interface, resp) catch {};
    sw.interface.flush() catch {};
    ctx.alloc.destroy(ctx);
}

pub const Server = struct {
    alloc: std.mem.Allocator,
    config: Config,
    running: std.atomic.Value(bool),

    pub fn init(alloc: std.mem.Allocator, config: Config) Server {
        return .{
            .alloc = alloc,
            .config = config,
            .running = std.atomic.Value(bool).init(false),
        };
    }

    pub fn stop(self: *Server) void {
        self.running.store(false, .seq_cst);
    }

    pub fn serve(self: *Server, handler: *const Handler) !void {
        var threaded = std.Io.Threaded.init(self.alloc, .{});
        const io = threaded.io();

        var tcp = try ThreadListener.init(self.config.host, self.config.port, io);
        defer tcp.deinit();

        if (self.config.tls_cert_path != null or self.config.tls_key_path != null) {
            std.log.info(
                "OrusConnect TLS: configure your reverse proxy (nginx/caddy/envoy) " ++
                "to terminate TLS and forward plain HTTP to {s}:{d}. " ++
                "cert={?s} key={?s}",
                .{ self.config.host, self.config.port,
                   self.config.tls_cert_path, self.config.tls_key_path },
            );
        }

        self.running.store(true, .seq_cst);

        while (self.running.load(.seq_cst)) {
            const stream = tcp.server.accept(io) catch |err| {
                if (err == error.SocketNotListening) break;
                std.log.err("accept error: {}", .{err});
                continue;
            };

            const ctx = self.alloc.create(ConnContext) catch {
                stream.close(io);
                continue;
            };
            ctx.* = .{
                .stream = stream,
                .io = io,
                .handler = handler,
                .alloc = self.alloc,
                .config = self.config,
            };

            const thread = std.Thread.spawn(.{}, handleConn, .{ctx}) catch |err| {
                std.log.err("thread spawn: {}", .{err});
                stream.close(io);
                self.alloc.destroy(ctx);
                continue;
            };
            thread.detach();
        }
    }
};
