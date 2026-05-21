const std = @import("std");
const orusshare = @import("orusshare");
const orusgateway = @import("orusgateway");

const serialize = orusshare.serialize;
const Gateway = orusgateway.Gateway;
const BankClient = orusgateway.BankClient;
const bank_client_mod = orusgateway.bank_client;
const BankServer = orusgateway.BankServer;
const GatewayBrokerClient = orusgateway.GatewayBrokerClient;
const schema_mod = orusgateway.schema;
const parseIsoSchema = orusgateway.parseIsoSchema;

// Wire protocol (client → gateway → client):
//   Request:  [u32 BE: length] [serialized InternalMessage]
//   Response: [u32 BE: length] [serialized InternalMessage]
//   Error:    [u32 BE: 0]      [u8: error code]
//
// Error codes:
const ERR_DESERIALIZE: u8 = 0x01;
const ERR_TRANSLATE: u8 = 0x02;
const ERR_BANK: u8 = 0x03;
const ERR_SERIALIZE: u8 = 0x04;
const ERR_INTERNAL: u8 = 0xFF;

const Config = struct {
    listen_host: []const u8,
    listen_port: u16,
    bank_host: []const u8,
    bank_port: u16,
    bank_prefix: bank_client_mod.LengthPrefix,
    max_connections: u32,
    // Direction 2: ISO 8583 listener (bank/switch → gateway)
    iso_listen_host: []const u8,
    iso_listen_port: u16,
    iso_schema_name: []const u8, // "gimac" | "visa" | "mastercard"
    broker_host: []const u8,
    broker_port: u16,
};

fn getenv(name: [:0]const u8) ?[]const u8 {
    const raw = std.c.getenv(name.ptr) orelse return null;
    return std.mem.span(raw);
}

fn envU16(name: [:0]const u8, default: u16) u16 {
    const s = getenv(name) orelse return default;
    return std.fmt.parseInt(u16, s, 10) catch default;
}

fn envU64(name: [:0]const u8, default: u64) u64 {
    const s = getenv(name) orelse return default;
    return std.fmt.parseInt(u64, s, 10) catch default;
}

fn loadConfig(alloc: std.mem.Allocator) !Config {
    const listen_host = if (getenv("GATEWAY_LISTEN_HOST")) |s|
        try alloc.dupe(u8, s)
    else
        try alloc.dupe(u8, "0.0.0.0");

    const bank_host = if (getenv("GATEWAY_BANK_HOST")) |s|
        try alloc.dupe(u8, s)
    else
        try alloc.dupe(u8, "127.0.0.1");

    const prefix: bank_client_mod.LengthPrefix = blk: {
        const s = getenv("GATEWAY_BANK_PREFIX") orelse break :blk .two_byte_be;
        break :blk if (std.mem.eql(u8, s, "4")) .four_byte_be else .two_byte_be;
    };

    const iso_host = if (getenv("GATEWAY_ISO_HOST")) |s|
        try alloc.dupe(u8, s)
    else
        try alloc.dupe(u8, "0.0.0.0");

    const broker_host = if (getenv("BROKER_HOST")) |s|
        try alloc.dupe(u8, s)
    else
        try alloc.dupe(u8, "127.0.0.1");

    const schema_name = if (getenv("GATEWAY_ISO_SCHEMA")) |s|
        try alloc.dupe(u8, s)
    else
        try alloc.dupe(u8, "gimac");

    return .{
        .listen_host     = listen_host,
        .listen_port     = envU16("GATEWAY_LISTEN_PORT", 7780),
        .bank_host       = bank_host,
        .bank_port       = envU16("GATEWAY_BANK_PORT", 5000),
        .bank_prefix     = prefix,
        .max_connections = 500,
        .iso_listen_host = iso_host,
        .iso_listen_port = envU16("GATEWAY_ISO_PORT", 7581),
        .iso_schema_name = schema_name,
        .broker_host     = broker_host,
        .broker_port     = envU16("BROKER_PORT", 7770),
    };
}

const ConnCtx = struct {
    stream: std.Io.net.Stream,
    io: std.Io,
    gateway: *const Gateway,
    alloc: std.mem.Allocator,
};

fn handleConn(ctx: *ConnCtx) void {
    defer ctx.stream.close(ctx.io);
    defer ctx.alloc.destroy(ctx);

    var rbuf: [8192]u8 = undefined;
    var sr = ctx.stream.reader(ctx.io, &rbuf);
    var r = &sr.interface;

    var wbuf: [8192]u8 = undefined;
    var sw = ctx.stream.writer(ctx.io, &wbuf);
    var w = &sw.interface;

    // Read request length
    var len_buf: [4]u8 = undefined;
    r.readSliceAll(&len_buf) catch return;
    const req_len = std.mem.readInt(u32, &len_buf, .big);
    if (req_len == 0 or req_len > 1024 * 1024) {
        sendError(w, ERR_INTERNAL) catch {};
        return;
    }

    // Read serialized InternalMessage
    const req_buf = ctx.alloc.alloc(u8, req_len) catch {
        sendError(w, ERR_INTERNAL) catch {};
        return;
    };
    defer ctx.alloc.free(req_buf);
    r.readSliceAll(req_buf) catch return;

    // Deserialize
    var fixed_r = std.Io.Reader.fixed(req_buf);
    const req = serialize.deserialize(&fixed_r, ctx.alloc) catch {
        sendError(w, ERR_DESERIALIZE) catch {};
        return;
    };
    defer serialize.free(&req, ctx.alloc);

    // Process through gateway
    const resp = ctx.gateway.process(&req, ctx.alloc) catch |err| {
        const code: u8 = switch (err) {
            error.BankUnreachable, error.ResponseTooLarge, error.MalformedResponse => ERR_BANK,
            error.MissingAmount, error.InvalidAmount, error.MissingCurrency => ERR_TRANSLATE,
            error.OutOfMemory => ERR_INTERNAL,
        };
        sendError(w, code) catch {};
        return;
    };
    defer {
        for (resp.fields) |f| ctx.alloc.free(f.value);
        ctx.alloc.free(resp.fields);
    }

    // Serialize response to a buffer to get its size
    var payload_list: std.ArrayList(u8) = .empty;
    payload_list.ensureTotalCapacity(ctx.alloc, 2048) catch {
        sendError(w, ERR_SERIALIZE) catch {};
        return;
    };
    var pw = std.Io.Writer.fromArrayList(&payload_list);
    serialize.serialize(&resp, &pw) catch {
        var leftover = std.Io.Writer.toArrayList(&pw);
        leftover.deinit(ctx.alloc);
        sendError(w, ERR_SERIALIZE) catch {};
        return;
    };
    var written = std.Io.Writer.toArrayList(&pw);
    defer written.deinit(ctx.alloc);
    const payload = written.items;

    // Write [u32 length] [payload]
    w.writeInt(u32, @intCast(payload.len), .big) catch return;
    w.writeAll(payload) catch return;
    w.flush() catch return;
}

fn sendError(w: *std.Io.Writer, code: u8) std.Io.Writer.Error!void {
    try w.writeInt(u32, 0, .big);
    try w.writeByte(code);
    try w.flush();
}

const schemas = @import("schemas");

fn resolveIsoSchema(name: []const u8, alloc: std.mem.Allocator) !schema_mod.IsoSchema {
    const src = if (std.mem.eql(u8, name, "visa"))
        schemas.iso8583.visa
    else if (std.mem.eql(u8, name, "mastercard"))
        schemas.iso8583.mastercard
    else
        schemas.iso8583.gimac; // default: GIMAC
    return parseIsoSchema(src, alloc);
}

// ── Direction 1: OrusBroker → Gateway → Bank ─────────────────────────────────

const D1Args = struct {
    gateway: *const Gateway,
    broker:  GatewayBrokerClient,
    alloc:   std.mem.Allocator,
};

fn onD1Message(raw_msg: *const orusshare.InternalMessage, raw_ctx: ?*anyopaque) void {
    const args: *const D1Args = @ptrCast(@alignCast(raw_ctx.?));

    // Avoid infinite loop: skip messages that already came from the bank leg.
    if (raw_msg.origin == .iso8583) return;

    const resp = args.gateway.process(raw_msg, args.alloc) catch |err| {
        std.log.err("D1 gateway.process failed: {}", .{err});
        return;
    };
    defer {
        for (resp.fields) |f| args.alloc.free(f.value);
        args.alloc.free(resp.fields);
    }

    args.broker.publish(&resp, args.alloc) catch |err| {
        std.log.err("D1 broker.publish failed: {}", .{err});
    };
}

// Reconnect loop — runs in a dedicated thread.
fn d1ConsumeLoop(args: *D1Args) void {
    while (true) {
        args.broker.consume(
            "transactions.inbound",
            args.alloc,
            onD1Message,
            @ptrCast(@constCast(args)),
        ) catch {};
        // Brief pause before reconnect so we don't spin on a dead broker.
        const ts = std.c.timespec{ .sec = 0, .nsec = 500_000_000 };
        _ = std.c.nanosleep(&ts, null);
    }
}

const BankServerArgs = struct {
    server: *const BankServer,
    alloc: std.mem.Allocator,
};

fn runBankServer(args: *BankServerArgs) void {
    args.server.serve(args.alloc) catch |err| {
        std.log.err("BankServer exited with error: {}", .{err});
    };
}

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const config = try loadConfig(alloc);
    defer alloc.free(config.listen_host);
    defer alloc.free(config.bank_host);
    defer alloc.free(config.iso_listen_host);
    defer alloc.free(config.broker_host);
    defer alloc.free(config.iso_schema_name);

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    // ── Direction 1: InternalMessage listener (broker pushes to us) ─────────
    const gw = Gateway.init(BankClient.init(.{
        .host = config.bank_host,
        .port = config.bank_port,
        .length_prefix = config.bank_prefix,
    }, io), envU64("PAN_HASH_SEED", 0));

    // ── Direction 2: ISO 8583 listener (bank/switch connects to us) ─────────
    var iso_schema = try resolveIsoSchema(config.iso_schema_name, alloc);
    defer iso_schema.deinit();

    const broker_gw = GatewayBrokerClient.init(.{
        .host = config.broker_host,
        .port = config.broker_port,
    }, io);

    const bank_srv = BankServer.init(.{
        .host          = config.iso_listen_host,
        .port          = config.iso_listen_port,
        .length_prefix = config.bank_prefix,
    }, io, &iso_schema, broker_gw);

    var bs_args = BankServerArgs{ .server = &bank_srv, .alloc = alloc };
    const bs_thread = try std.Thread.spawn(.{}, runBankServer, .{&bs_args});
    bs_thread.detach();

    // D1 subscriber: OrusBroker → Gateway → Bank → OrusBroker (response topic).
    // Uses page_allocator so the thread doesn't share the main GPA.
    var d1_args = D1Args{
        .gateway = &gw,
        .broker  = GatewayBrokerClient.init(.{
            .host = config.broker_host,
            .port = config.broker_port,
        }, io),
        .alloc = std.heap.page_allocator,
    };
    const d1_thread = try std.Thread.spawn(.{}, d1ConsumeLoop, .{&d1_args});
    d1_thread.detach();

    const addr = try std.Io.net.IpAddress.parse(config.listen_host, config.listen_port);
    var server = try addr.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);

    std.log.info("OrusGateway D1-listener={s}:{d}  D2-iso={s}:{d}  bank={s}:{d}  schema={s}", .{
        config.listen_host,     config.listen_port,
        config.iso_listen_host, config.iso_listen_port,
        config.bank_host,       config.bank_port,
        config.iso_schema_name,
    });

    while (true) {
        const stream = server.accept(io) catch |err| {
            if (err == error.SocketNotListening) break;
            std.log.err("accept: {}", .{err});
            continue;
        };

        const ctx = alloc.create(ConnCtx) catch {
            stream.close(io);
            continue;
        };
        ctx.* = .{
            .stream = stream,
            .io = io,
            .gateway = &gw,
            .alloc = alloc,
        };

        const thread = std.Thread.spawn(.{}, handleConn, .{ctx}) catch |err| {
            std.log.err("thread spawn: {}", .{err});
            stream.close(io);
            alloc.destroy(ctx);
            continue;
        };
        thread.detach();
    }
}
