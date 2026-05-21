const std = @import("std");
const orusconnect = @import("orusconnect");
const orusshare = @import("orusshare");

const http = orusconnect.http_server;
const Router = orusconnect.Router;
const BrokerClient = orusconnect.BrokerClient;
const PendingTxStore = orusconnect.PendingTxStore;
const toml = orusconnect.toml;
const ApiClient = orusconnect.ApiClient;
const generic_momo = orusconnect.adapters.generic_momo;
const GenericMoMoAdapter = generic_momo.GenericMoMoAdapter;

// ── Signal handling ───────────────────────────────────────────────────────────

fn handleSignal(sig: std.posix.SIG) callconv(.c) void {
    _ = sig;
    std.process.exit(0);
}

fn installSignalHandlers() void {
    const sa = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
    std.posix.sigaction(std.posix.SIG.INT,  &sa, null);
}

// ── Generic route shims ───────────────────────────────────────────────────────

fn paymentFn(ctx: *anyopaque, req: *http.HttpRequest, alloc: std.mem.Allocator) http.HttpResponse {
    return (@as(*GenericMoMoAdapter, @ptrCast(@alignCast(ctx)))).handlePayment(req, alloc);
}

fn callbackFn(ctx: *anyopaque, req: *http.HttpRequest, alloc: std.mem.Allocator) http.HttpResponse {
    return (@as(*GenericMoMoAdapter, @ptrCast(@alignCast(ctx)))).handleCallback(req, alloc);
}

fn healthFn(ctx: *anyopaque, req: *http.HttpRequest, alloc: std.mem.Allocator) http.HttpResponse {
    _ = ctx;
    _ = req;
    _ = alloc;
    return http.HttpResponse.ok("{\"status\":\"ok\"}");
}

// ── Direction 2: broker → MoMo dispatch ──────────────────────────────────────

// MSISDN prefix routing table — built dynamically at startup from TOML schemas.
const PrefixEntry = struct {
    prefix:  []const u8,
    adapter: *GenericMoMoAdapter,
};

const DispatchCtx = struct {
    prefixes: []const PrefixEntry,
    alloc:    std.mem.Allocator,
};

fn findField(fields: []const orusshare.InternalMessage.FieldEntry, id: u8) ?[]const u8 {
    for (fields) |f| if (f.id == id) return f.value;
    return null;
}

fn onDispatch(msg: *const orusshare.InternalMessage, raw_ctx: ?*anyopaque) void {
    const ctx: *const DispatchCtx = @ptrCast(@alignCast(raw_ctx.?));

    const msisdn = findField(msg.fields, 2) orelse {
        std.log.warn("dispatch: no MSISDN (field 2) in msg_id={s}", .{
            std.fmt.bytesToHex(msg.msg_id, .lower),
        });
        return;
    };

    for (ctx.prefixes) |entry| {
        if (std.mem.startsWith(u8, msisdn, entry.prefix)) {
            _ = generic_momo.dispatch(entry.adapter, msg, ctx.alloc) catch |err| {
                std.log.warn("dispatch: prefix {s} failed: {}", .{ entry.prefix, err });
            };
            return;
        }
    }

    std.log.warn("dispatch: no adapter for MSISDN prefix '{s}'", .{msisdn});
}

const ConsumeArgs = struct {
    broker: *const BrokerClient,
    ctx:    *DispatchCtx,
    alloc:  std.mem.Allocator,
};

fn consumeLoop(args: *ConsumeArgs) void {
    while (true) {
        args.broker.consume(
            "transactions.outbound",
            args.alloc,
            onDispatch,
            args.ctx,
        ) catch |err| {
            std.log.warn("consume: broker connection lost ({}), reconnecting in 5s...", .{err});
        };
        const ts = std.c.timespec{ .sec = 5, .nsec = 0 };
        _ = std.c.nanosleep(&ts, null);
    }
}

// ── Config helpers ────────────────────────────────────────────────────────────

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

// Resolve an env var whose name comes from a TOML schema (non-null-terminated slice).
fn resolveEnvVar(name: []const u8, alloc: std.mem.Allocator) ?[]const u8 {
    if (name.len == 0) return null;
    const name_z = alloc.dupeZ(u8, name) catch return null;
    defer alloc.free(name_z);
    const raw = std.c.getenv(name_z.ptr) orelse return null;
    return std.mem.span(raw);
}

fn parsePort(s: []const u8, default: u16) u16 {
    return std.fmt.parseInt(u16, s, 10) catch default;
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    installSignalHandlers();

    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    var tx_store = PendingTxStore.init(alloc);
    defer tx_store.deinit();

    const broker = BrokerClient.init(.{
        .host = getenv("BROKER_HOST") orelse "127.0.0.1",
        .port = envU16("BROKER_PORT", 7770),
    }, io);

    const pan_seed = envU64("PAN_HASH_SEED", 0);

    // ── Load adapters from TOML schema directory ──────────────────────────────

    var adapters_list: std.ArrayList(*GenericMoMoAdapter) = .empty;
    defer {
        for (adapters_list.items) |a| {
            a.schema.deinit();
            alloc.destroy(a);
        }
        adapters_list.deinit(alloc);
    }

    // TOML source buffers must outlive the adapter schemas (strings point into them).
    var src_bufs: std.ArrayList([]u8) = .empty;
    defer {
        for (src_bufs.items) |b| alloc.free(b);
        src_bufs.deinit(alloc);
    }

    var prefix_list: std.ArrayList(PrefixEntry) = .empty;
    defer prefix_list.deinit(alloc);

    var router = Router.init(alloc);
    defer router.deinit();

    const schema_dir_path = getenv("CONNECT_SCHEMA_DIR") orelse "./schemas/momo";

    {
        const cwd = std.Io.Dir.cwd();
        const schema_dir = std.Io.Dir.openDir(cwd, io, schema_dir_path, .{ .iterate = true }) catch |err| {
            std.log.err("Cannot open schema dir '{s}': {}", .{ schema_dir_path, err });
            return err;
        };
        defer std.Io.Dir.close(schema_dir, io);

        var it = std.Io.Dir.iterate(schema_dir);
        while (try it.next(io)) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".toml")) continue;

            const src = std.Io.Dir.readFileAlloc(schema_dir, io, entry.name, alloc, std.Io.Limit.limited(512 * 1024)) catch |err| {
                std.log.warn("Failed to read '{s}': {}", .{ entry.name, err });
                continue;
            };

            var schema = toml.parseAdapterSchema(src, alloc) catch |err| {
                std.log.warn("Failed to parse '{s}': {}", .{ entry.name, err });
                alloc.free(src);
                continue;
            };
            try src_bufs.append(alloc, src);

            const bearer = resolveEnvVar(schema.env.bearer_token, alloc) orelse {
                std.log.warn("Env var '{s}' not set for adapter '{s}', skipping", .{
                    schema.env.bearer_token, schema.id,
                });
                schema.deinit();
                _ = src_bufs.pop();
                alloc.free(src);
                continue;
            };

            const callback_tok = if (schema.env.callback_token.len > 0)
                resolveEnvVar(schema.env.callback_token, alloc) orelse bearer
            else
                bearer;

            const api_host = if (schema.env.api_host.len > 0)
                resolveEnvVar(schema.env.api_host, alloc) orelse "127.0.0.1"
            else
                "127.0.0.1";

            const api_port: u16 = if (schema.env.api_port.len > 0) blk: {
                const port_str = resolveEnvVar(schema.env.api_port, alloc) orelse break :blk @as(u16, 8090);
                break :blk parsePort(port_str, 8090);
            } else 8090;

            const api = ApiClient.init(api_host, api_port, io);

            const adapter_ptr = try alloc.create(GenericMoMoAdapter);
            adapter_ptr.* = GenericMoMoAdapter.init(
                schema, bearer, callback_tok, &tx_store, &broker, api, pan_seed,
            );
            try adapters_list.append(alloc, adapter_ptr);

            if (schema.http.payment_path.len > 0)
                try router.add(.POST, schema.http.payment_path, adapter_ptr, paymentFn);
            if (schema.http.callback_path.len > 0)
                try router.add(.POST, schema.http.callback_path, adapter_ptr, callbackFn);

            for (schema.routing.msisdn_prefixes) |prefix| {
                try prefix_list.append(alloc, .{ .prefix = prefix, .adapter = adapter_ptr });
            }

            std.log.info("Loaded adapter '{s}' ({d} prefix(es), payment={s})", .{
                schema.id,
                schema.routing.msisdn_prefixes.len,
                schema.http.payment_path,
            });
        }
    }

    if (adapters_list.items.len == 0) {
        std.log.warn("No adapters loaded from '{s}' — all payment requests will be rejected", .{
            schema_dir_path,
        });
    }

    var dummy: u8 = 0;
    try router.add(.GET, "/health", &dummy, healthFn);

    // ── Direction 2: consume broker → dispatch to MoMo ────────────────────────

    var dispatch_ctx = DispatchCtx{
        .prefixes = prefix_list.items,
        .alloc    = alloc,
    };
    var consume_args = ConsumeArgs{ .broker = &broker, .ctx = &dispatch_ctx, .alloc = alloc };
    const consume_thread = try std.Thread.spawn(.{}, consumeLoop, .{&consume_args});
    consume_thread.detach();

    // ── HTTP server ───────────────────────────────────────────────────────────

    const listen_host = getenv("CONNECT_LISTEN_HOST") orelse "0.0.0.0";
    const listen_port = envU16("CONNECT_LISTEN_PORT", 8080);

    var server = http.Server.init(alloc, .{
        .host = listen_host,
        .port = listen_port,
    });

    std.log.info("OrusConnect listening on {s}:{d}  broker={s}:{d}  {d} adapter(s)", .{
        listen_host,
        listen_port,
        getenv("BROKER_HOST") orelse "127.0.0.1",
        envU16("BROKER_PORT", 7770),
        adapters_list.items.len,
    });

    var h = router.handler();
    try server.serve(&h);
}
