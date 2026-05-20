const std = @import("std");
const orusconnect = @import("orusconnect");
const orusshare = @import("orusshare");
const schemas = @import("schemas");

const http = orusconnect.http_server;
const Router = orusconnect.Router;
const BrokerClient = orusconnect.BrokerClient;
const PendingTxStore = orusconnect.PendingTxStore;
const toml = orusconnect.toml;
const pending = orusconnect.state.pending_tx;
const mtn_mod = orusconnect.adapters.mtn_momo;
const airtel_mod = orusconnect.adapters.airtel_money;
const ApiClient = orusconnect.ApiClient;

// ── Route handler shims ───────────────────────────────────────────────────────

fn mtnTransferFn(ctx: *anyopaque, req: *http.HttpRequest, alloc: std.mem.Allocator) http.HttpResponse {
    const a: *mtn_mod.MtnMomoAdapter = @ptrCast(@alignCast(ctx));
    return a.handleRequestToPay(req, alloc);
}

fn mtnCallbackFn(ctx: *anyopaque, req: *http.HttpRequest, alloc: std.mem.Allocator) http.HttpResponse {
    const a: *mtn_mod.MtnMomoAdapter = @ptrCast(@alignCast(ctx));
    return a.handleCallback(req, alloc);
}

fn airtelPaymentFn(ctx: *anyopaque, req: *http.HttpRequest, alloc: std.mem.Allocator) http.HttpResponse {
    const a: *airtel_mod.AirtelMoneyAdapter = @ptrCast(@alignCast(ctx));
    return a.handlePayment(req, alloc);
}

fn airtelCallbackFn(ctx: *anyopaque, req: *http.HttpRequest, alloc: std.mem.Allocator) http.HttpResponse {
    const a: *airtel_mod.AirtelMoneyAdapter = @ptrCast(@alignCast(ctx));
    return a.handleCallback(req, alloc);
}

fn healthFn(ctx: *anyopaque, req: *http.HttpRequest, alloc: std.mem.Allocator) http.HttpResponse {
    _ = ctx;
    _ = req;
    _ = alloc;
    return http.HttpResponse.ok("{\"status\":\"ok\"}");
}

// ── Direction 2: broker → MoMo dispatch ──────────────────────────────────────

const DispatchCtx = struct {
    mtn:        *const mtn_mod.MtnMomoAdapter,
    airtel:     *const airtel_mod.AirtelMoneyAdapter,
    mtn_client: ApiClient,
    airtel_client: ApiClient,
    alloc:      std.mem.Allocator,
};

fn onDispatch(msg: *const orusshare.InternalMessage, raw_ctx: ?*anyopaque) void {
    const ctx: *DispatchCtx = @ptrCast(@alignCast(raw_ctx.?));
    switch (msg.provider) {
        .mtn_momo => {
            const result = mtn_mod.dispatch(ctx.mtn, msg, &ctx.mtn_client, ctx.alloc) catch |err| {
                std.log.warn("dispatch: MTN dispatch failed: {}", .{err});
                return;
            };
            _ = result;
        },
        .airtel_money => {
            const result = airtel_mod.dispatch(ctx.airtel, msg, &ctx.airtel_client, ctx.alloc) catch |err| {
                std.log.warn("dispatch: Airtel dispatch failed: {}", .{err});
                return;
            };
            _ = result;
        },
        else => {
            std.log.warn("dispatch: unknown provider for msg_id={s}", .{std.fmt.bytesToHex(msg.msg_id, .lower)});
        },
    }
}

const ConsumeArgs = struct {
    broker: *const BrokerClient,
    ctx:    *DispatchCtx,
    alloc:  std.mem.Allocator,
};

fn consumeLoop(args: *ConsumeArgs) void {
    while (true) {
        args.broker.consume(
            "transactions.financial",
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

fn require(name: [:0]const u8) []const u8 {
    return getenv(name) orelse {
        std.log.err("Required env var {s} is not set", .{name});
        std.process.exit(1);
    };
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub fn main() !void {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // Parse schemas (embedded at compile time via schemas module)
    var mtn_schema = toml.parseAdapterSchema(schemas.mtn_momo, alloc) catch |err| {
        std.log.err("Failed to parse MTN schema: {}", .{err});
        return err;
    };
    defer mtn_schema.deinit();

    var airtel_schema = toml.parseAdapterSchema(schemas.airtel_money, alloc) catch |err| {
        std.log.err("Failed to parse Airtel schema: {}", .{err});
        return err;
    };
    defer airtel_schema.deinit();

    // Shared pending transaction store
    var tx_store = PendingTxStore.init(alloc);
    defer tx_store.deinit();

    // Io runtime for broker connections and timestamps
    var threaded = std.Io.Threaded.init(alloc, .{});
    const io = threaded.io();

    const broker = BrokerClient.init(.{
        .host = getenv("BROKER_HOST") orelse "127.0.0.1",
        .port = envU16("BROKER_PORT", 7770),
    }, io);

    // Adapters
    var mtn = mtn_mod.MtnMomoAdapter.init(
        mtn_schema,
        require("MTN_BEARER_TOKEN"),
        require("MTN_CALLBACK_TOKEN"),
        &tx_store,
        &broker,
    );

    var airtel = airtel_mod.AirtelMoneyAdapter.init(
        airtel_schema,
        require("AIRTEL_BEARER_TOKEN"),
        &tx_store,
        &broker,
    );

    // Direction 2: consume loop (broker → MoMo dispatch)
    const mtn_api_host  = getenv("MTN_API_HOST")    orelse "127.0.0.1";
    const mtn_api_port  = envU16("MTN_API_PORT",  8090);
    const airtel_api_host = getenv("AIRTEL_API_HOST") orelse "127.0.0.1";
    const airtel_api_port = envU16("AIRTEL_API_PORT", 8091);

    var dispatch_ctx = DispatchCtx{
        .mtn          = &mtn,
        .airtel       = &airtel,
        .mtn_client   = ApiClient.init(mtn_api_host,    mtn_api_port,    io),
        .airtel_client = ApiClient.init(airtel_api_host, airtel_api_port, io),
        .alloc        = alloc,
    };
    var consume_args = ConsumeArgs{ .broker = &broker, .ctx = &dispatch_ctx, .alloc = alloc };
    const consume_thread = try std.Thread.spawn(.{}, consumeLoop, .{&consume_args});
    consume_thread.detach();

    // Router
    var router = Router.init(alloc);
    defer router.deinit();

    try router.add(.POST, "/collection/v1_0/requesttopay", &mtn, mtnTransferFn);
    try router.add(.POST, "/webhooks/mtn",                 &mtn, mtnCallbackFn);
    try router.add(.POST, "/v1/airtel/payment",            &airtel, airtelPaymentFn);
    try router.add(.POST, "/webhooks/airtel",              &airtel, airtelCallbackFn);

    var dummy: u8 = 0;
    try router.add(.GET, "/health", &dummy, healthFn);

    // HTTP server
    const listen_host = getenv("CONNECT_LISTEN_HOST") orelse "0.0.0.0";
    const listen_port = envU16("CONNECT_LISTEN_PORT", 8080);

    var server = http.Server.init(alloc, .{
        .host = listen_host,
        .port = listen_port,
    });

    std.log.info("OrusConnect listening on {s}:{d}  broker={s}:{d}", .{
        listen_host, listen_port,
        getenv("BROKER_HOST") orelse "127.0.0.1",
        envU16("BROKER_PORT", 7770),
    });

    var h = router.handler();
    try server.serve(&h);
}
