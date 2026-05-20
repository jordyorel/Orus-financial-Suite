const std = @import("std");
const http = @import("http_server.zig");

// Simple linear-scan router. Good for V1 (~20 routes).
// Routes are (method, path_prefix) → handler function pointer.

pub const RouteHandler = *const fn (*anyopaque, *http.HttpRequest, std.mem.Allocator) http.HttpResponse;

pub const Route = struct {
    method: http.Method,
    path: []const u8,
    handler: RouteHandler,
    ctx: *anyopaque,
};

pub const Router = struct {
    routes: std.ArrayList(Route),
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) Router {
        return .{ .routes = .empty, .alloc = alloc };
    }

    pub fn deinit(self: *Router) void {
        self.routes.deinit(self.alloc);
    }

    pub fn add(self: *Router, method: http.Method, path: []const u8, ctx: *anyopaque, h: RouteHandler) !void {
        try self.routes.append(self.alloc, .{ .method = method, .path = path, .handler = h, .ctx = ctx });
    }

    pub fn dispatch(self: *const Router, req: *http.HttpRequest, alloc: std.mem.Allocator) http.HttpResponse {
        for (self.routes.items) |route| {
            if (route.method == req.method and std.mem.eql(u8, route.path, req.path)) {
                return route.handler(route.ctx, req, alloc);
            }
        }
        return http.HttpResponse.notFound();
    }

    // Build an http.Handler that dispatches through this router.
    pub fn handler(self: *Router) http.Handler {
        return .{
            .ptr = self,
            .vtable = &.{ .handle = dispatchFn },
        };
    }

    fn dispatchFn(ptr: *anyopaque, req: *http.HttpRequest) http.HttpResponse {
        const self: *Router = @ptrCast(@alignCast(ptr));
        return self.dispatch(req, req.arena.allocator());
    }
};

test "Router: matches exact path" {
    const alloc = std.testing.allocator;
    var router = Router.init(alloc);
    defer router.deinit();

    const TestCtx = struct {
        called: bool = false,
        fn handle(ctx: *anyopaque, req: *http.HttpRequest, a: std.mem.Allocator) http.HttpResponse {
            _ = req;
            _ = a;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.called = true;
            return http.HttpResponse.ok("{}");
        }
    };
    var ctx = TestCtx{};
    try router.add(.POST, "/v2/payment", &ctx, TestCtx.handle);

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var req = http.HttpRequest{
        .arena = arena,
        .method = .POST,
        .path = "/v2/payment",
        .headers = .{},
        .body = "",
    };
    const resp = router.dispatch(&req, alloc);
    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(ctx.called);
}

test "Router: 404 on unknown path" {
    const alloc = std.testing.allocator;
    var router = Router.init(alloc);
    defer router.deinit();

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var req = http.HttpRequest{
        .arena = arena,
        .method = .POST,
        .path = "/unknown",
        .headers = .{},
        .body = "",
    };
    const resp = router.dispatch(&req, alloc);
    try std.testing.expectEqual(@as(u16, 404), resp.status);
}
