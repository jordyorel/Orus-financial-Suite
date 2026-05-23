// ISO 8583 Network Management — MTI 0800/0810.
//
// The switch (GIMAC/BEAC/BCEAO) uses 0800 messages to:
//   NMI 301 — Sign-On  : establish session, exchange MAC key (Field 96)
//   NMI 302 — Sign-Off : close session cleanly
//   NMI 801 — Echo     : heartbeat, keep TCP alive (sent every ~30s)
//   NMI 802 — Cutover  : end-of-day boundary, triggers reconciliation

const std    = @import("std");
const parser = @import("iso8583/parser.zig");

pub const IsoMessage = parser.IsoMessage;

pub const NMI = struct {
    pub const SIGNON:  []const u8 = "301";
    pub const SIGNOFF: []const u8 = "302";
    pub const ECHO:    []const u8 = "801";
    pub const CUTOVER: []const u8 = "802";
};

pub fn isNetworkMgmt(mti: [4]u8) bool {
    return std.mem.eql(u8, &mti, "0800") or std.mem.eql(u8, &mti, "0810");
}

// Build a 0810 response to an inbound 0800.
// Echoes STAN (F11) and NMI code (F70). RC=00 = accepted.
// key_material: if non-empty, set F96 (used at sign-on to deliver session key).
pub fn buildResponse(
    request:      *const IsoMessage,
    alloc:        std.mem.Allocator,
    key_material: []const u8,
) !IsoMessage {
    var resp = IsoMessage.init(alloc, "0810".*);
    errdefer resp.deinit();
    try resp.set(39, "00");
    if (request.get(11)) |stan| try resp.set(11, stan);
    if (request.get(70)) |nmi|  try resp.set(70, nmi);
    if (key_material.len > 0)   try resp.set(96, key_material);
    return resp;
}

// Build a 0800 request (outbound — heartbeat, sign-on, sign-off).
// `stan` is incremented in-place (wraps at 999999).
pub fn buildRequest(
    nmi:  []const u8,
    stan: *u32,
    alloc: std.mem.Allocator,
) !IsoMessage {
    stan.* = (stan.* + 1) % 1_000_000;
    var req = IsoMessage.init(alloc, "0800".*);
    errdefer req.deinit();
    var stan_buf: [6]u8 = undefined;
    _ = try std.fmt.bufPrint(&stan_buf, "{d:0>6}", .{stan.*});
    try req.set(11, &stan_buf);
    try req.set(70, nmi);
    return req;
}

// ── Tests ─────────────────────────────────────────────────────────────────────

test "buildRequest: echo" {
    const alloc = std.testing.allocator;
    var stan: u32 = 0;
    var req = try buildRequest(NMI.ECHO, &stan, alloc);
    defer req.deinit();
    try std.testing.expectEqualSlices(u8, "0800", &req.mti);
    try std.testing.expectEqualStrings("801", req.get(70).?);
    try std.testing.expectEqualStrings("000001", req.get(11).?);
    try std.testing.expectEqual(@as(u32, 1), stan);
}

test "buildResponse: echo" {
    const alloc = std.testing.allocator;
    var stan: u32 = 5;
    var req = try buildRequest(NMI.ECHO, &stan, alloc);
    defer req.deinit();
    var resp = try buildResponse(&req, alloc, "");
    defer resp.deinit();
    try std.testing.expectEqualSlices(u8, "0810", &resp.mti);
    try std.testing.expectEqualStrings("00", resp.get(39).?);
    try std.testing.expectEqualStrings("801", resp.get(70).?);
}

test "buildResponse: sign-on includes key material in F96" {
    const alloc = std.testing.allocator;
    var stan: u32 = 0;
    var req = try buildRequest(NMI.SIGNON, &stan, alloc);
    defer req.deinit();
    var resp = try buildResponse(&req, alloc, "test_key_32bytes_padding_xxxxxxx");
    defer resp.deinit();
    try std.testing.expectEqualStrings("301", resp.get(70).?);
    try std.testing.expect(resp.get(96) != null);
}
