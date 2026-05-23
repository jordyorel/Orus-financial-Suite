// Reconciliation engine — ISO 8583 MTI 0500/0510.
//
// ReconciliationState accumulates per-transaction totals in a thread-safe way.
// The BankServer calls record() for every D2 transaction, the Gateway calls it
// for every D1 transaction. When the bank sends a 0500, buildResponse() returns
// a ready-to-send 0510 IsoMessage.
//
// Totals are encoded in Field 60 (LLLVAR, 120 chars) as:
//   TX{count:06}{amount:012}RV{count:06}{amount:012}
//   TX = transactions (count + gross amount in smallest currency unit)
//   RV = reversals    (count + gross amount)
// This is a pragmatic encoding — replace with your switch's native format for
// production GIMAC certification (DE 68-73 in some dialects).

const std       = @import("std");
const orusshare = @import("orusshare");
const parser    = @import("iso8583/parser.zig");
const translator = @import("translator.zig");

pub const ReconciliationState = struct {
    tx_count:      std.atomic.Value(u32),
    tx_amount:     std.atomic.Value(i64),
    rev_count:     std.atomic.Value(u32),
    rev_amount:    std.atomic.Value(i64),

    pub fn init() ReconciliationState {
        return .{
            .tx_count  = std.atomic.Value(u32).init(0),
            .tx_amount = std.atomic.Value(i64).init(0),
            .rev_count = std.atomic.Value(u32).init(0),
            .rev_amount = std.atomic.Value(i64).init(0),
        };
    }

    // Record one processed transaction. Thread-safe (atomic ops, no lock).
    pub fn record(self: *ReconciliationState, msg: *const orusshare.InternalMessage) void {
        if (translator.isReversal(msg.mti)) {
            _ = self.rev_count.fetchAdd(1, .monotonic);
            _ = self.rev_amount.fetchAdd(msg.amount, .monotonic);
        } else if (!translator.isReconciliation(msg.mti)) {
            _ = self.tx_count.fetchAdd(1, .monotonic);
            _ = self.tx_amount.fetchAdd(msg.amount, .monotonic);
        }
    }

    // Reset counters (call at start of each reconciliation day).
    pub fn reset(self: *ReconciliationState) void {
        self.tx_count.store(0, .monotonic);
        self.tx_amount.store(0, .monotonic);
        self.rev_count.store(0, .monotonic);
        self.rev_amount.store(0, .monotonic);
    }

    // Build a 0510 response. Caller owns the result — call deinit().
    // Echoes STAN from the 0500 request (field 11) and sets RC=00.
    pub fn buildResponse(
        self: *const ReconciliationState,
        request: *const parser.IsoMessage,
        alloc: std.mem.Allocator,
    ) !parser.IsoMessage {
        var resp = parser.IsoMessage.init(alloc, "0510".*);
        errdefer resp.deinit();

        try resp.set(39, "00");
        if (request.get(11)) |stan| try resp.set(11, stan);

        // Totals in F60: "TX{count:06}{amount:012}RV{count:06}{amount:012}"
        var buf: [42]u8 = undefined;
        const totals = std.fmt.bufPrint(&buf,
            "TX{d:0>6}{d:0>12}RV{d:0>6}{d:0>12}",
            .{
                self.tx_count.load(.monotonic),
                @as(u64, @intCast(@max(0, self.tx_amount.load(.monotonic)))),
                self.rev_count.load(.monotonic),
                @as(u64, @intCast(@max(0, self.rev_amount.load(.monotonic)))),
            },
        ) catch return error.OutOfMemory;
        try resp.set(60, totals);

        return resp;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────────────

test "ReconciliationState: record transactions and reversals" {
    const alloc = std.testing.allocator;

    var state = ReconciliationState.init();

    const tx = orusshare.InternalMessage{
        .msg_id = [_]u8{1} ** 16, .schema_id = "gimac",
        .topic = "t", .origin = .rest_json, .provider = .mtn_momo,
        .mti = "0200".*, .fields = &.{}, .stan = [_]u8{0} ** 6,
        .pan_hash = 0, .amount = 10_000, .currency = "XAF".*,
        .received_at = 0, .source_ip = [_]u8{0} ** 16,
        .hop_count = 0, .external_id = null,
    };
    const rev = orusshare.InternalMessage{
        .msg_id = [_]u8{2} ** 16, .schema_id = "gimac",
        .topic = "t", .origin = .rest_json, .provider = .mtn_momo,
        .mti = "0420".*, .fields = &.{}, .stan = [_]u8{0} ** 6,
        .pan_hash = 0, .amount = 10_000, .currency = "XAF".*,
        .received_at = 0, .source_ip = [_]u8{0} ** 16,
        .hop_count = 0, .external_id = null,
    };

    state.record(&tx);
    state.record(&tx);
    state.record(&rev);

    try std.testing.expectEqual(@as(u32, 2), state.tx_count.load(.monotonic));
    try std.testing.expectEqual(@as(i64, 20_000), state.tx_amount.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), state.rev_count.load(.monotonic));
    try std.testing.expectEqual(@as(i64, 10_000), state.rev_amount.load(.monotonic));

    // Build 0510 response
    var req_iso = parser.IsoMessage.init(alloc, "0500".*);
    defer req_iso.deinit();
    try req_iso.set(11, "001234");

    var resp = try state.buildResponse(&req_iso, alloc);
    defer resp.deinit();

    try std.testing.expectEqualSlices(u8, "0510", &resp.mti);
    try std.testing.expectEqualStrings("00", resp.get(39).?);
    try std.testing.expectEqualStrings("001234", resp.get(11).?);

    // F60 should contain totals
    const f60 = resp.get(60).?;
    try std.testing.expect(std.mem.startsWith(u8, f60, "TX000002"));
    try std.testing.expect(std.mem.indexOf(u8, f60, "RV000001") != null);
}

test "ReconciliationState: reset clears all counters" {
    var state = ReconciliationState.init();
    const tx = orusshare.InternalMessage{
        .msg_id = [_]u8{1} ** 16, .schema_id = "gimac",
        .topic = "t", .origin = .rest_json, .provider = .mtn_momo,
        .mti = "0200".*, .fields = &.{}, .stan = [_]u8{0} ** 6,
        .pan_hash = 0, .amount = 5_000, .currency = "XAF".*,
        .received_at = 0, .source_ip = [_]u8{0} ** 16,
        .hop_count = 0, .external_id = null,
    };
    state.record(&tx);
    state.reset();
    try std.testing.expectEqual(@as(u32, 0), state.tx_count.load(.monotonic));
    try std.testing.expectEqual(@as(i64, 0), state.tx_amount.load(.monotonic));
}
