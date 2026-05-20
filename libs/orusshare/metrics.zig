const std = @import("std");

pub const Metrics = struct {
    messages_in: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    messages_out: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    serialize_errors: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    deserialize_errors: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    bytes_written: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    bytes_read: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn incIn(self: *Metrics) void {
        _ = self.messages_in.fetchAdd(1, .monotonic);
    }

    pub fn incOut(self: *Metrics) void {
        _ = self.messages_out.fetchAdd(1, .monotonic);
    }

    pub fn incSerializeError(self: *Metrics) void {
        _ = self.serialize_errors.fetchAdd(1, .monotonic);
    }

    pub fn incDeserializeError(self: *Metrics) void {
        _ = self.deserialize_errors.fetchAdd(1, .monotonic);
    }

    pub fn addBytesWritten(self: *Metrics, n: u64) void {
        _ = self.bytes_written.fetchAdd(n, .monotonic);
    }

    pub fn addBytesRead(self: *Metrics, n: u64) void {
        _ = self.bytes_read.fetchAdd(n, .monotonic);
    }

    pub fn snapshot(self: *const Metrics) Snapshot {
        return .{
            .messages_in = self.messages_in.load(.monotonic),
            .messages_out = self.messages_out.load(.monotonic),
            .serialize_errors = self.serialize_errors.load(.monotonic),
            .deserialize_errors = self.deserialize_errors.load(.monotonic),
            .bytes_written = self.bytes_written.load(.monotonic),
            .bytes_read = self.bytes_read.load(.monotonic),
        };
    }

    pub fn reset(self: *Metrics) void {
        self.messages_in.store(0, .monotonic);
        self.messages_out.store(0, .monotonic);
        self.serialize_errors.store(0, .monotonic);
        self.deserialize_errors.store(0, .monotonic);
        self.bytes_written.store(0, .monotonic);
        self.bytes_read.store(0, .monotonic);
    }

    pub const Snapshot = struct {
        messages_in: u64,
        messages_out: u64,
        serialize_errors: u64,
        deserialize_errors: u64,
        bytes_written: u64,
        bytes_read: u64,
    };
};

test "Metrics: counters start at zero" {
    var m = Metrics{};
    const s = m.snapshot();
    try std.testing.expectEqual(@as(u64, 0), s.messages_in);
    try std.testing.expectEqual(@as(u64, 0), s.messages_out);
    try std.testing.expectEqual(@as(u64, 0), s.serialize_errors);
    try std.testing.expectEqual(@as(u64, 0), s.deserialize_errors);
}

test "Metrics: increments are reflected in snapshot" {
    var m = Metrics{};
    m.incIn();
    m.incIn();
    m.incIn();
    m.incOut();
    m.incSerializeError();
    const s = m.snapshot();
    try std.testing.expectEqual(@as(u64, 3), s.messages_in);
    try std.testing.expectEqual(@as(u64, 1), s.messages_out);
    try std.testing.expectEqual(@as(u64, 1), s.serialize_errors);
}

test "Metrics: reset clears all counters" {
    var m = Metrics{};
    m.incIn();
    m.incOut();
    m.reset();
    const s = m.snapshot();
    try std.testing.expectEqual(@as(u64, 0), s.messages_in);
    try std.testing.expectEqual(@as(u64, 0), s.messages_out);
}
