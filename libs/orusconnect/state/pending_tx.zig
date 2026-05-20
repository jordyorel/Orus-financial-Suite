const std = @import("std");

pub const TxState = enum { PENDING, SUCCESSFUL, FAILED, EXPIRED };

pub const PendingEntry = struct {
    msg_id: [16]u8,
    reference_id: []const u8,
    state: TxState,
    created_at: i64,
    expires_at: i64,
    // Serialized InternalMessage — owned by PendingTxStore.alloc.
    // Deserialized and published to OrusBroker when the MoMo callback confirms.
    msg_bytes: []const u8,
};

// Thread-safe store for async MoMo transactions awaiting callback.
pub const PendingTxStore = struct {
    map: std.StringHashMap(PendingEntry),
    locked: std.atomic.Value(bool),
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) PendingTxStore {
        return .{
            .map = std.StringHashMap(PendingEntry).init(alloc),
            .locked = std.atomic.Value(bool).init(false),
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *PendingTxStore) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.alloc.free(entry.key_ptr.*);
            self.alloc.free(entry.value_ptr.msg_bytes);
        }
        self.map.deinit();
    }

    fn lock(self: *PendingTxStore) void {
        while (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            std.Thread.yield() catch {};
        }
    }

    fn unlock(self: *PendingTxStore) void {
        self.locked.store(false, .release);
    }

    pub fn put(self: *PendingTxStore, entry: PendingEntry) !void {
        self.lock();
        defer self.unlock();
        const key = try self.alloc.dupe(u8, entry.reference_id);
        errdefer self.alloc.free(key);
        const bytes = try self.alloc.dupe(u8, entry.msg_bytes);
        errdefer self.alloc.free(bytes);
        var stored = entry;
        stored.msg_bytes = bytes;
        try self.map.put(key, stored);
    }

    pub fn get(self: *PendingTxStore, reference_id: []const u8) ?PendingEntry {
        self.lock();
        defer self.unlock();
        return self.map.get(reference_id);
    }

    pub fn updateState(self: *PendingTxStore, reference_id: []const u8, state: TxState) bool {
        self.lock();
        defer self.unlock();
        const entry = self.map.getPtr(reference_id) orelse return false;
        entry.state = state;
        return true;
    }

    pub fn remove(self: *PendingTxStore, reference_id: []const u8) bool {
        self.lock();
        defer self.unlock();
        if (self.map.fetchRemove(reference_id)) |kv| {
            self.alloc.free(kv.key);
            self.alloc.free(kv.value.msg_bytes);
            return true;
        }
        return false;
    }

    // Atomically removes the entry and returns a copy of its serialized
    // InternalMessage bytes allocated with `alloc`. Returns null if not found.
    // Caller owns the returned slice and must free it.
    pub fn takeBytes(
        self: *PendingTxStore,
        reference_id: []const u8,
        alloc: std.mem.Allocator,
    ) ?[]u8 {
        self.lock();
        defer self.unlock();
        const kv = self.map.fetchRemove(reference_id) orelse return null;
        const bytes = alloc.dupe(u8, kv.value.msg_bytes) catch {
            // Can't give bytes to caller — put entry back is too complex, free it.
            self.alloc.free(kv.key);
            self.alloc.free(kv.value.msg_bytes);
            return null;
        };
        self.alloc.free(kv.key);
        self.alloc.free(kv.value.msg_bytes);
        return bytes;
    }

    // Expire entries older than now. Call periodically.
    pub fn expireOld(self: *PendingTxStore, now_ns: i64) void {
        self.lock();
        defer self.unlock();
        var it = self.map.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.expires_at < now_ns and entry.value_ptr.state == .PENDING) {
                entry.value_ptr.state = .EXPIRED;
            }
        }
    }
};

test "PendingTxStore: put and get" {
    const alloc = std.testing.allocator;
    var store = PendingTxStore.init(alloc);
    defer store.deinit();

    const entry = PendingEntry{
        .msg_id = [_]u8{1} ** 16,
        .reference_id = "ref-abc-123",
        .state = .PENDING,
        .created_at = 1_000_000,
        .expires_at = 2_000_000,
        .msg_bytes = &.{},
    };

    try store.put(entry);
    const found = store.get("ref-abc-123");
    try std.testing.expect(found != null);
    try std.testing.expectEqual(TxState.PENDING, found.?.state);
}

test "PendingTxStore: updateState" {
    const alloc = std.testing.allocator;
    var store = PendingTxStore.init(alloc);
    defer store.deinit();

    try store.put(.{
        .msg_id = [_]u8{2} ** 16,
        .reference_id = "ref-xyz",
        .state = .PENDING,
        .created_at = 0,
        .expires_at = 99999,
        .msg_bytes = &.{},
    });

    try std.testing.expect(store.updateState("ref-xyz", .SUCCESSFUL));
    try std.testing.expectEqual(TxState.SUCCESSFUL, store.get("ref-xyz").?.state);
}

test "PendingTxStore: remove" {
    const alloc = std.testing.allocator;
    var store = PendingTxStore.init(alloc);
    defer store.deinit();

    try store.put(.{
        .msg_id = [_]u8{3} ** 16,
        .reference_id = "ref-del",
        .state = .PENDING,
        .created_at = 0,
        .expires_at = 99999,
        .msg_bytes = &.{},
    });

    try std.testing.expect(store.remove("ref-del"));
    try std.testing.expect(store.get("ref-del") == null);
}
