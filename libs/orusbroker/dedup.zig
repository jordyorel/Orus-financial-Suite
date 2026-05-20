// Deduplication filter — ring buffer of the last RING_SIZE processed msg_ids.
//
// O(N) scan per check; N=1024 → ~16 KB of state, fast enough for V1.
// Thread-safe via internal spinlock.

const std       = @import("std");
const mutex_mod = @import("mutex.zig");

pub const RING_SIZE: usize = 1024;

pub const DedupFilter = struct {
    ring:     [RING_SIZE][16]u8 = std.mem.zeroes([RING_SIZE][16]u8),
    occupied: [RING_SIZE]bool   = [_]bool{false} ** RING_SIZE,
    head:     usize             = 0,
    mutex:    mutex_mod.SpinMutex = .{},

    pub fn init() DedupFilter {
        return .{};
    }

    // Returns true if msg_id was already seen (duplicate).
    // Records msg_id and returns false if it is new.
    pub fn checkAndSet(self: *DedupFilter, msg_id: [16]u8) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (0..RING_SIZE) |i| {
            if (self.occupied[i] and std.mem.eql(u8, &self.ring[i], &msg_id)) return true;
        }

        self.ring[self.head] = msg_id;
        self.occupied[self.head] = true;
        self.head = (self.head + 1) % RING_SIZE;
        return false;
    }
};

test "dedup: new id is not a duplicate" {
    var d = DedupFilter.init();
    const id = [_]u8{1} ** 16;
    try std.testing.expect(!d.checkAndSet(id));
}

test "dedup: same id twice is a duplicate" {
    var d = DedupFilter.init();
    const id = [_]u8{2} ** 16;
    _ = d.checkAndSet(id);
    try std.testing.expect(d.checkAndSet(id));
}

test "dedup: different ids are not duplicates" {
    var d = DedupFilter.init();
    var id_a = [_]u8{0} ** 16;
    var id_b = [_]u8{0} ** 16;
    id_a[0] = 0xAA;
    id_b[0] = 0xBB;
    try std.testing.expect(!d.checkAndSet(id_a));
    try std.testing.expect(!d.checkAndSet(id_b));
}
