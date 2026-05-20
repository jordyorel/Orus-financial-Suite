// SpinMutex — simple acquire/release spinlock.
// Mirrors the pattern used in libs/orusconnect/state/pending_tx.zig.
// Suitable for V1 where critical sections are short-lived.

const std = @import("std");

pub const SpinMutex = struct {
    locked: std.atomic.Value(bool) = .init(false),

    pub fn lock(self: *SpinMutex) void {
        while (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {}
    }

    pub fn unlock(self: *SpinMutex) void {
        self.locked.store(false, .release);
    }
};
