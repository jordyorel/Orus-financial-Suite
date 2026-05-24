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

// BlockingMutex — OS-level mutex (pthread_mutex_t).
// Waiting threads are parked by the kernel instead of spinning, freeing CPU
// for threads that are actually doing work. Use this when the lock is held
// for I/O or when many threads (≥16) contend on the same critical section.
pub const BlockingMutex = struct {
    inner: std.c.pthread_mutex_t = .{},

    pub fn lock(self: *BlockingMutex) void {
        _ = std.c.pthread_mutex_lock(&self.inner);
    }

    pub fn unlock(self: *BlockingMutex) void {
        _ = std.c.pthread_mutex_unlock(&self.inner);
    }
};
