// MPSC bounded ring buffer — lock-free push, sequential pop.
//
// Multiple producers claim slots with a single fetch_add; the single consumer
// drains them in order.  Each slot carries a sequence number that acts as a
// generation counter, making the full/empty states unambiguous without any
// additional mutex.
//
// Slot lifecycle (global index k, slot position k % CAP):
//   seq == k         → slot is free; producer may claim it
//   seq == k + 1     → payload written; consumer may read it
//   seq == k + CAP   → consumer done; slot free for cycle k+CAP
//
// Capacity must be a power of two.  With 256 concurrent producers each
// holding at most one in-flight slot, CAP=512 provides 2× headroom.

const std = @import("std");

pub const CAP: usize = 512;

const Slot = struct {
    payload: []u8                  = &.{},
    seq:     std.atomic.Value(u64) = .init(0),
};

pub const Ring = struct {
    slots: [CAP]Slot,
    head:  std.atomic.Value(u64), // shared by all producers
    tail:  u64,                   // consumer-only, never shared

    pub fn init() Ring {
        var r = Ring{
            .slots = undefined,
            .head  = .init(0),
            .tail  = 0,
        };
        // Each slot starts free at its own cycle-zero index.
        for (&r.slots, 0..) |*s, i| s.seq = .init(i);
        return r;
    }

    // Push — may be called from any thread.
    // Spins only when the ring is full, which should not happen in practice
    // because the deliver thread drains faster than producers fill.
    pub fn push(self: *Ring, payload: []u8) void {
        const idx = self.head.fetchAdd(1, .monotonic);
        const slot = &self.slots[idx % CAP];
        // Wait until this slot's previous consumer cycle is done.
        while (slot.seq.load(.acquire) != idx) std.atomic.spinLoopHint();
        slot.payload = payload;
        slot.seq.store(idx + 1, .release);
    }

    // Pop — must only be called from the single consumer thread.
    // Returns null when the ring is empty.
    pub fn pop(self: *Ring) ?[]u8 {
        const slot = &self.slots[self.tail % CAP];
        if (slot.seq.load(.acquire) != self.tail + 1) return null;
        const payload = slot.payload;
        slot.seq.store(self.tail + CAP, .release); // free slot for next cycle
        self.tail += 1;
        return payload;
    }
};
