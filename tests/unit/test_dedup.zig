// CDC §4 — "Exactement-once sémantique. Déduplication."
// Vérifie : overflow du ring, éviction des anciens IDs, thread safety.

const std        = @import("std");
const orusbroker = @import("orusbroker");
const DedupFilter = orusbroker.DedupFilter;
const RING_SIZE   = orusbroker.dedup.RING_SIZE;

const testing = std.testing;

fn makeId(n: u8) [16]u8 {
    var id = [_]u8{0} ** 16;
    id[0] = n;
    return id;
}

fn makeId16(n: u16) [16]u8 {
    var id = [_]u8{0} ** 16;
    id[0] = @intCast(n >> 8);
    id[1] = @intCast(n & 0xFF);
    return id;
}

test "DedupFilter: id neuf → false, même id → true" {
    var d = DedupFilter.init();
    const id = makeId(0x42);
    try testing.expect(!d.checkAndSet(id));
    try testing.expect(d.checkAndSet(id));
}

test "DedupFilter: RING_SIZE IDs distincts tous acceptés" {
    var d = DedupFilter.init();
    for (0..RING_SIZE) |i| {
        const id = makeId16(@intCast(i));
        try testing.expect(!d.checkAndSet(id));
    }
}

test "DedupFilter: overflow — le (RING_SIZE+1)ème ID évince le premier" {
    // Remplit exactement le ring avec les IDs 0..RING_SIZE-1,
    // puis insère RING_SIZE. L'ID 0 doit avoir été évincé et
    // être accepté comme nouveau.
    var d = DedupFilter.init();

    for (0..RING_SIZE) |i| {
        _ = d.checkAndSet(makeId16(@intCast(i)));
    }

    // ID RING_SIZE est nouveau → false.
    try testing.expect(!d.checkAndSet(makeId16(RING_SIZE)));

    // L'ID 0 a été évincé par l'écriture ci-dessus → false (neuf).
    try testing.expect(!d.checkAndSet(makeId16(0)));
}

test "DedupFilter: les IDs non évincés restent détectés comme doublons" {
    var d = DedupFilter.init();

    // Insère 10 IDs.
    for (0..10) |i| {
        _ = d.checkAndSet(makeId16(@intCast(i)));
    }

    // Ces 10 IDs doivent encore être reconnus comme doublons.
    for (0..10) |i| {
        try testing.expect(d.checkAndSet(makeId16(@intCast(i))));
    }
}

test "DedupFilter: accès concurrent — aucun data race (CDC §7)" {
    // 8 threads insèrent chacun RING_SIZE/8 IDs distincts.
    // Aucune panique ne doit survenir.
    const THREADS = 8;
    const PER_THREAD = RING_SIZE / THREADS;

    const Args = struct {
        d:      *DedupFilter,
        offset: usize,
    };

    const worker = struct {
        fn run(args: Args) void {
            for (0..PER_THREAD) |i| {
                const n: u16 = @intCast(args.offset + i);
                _ = args.d.checkAndSet(makeId16(n));
            }
        }
    };

    var d = DedupFilter.init();
    var threads: [THREADS]std.Thread = undefined;

    for (0..THREADS) |t| {
        const args = Args{ .d = &d, .offset = t * PER_THREAD };
        threads[t] = try std.Thread.spawn(.{}, worker.run, .{args});
    }
    for (&threads) |*th| th.join();
    // Aucun crash = succès.
}

test "DedupFilter: deux IDs qui diffèrent seulement sur le dernier octet" {
    var d = DedupFilter.init();
    const id_a = [_]u8{0xDE, 0xAD, 0xBE, 0xEF} ++ [_]u8{0} ** 12;
    var id_b = id_a;
    id_b[15] = 0x01;

    try testing.expect(!d.checkAndSet(id_a));
    try testing.expect(!d.checkAndSet(id_b));
    try testing.expect(d.checkAndSet(id_a));
    try testing.expect(d.checkAndSet(id_b));
}
