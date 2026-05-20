// CDC §5 — "Zéro perte de transaction en cas de crash. WAL fsync."
// Vérifie : append, replay, détection CRC, récupération après crash simulé.

const std       = @import("std");
const orusbroker = @import("orusbroker");
const Wal        = orusbroker.Wal;

const testing = std.testing;

fn io_init(alloc: std.mem.Allocator) std.Io {
    var t = std.Io.Threaded.init(alloc, .{});
    return t.io();
}

fn cleanup(io: std.Io, path: []const u8) void {
    std.Io.Dir.cwd().deleteFile(io, path) catch {};
}

// Callback simple qui compte les entrées rejouées.
const Ctx = struct {
    count: usize = 0,
    last:  [64]u8 = undefined,
    last_len: usize = 0,

    fn cb(payload: []const u8, raw: ?*anyopaque) void {
        const self: *Ctx = @ptrCast(@alignCast(raw.?));
        self.count += 1;
        const n = @min(payload.len, self.last.len);
        @memcpy(self.last[0..n], payload[0..n]);
        self.last_len = n;
    }
};

test "WAL: append puis replay retrouve toutes les entrées" {
    const alloc = testing.allocator;
    const io    = io_init(alloc);
    const path  = "/tmp/orus_test_basic.wal";
    defer cleanup(io, path);

    {
        var w = try Wal.open(io, path);
        defer w.close();
        try w.append("entry_one");
        try w.append("entry_two");
        try w.append("entry_three");
    }

    // Réouverture = simulation de redémarrage après crash.
    var w2 = try Wal.open(io, path);
    defer w2.close();

    var ctx = Ctx{};
    try w2.replay(alloc, Ctx.cb, &ctx);
    try testing.expectEqual(@as(usize, 3), ctx.count);
}

test "WAL: replay préserve le contenu exact des payloads" {
    const alloc = testing.allocator;
    const io    = io_init(alloc);
    const path  = "/tmp/orus_test_content.wal";
    defer cleanup(io, path);

    const payload = "hello_orus_broker";
    {
        var w = try Wal.open(io, path);
        defer w.close();
        try w.append(payload);
    }

    var w2 = try Wal.open(io, path);
    defer w2.close();

    const Verifier = struct {
        fn cb(data: []const u8, raw: ?*anyopaque) void {
            _ = raw;
            std.testing.expectEqualSlices(u8, payload, data) catch unreachable;
        }
    };
    try w2.replay(alloc, Verifier.cb, null);
}

test "WAL: détecte une corruption CRC" {
    const alloc = testing.allocator;
    const io    = io_init(alloc);
    const path  = "/tmp/orus_test_crc.wal";
    defer cleanup(io, path);

    // Écrit une vraie entrée puis corrompt 4 octets dans le payload.
    {
        var w = try Wal.open(io, path);
        w.close();
    }
    {
        var w = try Wal.open(io, path);
        defer w.close();
        try w.append("data_to_corrupt");
    }

    // Corrompt le fichier directement (octet 13 = début du payload).
    const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer std.Io.File.close(file, io);
    // Octet 12 = premier octet du payload (après 12 octets d'en-tête).
    var corrupt: [1]u8 = .{0xFF};
    try std.Io.File.writePositionalAll(file, io, &corrupt, 12);

    var w3 = try Wal.open(io, path);
    defer w3.close();
    var ctx = Ctx{};
    try testing.expectError(error.Corrupt, w3.replay(alloc, Ctx.cb, &ctx));
}

test "WAL: 500 entrées — crash recovery complet (CDC §5)" {
    const alloc = testing.allocator;
    const io    = io_init(alloc);
    const path  = "/tmp/orus_test_bulk.wal";
    defer cleanup(io, path);

    const N: usize = 500;
    var buf: [32]u8 = undefined;

    // Phase écriture — pas de close propre = simule crash.
    {
        var w = try Wal.open(io, path);
        // Pas de defer close — le système ferme le fd à la fin du bloc.
        for (0..N) |i| {
            const s = try std.fmt.bufPrint(&buf, "msg_{d:0>5}", .{i});
            try w.append(s);
        }
        w.close(); // on ferme quand même pour flusher l'OS buffer.
    }

    // Phase replay depuis le début.
    var w2 = try Wal.open(io, path);
    defer w2.close();
    var ctx = Ctx{};
    try w2.replay(alloc, Ctx.cb, &ctx);
    try testing.expectEqual(N, ctx.count);
}

test "WAL: append après replay continue d'écrire à la bonne position" {
    const alloc = testing.allocator;
    const io    = io_init(alloc);
    const path  = "/tmp/orus_test_append_after.wal";
    defer cleanup(io, path);

    {
        var w = try Wal.open(io, path);
        defer w.close();
        try w.append("first");
    }

    var w2 = try Wal.open(io, path);
    defer w2.close();
    var ctx = Ctx{};
    try w2.replay(alloc, Ctx.cb, &ctx);
    try w2.append("second"); // append après replay

    // Troisième ouverture : doit voir les deux entrées.
    var w3 = try Wal.open(io, path);
    defer w3.close();
    var ctx2 = Ctx{};
    try w3.replay(alloc, Ctx.cb, &ctx2);
    try testing.expectEqual(@as(usize, 2), ctx2.count);
}
