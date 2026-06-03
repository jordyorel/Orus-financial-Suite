// En Zig, tout fichier commence par importer la bibliothèque standard
const std = @import("std");

// Zig appelle cette fonction quand tu fais `zig build`
// b = le "builder" — l'objet qui sait comment compiler des choses
pub fn build(b: *std.Build) void {

    // "Pour quelle machine on compile ?" — par défaut : la tienne
    // `zig build -Dtarget=aarch64-linux` pour cross-compiler
    const target = b.standardTargetOptions(.{});

    // "Avec quel niveau d'optimisation ?"
    // Debug (défaut) = rapide à compiler, lent à exécuter, erreurs détaillées
    // ReleaseFast    = lent à compiler, très rapide, peu d'erreurs de runtime
    const optimize = b.standardOptimizeOption(.{});

    // ── Étape 1 : Déclarer orusshare comme module réutilisable ────────────────
    //
    // Un "module" en Zig = un ensemble de fichiers .zig qu'on peut importer
    // root_source_file = le fichier d'entrée du module (comme index.js)
    const orusshare_mod = b.addModule("orusshare", .{
        .root_source_file = b.path("libs/orusshare/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ── Étape 2 : Déclarer orusbroker comme module ───────────────────────────
    const orusbroker_mod = b.addModule("orusbroker", .{
        .root_source_file = b.path("libs/orusbroker/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    // orusbroker a besoin de orusshare — on déclare cette dépendance
    // Après ça, dans orusbroker on peut faire : @import("orusshare")
    orusbroker_mod.addImport("orusshare", orusshare_mod);

    // ── Étape 3 : Créer un exécutable orusbroker ─────────────────────────────
    //
    // addExecutable() dit "compile ce module en binaire exécutable"
    const broker_exe = b.addExecutable(.{
        .name = "orusbroker",           // le nom du binaire final
        .root_module = orusbroker_mod,  // le module à compiler
    });
    // installArtifact() = "mets le binaire dans zig-out/bin/ après compilation"
    b.installArtifact(broker_exe);

    // ── Étape 4 : Créer une commande `zig build run-broker` ──────────────────
    const run_broker = b.addRunArtifact(broker_exe);
    // Si l'utilisateur passe des args (zig build run-broker -- --port 8080)
    if (b.args) |args| run_broker.addArgs(args);
    // Crée le step nommé "run-broker"
    const run_broker_step = b.step("run-broker", "Run OrusBroker");
    run_broker_step.dependOn(&run_broker.step);

    // ── Étape 5 : Créer une commande `zig build test` ─────────────────────────
    const test_step = b.step("test", "Run all tests");

    // Ajouter les tests d'orusshare
    const orusshare_tests = b.addTest(.{ .root_module = orusshare_mod });
    test_step.dependOn(&b.addRunArtifact(orusshare_tests).step);

    // Ajouter les tests d'orusbroker
    const orusbroker_tests = b.addTest(.{ .root_module = orusbroker_mod });
    test_step.dependOn(&b.addRunArtifact(orusbroker_tests).step);
}
