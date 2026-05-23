const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const orusshare_mod = b.addModule("orusshare", .{
        .root_source_file = b.path("libs/orusshare/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    const orusconnect_mod = b.addModule("orusconnect", .{
        .root_source_file = b.path("libs/orusconnect/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    orusconnect_mod.addImport("orusshare", orusshare_mod);

    const orusgateway_mod = b.addModule("orusgateway", .{
        .root_source_file = b.path("libs/orusgateway/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    orusgateway_mod.addImport("orusshare", orusshare_mod);

    const orusbroker_mod = b.addModule("orusbroker", .{
        .root_source_file = b.path("libs/orusbroker/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    orusbroker_mod.addImport("orusshare", orusshare_mod);

    // ── Executables ───────────────────────────────────────────────────────────

    const schemas_mod = b.addModule("schemas", .{
        .root_source_file = b.path("schemas/schemas.zig"),
        .target = target,
        .optimize = optimize,
    });

    const connect_app_mod = b.addModule("orusconnect-app", .{
        .root_source_file = b.path("apps/orusconnect/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    connect_app_mod.addImport("orusshare", orusshare_mod);
    connect_app_mod.addImport("orusconnect", orusconnect_mod);
    connect_app_mod.addImport("schemas", schemas_mod);

    const connect_exe = b.addExecutable(.{
        .name = "orusconnect",
        .root_module = connect_app_mod,
    });
    b.installArtifact(connect_exe);

    const run_connect = b.addRunArtifact(connect_exe);
    if (b.args) |args| run_connect.addArgs(args);
    const run_connect_step = b.step("run-connect", "Run OrusConnect");
    run_connect_step.dependOn(&run_connect.step);

    const gateway_app_mod = b.addModule("orusgateway-app", .{
        .root_source_file = b.path("apps/orusgateway/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    gateway_app_mod.addImport("orusshare", orusshare_mod);
    gateway_app_mod.addImport("orusgateway", orusgateway_mod);
    gateway_app_mod.addImport("schemas", schemas_mod);

    const gateway_exe = b.addExecutable(.{
        .name = "orusgateway",
        .root_module = gateway_app_mod,
    });
    b.installArtifact(gateway_exe);

    const run_gateway = b.addRunArtifact(gateway_exe);
    if (b.args) |args| run_gateway.addArgs(args);
    const run_step = b.step("run-gateway", "Run OrusGateway");
    run_step.dependOn(&run_gateway.step);

    const broker_app_mod = b.addModule("orusbroker-app", .{
        .root_source_file = b.path("apps/orusbroker/main.zig"),
        .target = target,
        .optimize = optimize,
    });
    broker_app_mod.addImport("orusbroker", orusbroker_mod);

    const broker_exe = b.addExecutable(.{
        .name = "orusbroker",
        .root_module = broker_app_mod,
    });
    b.installArtifact(broker_exe);

    const run_broker = b.addRunArtifact(broker_exe);
    if (b.args) |args| run_broker.addArgs(args);
    const run_broker_step = b.step("run-broker", "Run OrusBroker");
    run_broker_step.dependOn(&run_broker.step);

    // ── Demo ─────────────────────────────────────────────────────────────────
    const demo_mod = b.addModule("broker-demo", .{
        .root_source_file = b.path("tools/broker_demo.zig"),
        .target = target,
        .optimize = optimize,
    });
    demo_mod.addImport("orusshare",   orusshare_mod);
    demo_mod.addImport("orusbroker",  orusbroker_mod);
    demo_mod.addImport("orusconnect", orusconnect_mod);

    const demo_exe = b.addExecutable(.{ .name = "broker-demo", .root_module = demo_mod });
    const run_demo = b.addRunArtifact(demo_exe);
    const demo_step = b.step("demo", "Démonstration broker en temps réel");
    demo_step.dependOn(&run_demo.step);

    const full_demo_mod = b.addModule("full-pipeline-demo", .{
        .root_source_file = b.path("tools/full_pipeline_demo.zig"),
        .target = target,
        .optimize = optimize,
    });
    full_demo_mod.addImport("orusshare",   orusshare_mod);
    full_demo_mod.addImport("orusbroker",  orusbroker_mod);
    full_demo_mod.addImport("orusconnect", orusconnect_mod);
    full_demo_mod.addImport("orusgateway", orusgateway_mod);

    const full_demo_exe = b.addExecutable(.{ .name = "full-pipeline-demo", .root_module = full_demo_mod });
    const run_full_demo = b.addRunArtifact(full_demo_exe);
    const full_demo_step = b.step("demo-full", "Démonstration pipeline complet D1+D2");
    full_demo_step.dependOn(&run_full_demo.step);

    const pipeline_mod = b.addModule("pipeline-bench", .{
        .root_source_file = b.path("tools/pipeline_bench.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    pipeline_mod.addImport("orusshare",   orusshare_mod);
    pipeline_mod.addImport("orusbroker",  orusbroker_mod);
    pipeline_mod.addImport("orusconnect", orusconnect_mod);
    pipeline_mod.addImport("orusgateway", orusgateway_mod);

    const pipeline_exe = b.addExecutable(.{ .name = "pipeline-bench", .root_module = pipeline_mod });
    const run_pipeline = b.addRunArtifact(pipeline_exe);
    const pipeline_step = b.step("pipeline", "Pipeline live avec dashboard web (http://localhost:7780)");
    pipeline_step.dependOn(&run_pipeline.step);

    const battle_sim_mod = b.addModule("battle-test", .{
        .root_source_file = b.path("tools/battle_test.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    battle_sim_mod.addImport("orusshare",   orusshare_mod);
    battle_sim_mod.addImport("orusbroker",  orusbroker_mod);
    battle_sim_mod.addImport("orusconnect", orusconnect_mod);
    battle_sim_mod.addImport("orusgateway", orusgateway_mod);

    const battle_sim_exe = b.addExecutable(.{ .name = "battle-test", .root_module = battle_sim_mod });
    const run_battle_sim = b.addRunArtifact(battle_sim_exe);
    const battle_sim_step = b.step("battle", "Chaos battle test avec dashboard (http://localhost:7782)");
    battle_sim_step.dependOn(&run_battle_sim.step);

    const bench_mod = b.addModule("benchmark", .{
        .root_source_file = b.path("tools/benchmark.zig"),
        .target = target,
        .optimize = .ReleaseFast, // benchmark en mode optimisé
    });
    bench_mod.addImport("orusshare",   orusshare_mod);
    bench_mod.addImport("orusbroker",  orusbroker_mod);
    bench_mod.addImport("orusconnect", orusconnect_mod);

    const bench_exe = b.addExecutable(.{ .name = "benchmark", .root_module = bench_mod });
    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench", "Benchmark throughput + latence");
    bench_step.dependOn(&run_bench.step);

    const test_step = b.step("test", "Run all unit tests");

    // orusshare tests
    const orusshare_tests = b.addTest(.{ .root_module = orusshare_mod });
    test_step.dependOn(&b.addRunArtifact(orusshare_tests).step);

    // orusconnect tests
    const orusconnect_tests = b.addTest(.{ .root_module = orusconnect_mod });
    test_step.dependOn(&b.addRunArtifact(orusconnect_tests).step);

    // orusgateway tests
    const orusgateway_tests = b.addTest(.{ .root_module = orusgateway_mod });
    test_step.dependOn(&b.addRunArtifact(orusgateway_tests).step);

    // orusbroker tests
    const orusbroker_tests = b.addTest(.{ .root_module = orusbroker_mod });
    test_step.dependOn(&b.addRunArtifact(orusbroker_tests).step);

    // ── Tests supplémentaires (unit / integration / battle) ───────────────────

    const addTest = struct {
        fn f(
            bb: *std.Build,
            step: *std.Build.Step,
            tgt: std.Build.ResolvedTarget,
            opt: std.builtin.OptimizeMode,
            path: []const u8,
            imports: []const struct { name: []const u8, mod: *std.Build.Module },
        ) void {
            const mod = bb.addModule(path, .{
                .root_source_file = bb.path(path),
                .target = tgt,
                .optimize = opt,
            });
            for (imports) |imp| mod.addImport(imp.name, imp.mod);
            const t = bb.addTest(.{ .root_module = mod });
            step.dependOn(&bb.addRunArtifact(t).step);
        }
    }.f;

    // unit/test_wal.zig
    addTest(b, test_step, target, optimize, "tests/unit/test_wal.zig", &.{
        .{ .name = "orusbroker", .mod = orusbroker_mod },
    });

    // unit/test_dedup.zig
    addTest(b, test_step, target, optimize, "tests/unit/test_dedup.zig", &.{
        .{ .name = "orusbroker", .mod = orusbroker_mod },
    });

    // unit/test_iso_dialects.zig
    addTest(b, test_step, target, optimize, "tests/unit/test_iso_dialects.zig", &.{
        .{ .name = "orusgateway", .mod = orusgateway_mod },
        .{ .name = "schemas",     .mod = schemas_mod },
    });

    // integration/test_broker.zig
    addTest(b, test_step, target, optimize, "tests/integration/test_broker.zig", &.{
        .{ .name = "orusshare",  .mod = orusshare_mod },
        .{ .name = "orusbroker", .mod = orusbroker_mod },
    });

    // integration/test_bidirectional.zig
    addTest(b, test_step, target, optimize, "tests/integration/test_bidirectional.zig", &.{
        .{ .name = "orusshare",   .mod = orusshare_mod },
        .{ .name = "orusgateway", .mod = orusgateway_mod },
        .{ .name = "schemas",     .mod = schemas_mod },
    });

    // battle/test_concurrent.zig
    addTest(b, test_step, target, optimize, "tests/battle/test_concurrent.zig", &.{
        .{ .name = "orusshare",  .mod = orusshare_mod },
        .{ .name = "orusbroker", .mod = orusbroker_mod },
    });

    // Steps dédiés pour cibler une catégorie de tests.
    const unit_step = b.step("test-unit", "Run unit tests only");
    addTest(b, unit_step, target, optimize, "tests/unit/test_wal.zig",         &.{ .{ .name = "orusbroker", .mod = orusbroker_mod } });
    addTest(b, unit_step, target, optimize, "tests/unit/test_dedup.zig",        &.{ .{ .name = "orusbroker", .mod = orusbroker_mod } });
    addTest(b, unit_step, target, optimize, "tests/unit/test_iso_dialects.zig", &.{ .{ .name = "orusgateway", .mod = orusgateway_mod }, .{ .name = "schemas", .mod = schemas_mod } });

    const integ_step = b.step("test-integration", "Run integration tests only");
    addTest(b, integ_step, target, optimize, "tests/integration/test_broker.zig",       &.{ .{ .name = "orusshare", .mod = orusshare_mod }, .{ .name = "orusbroker", .mod = orusbroker_mod } });
    addTest(b, integ_step, target, optimize, "tests/integration/test_bidirectional.zig", &.{ .{ .name = "orusshare", .mod = orusshare_mod }, .{ .name = "orusgateway", .mod = orusgateway_mod }, .{ .name = "schemas", .mod = schemas_mod } });

    const battle_step = b.step("test-battle", "Run battle/stress tests only");
    addTest(b, battle_step, target, optimize, "tests/battle/test_concurrent.zig", &.{ .{ .name = "orusshare", .mod = orusshare_mod }, .{ .name = "orusbroker", .mod = orusbroker_mod } });
}
