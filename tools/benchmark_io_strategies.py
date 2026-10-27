#!/usr/bin/env python3
"""A/B test macOS pack preallocation and uncached source reads in disposable builds."""

import argparse
import difflib
import json
import os
import platform
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

from benchmark_io_floor import ROOT, measure_case, run_command, sha256
from benchmark_storage_formats import make_fixture


COUNTER_PREFIX = "PBE_IO_EXPERIMENT "
CASES = ("baseline", "preallocate", "source-nocache")


def source_hashes():
    """Protect all production source files, including current uncommitted work."""
    return {str(path.relative_to(ROOT)): sha256(path)
            for directory in ("src", "include")
            for path in sorted((ROOT / directory).rglob("*")) if path.is_file()}


def replace_once(text, before, after):
    if text.count(before) != 1:
        raise RuntimeError(f"diagnostic patch anchor is not unique: {before[:80]!r}")
    return text.replace(before, after, 1)


def preallocation_source(original):
    text = replace_once(original, "#include <algorithm>", "#include <algorithm>\n#include <atomic>")
    counters = '''
// Diagnostic counters: print once after workers stop, outside backup timing.
struct PreallocationCounters {
    std::atomic<std::uint64_t> packs{0}, allocated_bytes{0}, elapsed{0};
    ~PreallocationCounters() {
        if (packs.load())
            std::cerr << "PBE_IO_EXPERIMENT {\\"preallocated_packs\\":" << packs.load()
                      << ",\\"allocated_bytes\\":" << allocated_bytes.load()
                      << ",\\"preallocation_ns\\":" << elapsed.load() << "}\\n";
    }
};
PreallocationCounters preallocation_counters;
'''
    text = replace_once(text, "constexpr std::size_t kPackHeaderSize = 16;",
                        "constexpr std::size_t kPackHeaderSize = 16;\n" + counters)
    reservation = '''
            // Reserve within the timed constructor; retain the normal publication/sync protocol.
            const auto reserve_begin = Clock::now();
            fstore_t space{};
            space.fst_flags = F_ALLOCATEALL;
            space.fst_posmode = F_PEOFPOSMODE;
            space.fst_length = static_cast<off_t>(kPackTargetBytes +
                kPackWriteBatchObjects * kChunkSize + kPackMaxObjects * kObjectHeaderSize + kPackHeaderSize);
            if (::fcntl(fd, F_PREALLOCATE, &space) != 0)
                throw std::runtime_error(error_text("diagnostic pack preallocation"));
            if (space.fst_bytesalloc < space.fst_length)
                throw std::runtime_error("diagnostic pack reservation was incomplete");
            const auto reserve_ns = elapsed_ns(reserve_begin);
            measured.setup_ns += reserve_ns;
            preallocation_counters.packs.fetch_add(1, std::memory_order_relaxed);
            preallocation_counters.allocated_bytes.fetch_add(space.fst_bytesalloc, std::memory_order_relaxed);
            preallocation_counters.elapsed.fetch_add(reserve_ns, std::memory_order_relaxed);
'''
    anchor = "        try {\n            std::array<std::uint8_t, kPackHeaderSize> pack_header{};"
    text = replace_once(text, anchor,
                        "        try {\n" + reservation + "\n            std::array<std::uint8_t, kPackHeaderSize> pack_header{};")
    # Reservation must never publish an unused logical tail. Check before the existing fsync.
    marker = "        try {\n            maybe_failpoint(failpoint, \"after-temp-write\");"
    finalize = '''        try {
            struct stat actual{};
            if (::fstat(fd, &actual) != 0)
                throw std::runtime_error(error_text("inspect diagnostic pack length"));
            if (actual.st_size != static_cast<off_t>(bytes) && ::ftruncate(fd, bytes) != 0)
                throw std::runtime_error(error_text("truncate unused diagnostic pack tail"));
            maybe_failpoint(failpoint, "after-temp-write");'''
    return replace_once(text, marker, finalize)


def uncached_source(original):
    text = replace_once(original, "#include <cerrno>",
                        "#include <atomic>\n#include <iostream>\n#include <cerrno>")
    counters = '''namespace {
struct UncachedSourceCounters {
    std::atomic<std::uint64_t> files{0};
    ~UncachedSourceCounters() {
        if (files.load())
            std::cerr << "PBE_IO_EXPERIMENT {\\"uncached_source_files\\":" << files.load() << "}\\n";
    }
};
UncachedSourceCounters uncached_source_counters;
} // namespace
'''
    text = replace_once(text, "namespace pbe::detail {", "namespace pbe::detail {\n" + counters)
    marker = '            throw std::runtime_error("source changed between scan and read: " + entry.relative);'
    flag = '''
        // One variable only: preserve read sizes, buffers and source mutation checks.
        if (::fcntl(fd_, F_NOCACHE, 1) != 0)
            throw std::runtime_error(error_text("diagnostic source F_NOCACHE"));
        uncached_source_counters.files.fetch_add(1, std::memory_order_relaxed);'''
    return replace_once(text, marker, marker + "\n" + flag)


def build_variants(work, output):
    source = work / "diagnostic-source"
    source.mkdir()
    for directory in ("src", "include"):
        shutil.copytree(ROOT / directory, source / directory)
    shutil.copyfile(ROOT / "CMakeLists.txt", source / "CMakeLists.txt")
    (source / "tools").mkdir()
    shutil.copyfile(ROOT / "tools/benchmark_io_floor.cpp", source / "tools/benchmark_io_floor.cpp")
    paths = {"preallocate": "src/storage/object_store.cpp",
             "source-nocache": "src/pipeline/01_reader/source_file.cpp"}
    originals = {name: (source / name).read_text() for name in paths.values()}
    build = work / "diagnostic-build"
    configure = run_command(["cmake", "-S", source, "-B", build,
                             "-DCMAKE_BUILD_TYPE=Release", "-DPBE_BUILD_TESTS=OFF"])
    (output / "configure.log").write_text(configure.stdout + configure.stderr)
    binaries = {}
    for case, patcher in (("preallocate", preallocation_source), ("source-nocache", uncached_source)):
        for path, original in originals.items():
            (source / path).write_text(original)
        path = paths[case]
        original = originals[path]
        patched = patcher(original)
        (source / path).write_text(patched)
        diff = difflib.unified_diff(original.splitlines(True), patched.splitlines(True),
                                    fromfile="production/" + path, tofile="diagnostic/" + path)
        (output / f"{case}.patch").write_text("".join(diff))
        print(f"Building disposable {case} diagnostic binary", flush=True)
        result = run_command(["cmake", "--build", build, "--target", "paravault", "--parallel", "4"])
        (output / f"{case}-build.log").write_text(result.stdout + result.stderr)
        executable = work / ("paravault-" + case)
        shutil.copyfile(build / "paravault", executable)
        executable.chmod(0o700)
        binaries[case] = executable
    return binaries


def diagnostics(record, file_count):
    lines = [line.removeprefix(COUNTER_PREFIX)
             for line in record.get("backup_stderr", "").splitlines() if line.startswith(COUNTER_PREFIX)]
    values = [json.loads(line) for line in lines]
    case = record["case"]
    if case == "baseline":
        if values:
            raise RuntimeError("baseline unexpectedly enabled an I/O experiment")
        return {}
    if len(values) != 1:
        raise RuntimeError("diagnostic setting did not report successful application")
    result = values[0]
    if case == "preallocate":
        if result.get("preallocated_packs") != record["pack_files"] or result.get("allocated_bytes", 0) <= 0:
            raise RuntimeError("preallocation was not applied to every pack")
    elif result.get("uncached_source_files") != file_count:
        raise RuntimeError("F_NOCACHE was not applied to every source file")
    return result


def smoke_check(work, output, binaries, production):
    small = work / "smoke"
    small.mkdir()
    source, logical, count = make_fixture(small, 2, 1, 2)
    rows = []
    for case in CASES:
        binary = production if case == "baseline" else binaries[case]
        repo = small / case
        row = measure_case(case, 1, binary, None, source, None, repo, output, logical, 256)
        applied = diagnostics(row, count)
        rows.append({"case": case, "verified": row["verified"], "applied": applied,
                     "pack_bytes": row["pack_bytes"], "objects": row["objects"]})
        shutil.rmtree(repo)
    if len({row["pack_bytes"] for row in rows}) != 1 or len({row["objects"] for row in rows}) != 1:
        raise RuntimeError("diagnostic smoke check changed output layout/size")
    shutil.rmtree(small)
    (output / "smoke.json").write_text(json.dumps(rows, indent=2) + "\n")


def write_report(output, manifest, rows):
    baseline = median(r["backup_seconds"] for r in rows if r["case"] == "baseline")
    lines = ["# macOS I/O strategy experiments", "",
        f"Measured UTC: {manifest['started_utc']}. Source: {manifest['logical_bytes']:,} bytes; {manifest['files']} files.",
        f"Every case uses 4 CPU workers, 1 writer, pack-v2, 256 MiB credits, Zstd level 3; {manifest['repeat']} runs per case.", "",
        "| Case | Runs (s) | Median backup (s) | Time change vs baseline | Full verify median (s) | CPU median (s) | Packs median |",
        "|---|---|---:|---:|---:|---:|---:|"]
    for case in CASES:
        selected = [r for r in rows if r["case"] == case]
        seconds = median(r["backup_seconds"] for r in selected)
        cpu = median(r["user_cpu_seconds"] + r["system_cpu_seconds"] for r in selected)
        runs = " / ".join(f"{r['backup_seconds']:.3f}" for r in selected)
        lines.append(f"| {case} | {runs} | {seconds:.3f} | {(seconds / baseline - 1) * 100:+.2f}% | {median(r['verify_seconds'] for r in selected):.3f} | {cpu:.3f} | {median(r['pack_files'] for r in selected):.0f} |")
    lines.extend(["", "Negative time change means a shorter observed backup; these are local measurements, not statistical significance claims.", "",
        "## Recorded stage time", "",
        "| Case | Source read (s) | Setup incl. preallocation (s) | Staging write (s) | File fsync (s) | Hash (s) | Compression (s) |",
        "|---|---:|---:|---:|---:|---:|---:|"])
    keys = ("source_read_ms", "object_setup_ms", "object_staging_write_ms", "object_file_sync_ms", "hash_ms", "compression_ms")
    for case in CASES:
        selected = [r for r in rows if r["case"] == case]
        values = " | ".join(f"{median(r['stages'][key] for r in selected) / 1000:.3f}" for key in keys)
        lines.append(f"| {case} | {values} |")
    lines.extend(["", "## Method and limits", "",
        "- Each experiment changes one policy in a disposable production source copy. The shipping source/binary is unchanged. Patches and build logs are retained.",
        "- Preallocation reserves the 128 MiB pack target plus one maximum batch and headers using F_PREALLOCATE/F_ALLOCATEALL. Reservation time is included in backup/setup timing. Before the normal fsync, actual file length is checked and any unused logical tail is truncated. Unused reserved storage is not made persistent.",
        "- Source-nocache applies F_NOCACHE to every regular source file after opening/stat validation. Chunk size, std::vector buffers, writer caching, and mutation checks stay unchanged. It does not change verification-file caching.",
        "- Each variant fails if its fcntl operation fails, and reports application counters to stderr. The audit checks these counters against pack/source-file counts for each run.",
        "- Source files are generated once with os.urandom and individually named small documents. Fixture files are fsynced before the trial series. Every trial uses the same retained fixture and a fresh repository. Case order rotates across repeats.",
        "- All snapshots receive full verification outside backup timing. A small-fixture check also requires equal object counts and stored pack byte counts across variants.",
        "- Cache state, background load, thermals, and device state are uncontrolled. The current source/destination share one filesystem volume. Stage times overlap across threads and cannot be added as wall-clock slices.",
        "- Files, repositories, source copies, and diagnostic binaries are deleted after validation; run traces, summaries, patches and audits are retained.", ""])
    (output / "README.md").write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pbe", type=Path, default=ROOT / "build-release/paravault")
    parser.add_argument("--asset-count", type=int, default=20)
    parser.add_argument("--asset-mib", type=int, default=512)
    parser.add_argument("--small-files", type=int, default=400)
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("F_PREALLOCATE/F_NOCACHE experiments require macOS")
    if min(args.asset_count, args.asset_mib, args.repeat) < 1 or args.small_files < 0:
        parser.error("counts/sizes/repeat must be positive; small-files may be zero")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        parser.error("output must be new or empty")
    output.mkdir(parents=True, exist_ok=True)
    production = args.pbe.resolve()
    original_sources = source_hashes()
    manifest = {"started_utc": datetime.now(timezone.utc).isoformat(), "system": platform.platform(),
        "repeat": args.repeat, "orders": [], "production_binary_sha256": sha256(production),
        "source_sha256": original_sources, "fixture_cleanup_complete": False}
    records = []
    try:
        with tempfile.TemporaryDirectory(prefix="paravault-io-strategies-", dir="/private/tmp") as folder:
            work = Path(folder)
            manifest["owned_work_root"] = str(work)
            required = args.asset_count * args.asset_mib * 1024**2 * 3 + 1024**3
            if shutil.disk_usage(work).free < required:
                raise RuntimeError("insufficient free space")
            binaries = build_variants(work, output)
            manifest["diagnostic_binary_sha256"] = {case: sha256(path) for case, path in binaries.items()}
            smoke = output / "smoke"
            smoke.mkdir()
            smoke_check(work, smoke, binaries, production)
            source, logical, count = make_fixture(work, args.asset_count, args.asset_mib, args.small_files)
            for path in source.rglob("*"):
                if path.is_file():
                    with path.open("rb+") as file:
                        os.fsync(file.fileno())
            manifest.update({"logical_bytes": logical, "files": count,
                "source_and_destination_device": os.stat(source).st_dev, "fixture_synced_before_trials": True})
            (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            for repeat in range(1, args.repeat + 1):
                offset = (repeat - 1) % len(CASES)
                order = CASES[offset:] + CASES[:offset]
                manifest["orders"].append(order)
                for case in order:
                    binary = production if case == "baseline" else binaries[case]
                    with tempfile.TemporaryDirectory(prefix="repo-", dir=work) as repo_folder:
                        row = measure_case(case, repeat, binary, None, source, None,
                            Path(repo_folder) / "repo", output, logical, 256)
                        row["io_experiment"] = diagnostics(row, count)
                        records.append(row)
                        with (output / "audited-results.jsonl").open("a") as out:
                            out.write(json.dumps(row) + "\n")
            manifest.update({"runs": len(records), "all_runs_verified": all(r["verified"] for r in records)})
            write_report(output, manifest, records)
        manifest["fixture_cleanup_complete"] = True
    finally:
        manifest["production_sources_preserved"] = source_hashes() == original_sources
        manifest["production_binary_preserved"] = sha256(production) == manifest["production_binary_sha256"]
        manifest["owned_work_root_removed"] = not Path(manifest.get("owned_work_root", "/")).exists()
        manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    if not manifest["production_sources_preserved"] or not manifest["production_binary_preserved"]:
        raise RuntimeError("production source/binary changed during the experiment")
    print(f"Completed {len(records)} verified trials; owned fixture/builds/repos removed. Report: {output / 'README.md'}", flush=True)


if __name__ == "__main__":
    main()
