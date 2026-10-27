#!/usr/bin/env python3
"""Compare current backups, a compression ablation, and the production writer's I/O reference."""

import argparse
import difflib
import hashlib
import json
import os
import platform
import resource
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

from benchmark_storage_formats import TRACE_KEYS, make_fixture, parse_output


ROOT = Path(__file__).resolve().parents[1]


def run_command(args):
    result = subprocess.run([str(arg) for arg in args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"command failed: {args!r}\n{result.stdout}\n{result.stderr}")
    return result


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_ablation(work, output):
    """Change only compress_chunk in a disposable copy; never modify production sources."""
    source = work / "no-compression-source"
    source.mkdir()
    for directory in ("src", "include"):
        shutil.copytree(ROOT / directory, source / directory)
    shutil.copyfile(ROOT / "CMakeLists.txt", source / "CMakeLists.txt")
    (source / "tools").mkdir()
    shutil.copyfile(ROOT / "tools/benchmark_io_floor.cpp", source / "tools/benchmark_io_floor.cpp")
    codec = source / "src/pipeline/02_cpu/chunk_codec.cpp"
    before = codec.read_text()
    start = before.index("BlobWriteTask compress_chunk(")
    end = before.rindex("} // namespace pbe::detail")
    replacement = """// Diagnostic ablation only: hashing, dedup, writing, metadata and verification remain enabled.
BlobWriteTask compress_chunk(PipelineContext&, ChunkTask& task, const Digest& digest) {
    BlobWriteTask blob;
    blob.digest = digest;
    blob.raw_bytes = task.raw.size();
    blob.codec = 0;
    blob.stored = std::move(task.raw);
    return blob;
}
"""
    after = before[:start] + replacement + before[end:]
    codec.write_text(after)
    patch = difflib.unified_diff(before.splitlines(True), after.splitlines(True),
                                fromfile="production/chunk_codec.cpp",
                                tofile="diagnostic/chunk_codec.cpp")
    (output / "no-compression.patch").write_text("".join(patch))
    build = work / "no-compression-build"
    print("Building disposable no-compression diagnostic binary", flush=True)
    configure = run_command(["cmake", "-S", source, "-B", build,
                             "-DCMAKE_BUILD_TYPE=Release", "-DPBE_BUILD_TESTS=OFF"])
    compiled = run_command(["cmake", "--build", build, "--target", "paravault", "--parallel", "4"])
    (output / "diagnostic-build.log").write_text(configure.stdout + configure.stderr + compiled.stdout + compiled.stderr)
    return build / "paravault"


def copy_observations(repo, run_id, output, name):
    trace = repo / "reports" / f"{run_id}.jsonl"
    rows = [json.loads(line) for line in trace.read_text().splitlines() if line.strip()]
    shutil.copyfile(trace, output / f"{name}.jsonl")
    stages = {key: sum(row.get("observations", {}).get(key, 0) for row in rows) for key in TRACE_KEYS}
    assert stages["object_write_operation_count"] > 0
    return stages


def measure_case(case, repeat, binary, probe, source, inventory, repo, output, logical, memory):
    name = f"r{repeat}-{case}"
    run_command([binary, "init", "--repo", repo, "--storage-format", "pack-v2"])
    print(f"{name}: backup starting", flush=True)
    if case == "io-reference":
        result = run_command([probe, "run", "--repo", repo, "--source", source,
                              "--manifest", inventory, "--memory-mib", memory])
        record = json.loads(result.stdout)
        (output / f"{name}.probe.json").write_text(result.stdout)
        run_id = record["run_id"]
        record["workers"] = 0
    else:
        workers = 8 if case == "normal-w8" else 4
        before = resource.getrusage(resource.RUSAGE_CHILDREN)
        backup = run_command([binary, "backup", "--repo", repo, "--source", source,
                              "--workers", workers, "--writer-workers", "1",
                              "--memory-mib", memory, "--zstd-level", "3"])
        after = resource.getrusage(resource.RUSAGE_CHILDREN)
        snapshot = parse_output(r"^Snapshot:\s+(\S+)$", backup.stdout, "snapshot")
        run_id = parse_output(r"^Run ID:\s+(\S+)$", backup.stdout, "run id")
        summary_path = repo / "reports" / f"{run_id}.summary.json"
        summary = json.loads(summary_path.read_text())
        shutil.copyfile(summary_path, output / f"{name}.summary.json")
        verify_begin = time.perf_counter()
        verified = run_command([binary, "verify", "--repo", repo, "--snapshot", snapshot, "--full"])
        verify_seconds = time.perf_counter() - verify_begin
        stats = json.loads(run_command([binary, "stats", "--repo", repo, "--json"]).stdout)
        if "Verified: COMPLETE" not in verified.stdout or stats["logical_bytes"] != logical:
            raise RuntimeError("full snapshot verification failed")
        if summary["reused_chunks"] != 0:
            raise RuntimeError("unique-data comparison unexpectedly reused chunks")
        record = {
            "workers": workers, "backup_seconds": summary["elapsed_seconds"],
            "verify_seconds": verify_seconds, "verified": True,
            "logical_bytes": summary["logical_bytes"], "objects": stats["objects"],
            "user_cpu_seconds": after.ru_utime - before.ru_utime,
            "system_cpu_seconds": after.ru_stime - before.ru_stime,
            "memory_credit_peak_bytes": summary["memory_credit_peak_bytes"],
            "rss_peak_bytes": summary["process_rss_peak_bytes"], "summary": summary,
            "backup_stderr": backup.stderr,
        }
        if backup.stderr:
            (output / f"{name}.stderr.log").write_text(backup.stderr)
    if not record["verified"] or record["logical_bytes"] != logical:
        raise RuntimeError("I/O probe verification or logical size mismatch")
    if record["memory_credit_peak_bytes"] > memory * 1024**2:
        raise RuntimeError("memory credit budget exceeded")
    packs = list((repo / "objects" / "packs").glob("*.pack"))
    record.update({
        "case": case, "repeat": repeat, "writer_workers": 1,
        "pack_files": len(packs), "pack_bytes": sum(path.stat().st_size for path in packs),
        "stages": copy_observations(repo, run_id, output, name),
    })
    if case == "no-compression-w4" and record["stages"]["compression_ms"] != 0:
        raise RuntimeError("compression ablation unexpectedly performed compression")
    with (output / "results.jsonl").open("a") as out:
        out.write(json.dumps(record) + "\n")
    print(f"{name}: {record['backup_seconds']:.3f}s; verification {record['verify_seconds']:.3f}s; packs={len(packs)}; verified", flush=True)
    return record


def write_report(output, manifest, records):
    cases = ["io-reference", "normal-w4", "normal-w8", "no-compression-w4"]
    medians = {case: median(row["backup_seconds"] for row in records if row["case"] == case) for case in cases}
    lines = ["# 10 GiB incompressible-data I/O reference", "",
        f"Measured UTC: {manifest['started_utc']}. Fixture: {manifest['logical_bytes']:,} bytes, {manifest['files']} files.",
        f"Fresh pack-v2 repositories, 1 writer, {manifest['memory_mib']} MiB credits, Zstd level 3, {manifest['repeat']} runs per case.", "",
        "| Case | Runs (s) | Median backup (s) | Full verification median (s) | User + system CPU median (s) | Packs median |",
        "|---|---|---:|---:|---:|---:|"]
    for case in cases:
        rows = [row for row in records if row["case"] == case]
        times = " / ".join(f"{row['backup_seconds']:.3f}" for row in rows)
        cpu = median(row["user_cpu_seconds"] + row["system_cpu_seconds"] for row in rows)
        lines.append(f"| {case} | {times} | {medians[case]:.3f} | {median(row['verify_seconds'] for row in rows):.3f} | {cpu:.3f} | {median(row['pack_files'] for row in rows):.0f} |")
    lines.extend(["", "## Comparisons", "",
        f"- Normal 4-worker / I/O reference median ratio: {medians['normal-w4'] / medians['io-reference']:.3f}.",
        f"- Normal 8-worker speedup versus 4 workers: {medians['normal-w4'] / medians['normal-w8']:.3f}x.",
        f"- Compression ablation speedup versus normal 4 workers: {medians['normal-w4'] / medians['no-compression-w4']:.3f}x.", "",
        "## Recorded stage time", "",
        "Times below are medians of per-run sums across concurrent operations; they overlap and must not be added as wall-clock slices.", "",
        "| Case | Source read (s) | Hash (s) | Compression (s) | Staging write (s) | File fsync (s) | Metadata commit (s) |",
        "|---|---:|---:|---:|---:|---:|---:|"])
    fields = ("source_read_ms", "hash_ms", "compression_ms", "object_staging_write_ms", "object_file_sync_ms", "catalog_commit_ms")
    for case in cases:
        rows = [row for row in records if row["case"] == case]
        values = " | ".join(f"{median(row['stages'][key] for row in rows) / 1000:.3f}" for key in fields)
        lines.append(f"| {case} | {values} |")
    lines.extend(["", "## Method and limits", "",
        "- The I/O reference reuses the actual Reader file checks, bounded credits/queue, production Pack Writer, writev batching, 128 MiB pack target, 2 ms idle flush, fsync, link publication, directory sync, and monitor. Source and destination use the same filesystem volume.",
        "- Chunk digests are prepared before timing and retained in chunk-manifest.txt. The reference omits hashing/compression/dedup/indexing during timing. It is a lower-work I/O reference, not a functional backup or a proof of physical SSD maximum throughput.",
        "- Reference output headers use real source digests. Every stored object is read, decoded and digest-verified after timing. Engine runs additionally perform full snapshot/reference verification. Verification time is excluded from backup time for all cases.",
        "- The diagnostic no-compression binary changes only compress_chunk in a disposable source copy. It retains actual hashing, deduplication, durable storage, SQLite, and full verification; the patch and build log are retained. Production compression behavior is unchanged.",
        "- The fixture uses os.urandom assets and individually named small files; every chunk is unique. Digests are retained, but generated source files and temporary repositories are removed after validation.",
        "- Trial order alternates/rotates. Filesystem cache state, thermal state, background activity, and SSD internal state are uncontrolled. These are local observations, not universal speedup guarantees or a cold-cache test.",
        "- CPU sums for the reference cover only the timed pipeline; its offline preparation/verification are excluded. Stage times can overlap or include kernel waiting.", ""])
    (output / "README.md").write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--pbe", type=Path, default=ROOT / "build-release/paravault")
    parser.add_argument("--probe", type=Path, default=ROOT / "build-release/pbe_io_floor")
    parser.add_argument("--asset-count", type=int, default=20)
    parser.add_argument("--asset-mib", type=int, default=512)
    parser.add_argument("--small-files", type=int, default=400)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--memory-mib", type=int, default=256)
    args = parser.parse_args()
    if min(args.asset_count, args.asset_mib, args.repeat, args.memory_mib) < 1 or args.small_files < 0:
        parser.error("counts, sizes, repeat and memory must be positive; small-files may be zero")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        parser.error("output must be new or empty")
    output.mkdir(parents=True, exist_ok=True)
    pbe, probe = args.pbe.resolve(), args.probe.resolve()
    manifest = {"started_utc": datetime.now(timezone.utc).isoformat(),
        "system": platform.platform(), "logical_cpus": os.cpu_count(),
        "repeat": args.repeat, "memory_mib": args.memory_mib,
        "production_binary_sha256": sha256(pbe), "probe_binary_sha256": sha256(probe),
        "fixture_rng": "os.urandom", "fixture_cleanup_complete": False,
        "orders": [], "method": "fresh repositories; alternating/rotating case order; full verification outside backup timing"}
    records = []
    try:
        with tempfile.TemporaryDirectory(prefix="paravault-io-floor-", dir="/private/tmp") as temp:
            work = Path(temp)
            required = args.asset_count * args.asset_mib * 1024**2 * 3 + 1024**3
            if shutil.disk_usage(work).free < required:
                raise RuntimeError("insufficient free space for fixture and repository")
            diagnostic = build_ablation(work, output)
            manifest["diagnostic_binary_sha256"] = sha256(diagnostic)
            source, logical, count = make_fixture(work, args.asset_count, args.asset_mib, args.small_files)
            manifest.update({"logical_bytes": logical, "files": count,
                             "source_and_destination_device": os.stat(source).st_dev})
            inventory = output / "chunk-manifest.txt"
            print("Preparing unique chunk digests outside I/O benchmark timing", flush=True)
            prepared = run_command([probe, "prepare", "--source", source, "--manifest", inventory])
            print(prepared.stdout.strip(), flush=True)
            manifest["chunk_manifest_sha256"] = sha256(inventory)
            (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
            cases = ["io-reference", "normal-w4", "normal-w8", "no-compression-w4"]
            for repeat in range(1, args.repeat + 1):
                if repeat % 3 == 1: order = cases
                elif repeat % 3 == 2: order = list(reversed(cases))
                else: order = cases[1:] + cases[:1]
                manifest["orders"].append(order)
                for case in order:
                    binary = diagnostic if case == "no-compression-w4" else pbe
                    with tempfile.TemporaryDirectory(prefix="repo-", dir=work) as repo_dir:
                        repo = Path(repo_dir) / "repo"
                        record = measure_case(case, repeat, binary, probe, source, inventory,
                                              repo, output, logical, args.memory_mib)
                        records.append(record)
            manifest["all_runs_verified"] = all(row["verified"] for row in records)
            manifest["runs"] = len(records)
            write_report(output, manifest, records)
        manifest["fixture_cleanup_complete"] = True
    finally:
        manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Completed {len(records)} verified runs; fixture/repositories removed. Report: {output / 'README.md'}", flush=True)


if __name__ == "__main__":
    main()
