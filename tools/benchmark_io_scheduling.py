#!/usr/bin/env python3
"""Compare real streaming/phased backups across bounded buffer budgets."""

import argparse
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

from benchmark_io_floor import ROOT, sha256
from benchmark_io_strategies import source_hashes
from benchmark_storage_formats import TRACE_KEYS, make_fixture, parse_output


def command(args):
    child = subprocess.Popen(list(map(str, args)), stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
    try:
        stdout, stderr = child.communicate()
    except BaseException:
        # Join the child before its temporary source/repository can be removed.
        child.terminate()
        try:
            child.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            child.kill()
            child.communicate()
        raise
    if child.returncode:
        raise RuntimeError(f"{args!r}\n{stdout}\n{stderr}")
    return subprocess.CompletedProcess(args, child.returncode, stdout, stderr)


def trial(binary, source, repo, output, repeat, strategy, memory, batch, logical):
    name = f"r{repeat}-{strategy}-m{memory}"
    command([binary, "init", "--repo", repo, "--storage-format", "pack-v2"])
    print(f"{name}: backup starting", flush=True)
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    backup = command([binary, "backup", "--repo", repo, "--source", source,
        "--workers", 4, "--writer-workers", 1, "--memory-mib", memory,
        "--io-strategy", strategy, "--io-batch-mib", batch if strategy == "phased" else 0])
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    (output / f"{name}.stdout").write_text(backup.stdout)
    (output / f"{name}.stderr").write_text(backup.stderr)
    run = parse_output(r"^Run ID:\s+(\S+)$", backup.stdout, "run")
    summary_path = repo / "reports" / f"{run}.summary.json"
    summary = json.loads(summary_path.read_text())
    shutil.copyfile(summary_path, output / f"{name}.summary.json")
    trace_path = repo / "reports" / f"{run}.jsonl"
    shutil.copyfile(trace_path, output / f"{name}.jsonl")
    windows = [json.loads(line) for line in trace_path.read_text().splitlines()]
    assert all(row["io_strategy"] == strategy for row in windows)
    stages = {key: sum(row["observations"].get(key, 0) for row in windows) for key in TRACE_KEYS}
    packs = list((repo / "objects/packs").glob("*.pack"))
    started = time.perf_counter()
    verified = command([binary, "verify", "--repo", repo, "--snapshot", summary["snapshot_id"], "--full"])
    verify_seconds = time.perf_counter() - started
    (output / f"{name}.verify.stdout").write_text(verified.stdout)
    if "Verified: COMPLETE" not in verified.stdout or summary["logical_bytes"] != logical:
        raise RuntimeError("full verification or logical-size mismatch")
    if summary["read_bytes"] != logical or summary["memory_credit_peak_bytes"] > memory << 20:
        raise RuntimeError("source reads or memory-credit accounting mismatch")
    if summary["io_strategy"] != strategy:
        raise RuntimeError("strategy was not applied")
    record = {"case": f"{strategy}-m{memory}", "repeat": repeat, "strategy": strategy,
        "memory_mib": memory, "requested_batch_mib": batch if strategy == "phased" else 0,
        "backup_seconds": summary["elapsed_seconds"], "verify_seconds": verify_seconds,
        "verified": True, "logical_bytes": logical,
        "user_cpu_seconds": after.ru_utime - before.ru_utime,
        "system_cpu_seconds": after.ru_stime - before.ru_stime,
        "pack_files": len(packs), "pack_bytes": sum(p.stat().st_size for p in packs),
        "source_device": source.stat().st_dev, "repo_device": repo.stat().st_dev,
        "summary": summary, "stages": stages}
    print(f"{name}: {record['backup_seconds']:.3f}s; fully verified", flush=True)
    return record


def write_report(output, manifest, rows):
    lines = ["# I/O scheduling comparison", "",
        f"{manifest['logical_bytes']:,} bytes; {manifest['files']} files; "
        f"{manifest['repeat']} repeats per case. Fixed 4 CPU workers / 1 writer, pack-v2.", "",
        "| Case | Runs (s) | Median (s) | Batch chunks | Packs (median) | Peak credits (MiB) |",
        "|---|---|---:|---:|---:|---:|"]
    for strategy, memory in manifest["cases"]:
        items = [r for r in rows if r["strategy"] == strategy and r["memory_mib"] == memory]
        if not items:
            continue
        times = " / ".join(f"{r['backup_seconds']:.3f}" for r in items)
        lines.append(f"| {strategy}-m{memory} | {times} | "
            f"{median(r['backup_seconds'] for r in items):.3f} | "
            f"{items[0]['summary']['io_batch_chunks']} | {median(r['pack_files'] for r in items):.0f} | "
            f"{max(r['summary']['memory_credit_peak_bytes'] for r in items)/2**20:.3f} |")
    lines += ["", "## Method", "",
        "- Both strategies use the same production hashing, compression, deduplication, queues, "
        "storage writer, SQLite and publication/sync behavior. Each trial starts a fresh repository "
        "and passes full verification outside timing. No injected delays/work or skipped compression.",
        "- Streaming dispatches each chunk immediately. Phased holds a bounded raw batch, dispatches "
        "it, and waits for every durable object ACK before reading the next batch. SQLite writes "
        "and OS background writeback can still overlap source reads; this is not device-wide I/O exclusion.",
        "- --memory-mib is an in-flight chunk credit budget, not a process RSS cap. A phased batch "
        "reserves raw + worst-case compressed credit per chunk. --io-batch-mib is an optional raw "
        "batch cap (0 = auto), further clamped by credits. Actual capacity is retained per run.",
        "- Phasing also changes pack arrival/flush timing and metadata batch frequency; pack counts "
        "and stage times are retained. A regression can reflect lost CPU/I/O overlap and extra syncs, "
        "rather than just source/write interference.",
        "- Cases rotate across repetitions; all results are retained. Caches, thermals and background "
        "load are uncontrolled. Filesystem device IDs are recorded; these do not identify separate physical SSDs.",
        "- The generated fixture and temporary output repositories are removed. A caller-supplied "
        "--dataset is preserved. Binary/source hashes and cleanup are recorded in manifest.json.", ""]
    (output / "README.md").write_text("\n".join(lines))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pbe", type=Path, default=ROOT / "build-release/paravault")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dataset", type=Path, help="reuse this tree; otherwise create a disposable 10 GiB fixture")
    parser.add_argument("--repo-parent", type=Path, help="parent for disposable repositories; can test another volume")
    parser.add_argument("--memory-mib", default="64,256", help="comma-separated credit budgets")
    parser.add_argument("--io-batch-mib", type=int, default=0, help="phased raw batch cap; 0 = auto")
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()
    try:
        memories = [int(v) for v in args.memory_mib.split(",")]
    except ValueError:
        parser.error("memory budgets must be integers")
    if not memories or len(set(memories)) != len(memories) or min(memories) < 3:
        parser.error("memory budgets must be distinct and at least 3 MiB")
    if args.repeat < 1 or args.io_batch_mib < 0:
        parser.error("repeat must be positive and batch cap nonnegative")
    output, binary = args.output.resolve(), args.pbe.resolve()
    if args.dataset:
        dataset = args.dataset.resolve()
        if not dataset.is_dir():
            parser.error("dataset must be a directory")
        if output == dataset or dataset in output.parents:
            parser.error("report output must be outside the dataset")
        if args.repo_parent:
            parent = args.repo_parent.resolve()
            if parent == dataset or dataset in parent.parents:
                parser.error("repository parent must be outside the dataset")
    if output.exists() and any(output.iterdir()):
        parser.error("output must be new or empty")
    output.mkdir(parents=True, exist_ok=True)
    original = source_hashes()
    cases = [(strategy, memory) for strategy in ("streaming", "phased") for memory in memories]
    manifest = {"started_utc": datetime.now(timezone.utc).isoformat(), "system": platform.platform(),
        "binary_sha256": sha256(binary), "source_sha256": original, "cases": cases,
        "harness_sha256": sha256(Path(__file__)),
        "repeat": args.repeat, "requested_batch_mib": args.io_batch_mib, "orders": [],
        "generated_fixture": args.dataset is None}
    rows, repositories = [], []
    try:
        with tempfile.TemporaryDirectory(prefix="paravault-io-scheduling-") as directory:
            work = Path(directory)
            manifest["owned_work_root"] = str(work)
            repo_parent = args.repo_parent.resolve() if args.repo_parent else work
            if not repo_parent.is_dir():
                raise RuntimeError("repository parent must already be a directory")
            if args.dataset:
                source = args.dataset.resolve()
                files = [p for p in source.rglob("*") if p.is_file() and not p.is_symlink()]
                logical, count = sum(p.stat().st_size for p in files), len(files)
            else:
                if shutil.disk_usage(work).free < 35 * 1024**3:
                    raise RuntimeError("need at least 35 GiB free for the generated fixture and repository")
                source, logical, count = make_fixture(work, 20, 512, 400)
                for path in source.rglob("*"):
                    if path.is_file():
                        with path.open("rb+") as file:
                            os.fsync(file.fileno())
            if not logical:
                raise RuntimeError("dataset must contain nonempty regular files")
            manifest.update({"dataset": str(source), "logical_bytes": logical, "files": count})
            if shutil.disk_usage(repo_parent).free < logical * 1.1 + (1 << 30):
                raise RuntimeError("insufficient repository free space")
            for repeat in range(1, args.repeat + 1):
                offset = (repeat - 1) % len(cases)
                order = cases[offset:] + cases[:offset]
                manifest["orders"].append(order)
                for strategy, memory in order:
                    with tempfile.TemporaryDirectory(prefix="repo-", dir=repo_parent) as destination:
                        repositories.append(Path(destination))
                        repo = Path(destination) / "vault"
                        row = trial(binary, source, repo, output, repeat, strategy, memory,
                                    args.io_batch_mib, logical)
                        rows.append(row)
                        with (output / "results.jsonl").open("a") as file:
                            file.write(json.dumps(row) + "\n")
            write_report(output, manifest, rows)
    finally:
        manifest.update({"finished_utc": datetime.now(timezone.utc).isoformat(),
            "completed_runs": len(rows),
            "all_runs_verified": len(rows) == len(cases) * args.repeat and all(r["verified"] for r in rows),
            "production_sources_preserved": source_hashes() == original,
            "binary_preserved": sha256(binary) == manifest["binary_sha256"],
            "owned_work_root_removed": not Path(manifest.get("owned_work_root", "/")).exists(),
            "repositories_removed": all(not path.exists() for path in repositories)})
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Completed {len(rows)} verified trials: {output / 'README.md'}", flush=True)


if __name__ == "__main__":
    main()
