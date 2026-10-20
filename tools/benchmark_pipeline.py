#!/usr/bin/env python3
"""Measure real backup scaling on a retained fixture, with full verification per run."""

import argparse
import hashlib
import json
import os
import platform
import resource
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from statistics import median

from benchmark_storage_formats import TRACE_KEYS, parse_output


def command(args):
    result = subprocess.run([str(arg) for arg in args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"{args!r}\n{result.stdout}\n{result.stderr}")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pbe", type=Path, required=True)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", default="1,2,4,8")
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--writer-workers", type=int, default=2)
    parser.add_argument("--memory-mib", type=int, default=256)
    parser.add_argument("--zstd-level", type=int, default=3)
    parser.add_argument("--storage-format", choices=("files-v1", "pack-v2"), default="pack-v2")
    args = parser.parse_args()
    workers = [int(value) for value in args.workers.split(",")]
    if not workers or min(workers) < 1 or args.repeat < 1:
        parser.error("workers and repeats must be positive")
    pbe = args.pbe.resolve()
    dataset = args.dataset.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if (output / "results.jsonl").exists():
        parser.error("output already contains results; choose a fresh directory")
    files = [path for path in dataset.rglob("*") if path.is_file()]
    logical = sum(path.stat().st_size for path in files)
    manifest = {
        "label": args.label, "binary_sha256": hashlib.sha256(pbe.read_bytes()).hexdigest(),
        "system": platform.platform(), "logical_cpus": os.cpu_count(),
        "dataset": str(dataset), "logical_bytes": logical, "file_count": len(files),
        "workers": workers, "repeats": args.repeat, "writer_workers": args.writer_workers,
        "memory_mib": args.memory_mib, "zstd_level": args.zstd_level,
        "storage_format": args.storage_format,
        "method": "fresh repository per run; same retained fixture; alternating worker order; full object verification; no injected work",
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    records = []
    for repeat in range(1, args.repeat + 1):
        order = workers if repeat % 2 else list(reversed(workers))
        for count in order:
            name = f"r{repeat}-w{count}"
            print(f"{args.label} {name}: backup starting", flush=True)
            with tempfile.TemporaryDirectory(prefix="pbe-pipeline-repo-", dir="/private/tmp") as temp:
                repo = Path(temp) / "repo"
                command([pbe, "init", "--repo", repo, "--storage-format", args.storage_format])
                before = resource.getrusage(resource.RUSAGE_CHILDREN)
                started = time.perf_counter()
                backup = command([pbe, "backup", "--repo", repo, "--source", dataset,
                    "--workers", count, "--writer-workers", args.writer_workers,
                    "--memory-mib", args.memory_mib, "--zstd-level", args.zstd_level])
                wall = time.perf_counter() - started
                after = resource.getrusage(resource.RUSAGE_CHILDREN)
                snapshot = parse_output(r"^Snapshot:\s+(\S+)$", backup.stdout, "snapshot id")
                run = parse_output(r"^Run ID:\s+(\S+)$", backup.stdout, "run id")
                summary = json.loads((repo / "reports" / f"{run}.summary.json").read_text())
                trace = repo / "reports" / f"{run}.jsonl"
                rows = [json.loads(line) for line in trace.read_text().splitlines() if line.strip()]
                stages = {key: sum(row.get("observations", {}).get(key, 0) for row in rows) for key in TRACE_KEYS}
                shutil.copyfile(trace, output / f"{name}.jsonl")
                shutil.copyfile(repo / "reports" / f"{run}.summary.json", output / f"{name}.summary.json")
                sizes = [path.stat().st_size for path in (repo / "objects" / "packs").glob("*.pack")]
                started = time.perf_counter()
                verified = command([pbe, "verify", "--repo", repo, "--snapshot", snapshot, "--full"])
                verify_seconds = time.perf_counter() - started
                stats = json.loads(command([pbe, "stats", "--repo", repo, "--json"]).stdout)
                if "Verified: COMPLETE" not in verified.stdout or stats["logical_bytes"] != logical:
                    raise RuntimeError("full verification or logical-size check failed")
                record = {
                    "label": args.label, "repeat": repeat, "workers": count,
                    "backup_seconds": summary["elapsed_seconds"], "process_wall_seconds": wall,
                    "user_cpu_seconds": after.ru_utime - before.ru_utime,
                    "system_cpu_seconds": after.ru_stime - before.ru_stime,
                    "verify_seconds": verify_seconds, "verified": True,
                    "objects": stats["objects"], "logical_bytes": logical,
                    "memory_credit_peak_bytes": summary["memory_credit_peak_bytes"],
                    "rss_peak_bytes": summary["process_rss_peak_bytes"],
                    "pack_files": len(sizes), "tiny_packs": sum(size < 1024 for size in sizes),
                    "large_packs": sum(size >= 64 * 1024**2 for size in sizes),
                    "median_pack_bytes": median(sizes) if sizes else 0,
                    "stages": stages, "summary": summary,
                }
                records.append(record)
                with (output / "results.jsonl").open("a") as out:
                    out.write(json.dumps(record) + "\n")
                print(f"{args.label} {name}: {record['backup_seconds']:.3f}s; full verify {verify_seconds:.3f}s; packs={len(sizes)} tiny={record['tiny_packs']} large={record['large_packs']}", flush=True)
    lines = [f"# Pipeline scaling: {args.label}", "", f"Fixture: {logical:,} bytes; {len(files)} files. Full verification passed after every run.", "", "| CPU workers | Backup median (s) | Speedup vs 1 | Full verify median (s) | Packs | Tiny packs (<1 KiB) |", "|---:|---:|---:|---:|---:|---:|"]
    one = [record["backup_seconds"] for record in records if record["workers"] == 1]
    for count in workers:
        subset = [record for record in records if record["workers"] == count]
        seconds = median(record["backup_seconds"] for record in subset)
        speedup = f"{median(one) / seconds:.2f}x" if one else "n/a"
        lines.append(f"| {count} | {seconds:.3f} | {speedup} | {median(record['verify_seconds'] for record in subset):.3f} | {median(record['pack_files'] for record in subset):.0f} | {median(record['tiny_packs'] for record in subset):.0f} |")
    lines.extend(["", "The fixture is retained by the caller; temporary repositories were removed. Trace and summary files accompany each run. Filesystem caches and background activity are uncontrolled.", ""])
    (output / "README.md").write_text("\n".join(lines))


if __name__ == "__main__":
    main()
