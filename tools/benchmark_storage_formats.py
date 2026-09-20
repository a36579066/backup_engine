#!/usr/bin/env python3
"""Compare files-v1 and pack-v2 on one large random fixture with full verification."""

import argparse
import json
import os
import platform
import re
import shutil
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median


TRACE_KEYS = (
    "object_setup_ms",
    "object_staging_write_ms",
    "object_file_sync_ms",
    "object_publish_ms",
    "object_directory_sync_ms",
    "object_cleanup_ms",
    "object_write_total_ms",
    "object_write_count",
    "object_write_operation_count",
    "source_read_ms",
    "catalog_commit_ms",
    "hash_ms",
    "compression_ms",
)


def run(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, check=check, capture_output=True, text=True)


def parse_output(pattern: str, text: str, label: str) -> str:
    match = re.search(pattern, text, re.MULTILINE)
    if not match:
        raise RuntimeError(f"PBE output omitted {label}:\n{text}")
    return match.group(1)


def make_fixture(root: Path, asset_count: int, asset_mib: int, small_files: int) -> tuple[Path, int, int]:
    source = root / "source"
    source.mkdir()
    asset_bytes = asset_mib * 1024 * 1024
    for index in range(asset_count):
        path = source / "assets" / f"asset-{index:02d}.bin"
        path.parent.mkdir(parents=True, exist_ok=True)
        remaining = asset_bytes
        with path.open("wb") as out:
            while remaining:
                amount = min(8 * 1024 * 1024, remaining)
                out.write(os.urandom(amount))
                remaining -= amount
        print(f"created random asset {index + 1}/{asset_count}", flush=True)

    small_dir = source / "small"
    small_dir.mkdir()
    small_size = 58
    for index in range(small_files):
        prefix = f"small-file-index={index:06d}; "
        payload = prefix.encode() + b"x" * (small_size - len(prefix))
        (small_dir / f"doc-{index:06d}.txt").write_bytes(payload)
    # Keep a final small file so the fixture remains useful with zero small_files.
    manifest_prefix = b"PBE storage comparison fixture\n"
    (source / "manifest.txt").write_bytes(manifest_prefix + b"m" * (341 - len(manifest_prefix)))
    total_bytes = sum(item.stat().st_size for item in source.rglob("*") if item.is_file())
    files = sum(1 for item in source.rglob("*") if item.is_file())
    return source, total_bytes, files


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pbe", type=Path, default=Path("build-release/pbe"))
    parser.add_argument("--output", type=Path, default=Path("reports/storage-format-benchmark"))
    parser.add_argument("--asset-count", type=int, default=20)
    parser.add_argument("--asset-mib", type=int, default=512)
    parser.add_argument("--small-files", type=int, default=400)
    parser.add_argument("--repeat", type=int, default=2)
    parser.add_argument("--cpu-workers", type=int, default=4)
    parser.add_argument("--memory-mib", type=int, default=256)
    args = parser.parse_args()
    if min(args.asset_count, args.asset_mib, args.repeat, args.cpu_workers, args.memory_mib) < 1 or args.small_files < 0:
        parser.error("asset count, asset size, repeats, workers, and memory must be positive; small-files may be zero")

    pbe = args.pbe.resolve()
    if not pbe.is_file():
        parser.error(f"PBE executable does not exist: {pbe}")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix="pbe-storage-compare-", dir="/private/tmp"))
    try:
        source, logical_bytes, file_count = make_fixture(temporary, args.asset_count, args.asset_mib, args.small_files)
        cases: list[tuple[str, int]] = []
        for _ in range(args.repeat):
            cases.extend((("files-v1", 2), ("pack-v2", 2)))
        cases.insert(2, ("pack-v2", 1))
        runs = []
        for number, (storage_format, writer_workers) in enumerate(cases, 1):
            repo = temporary / f"repo-{number}"
            run([str(pbe), "init", "--repo", str(repo), "--storage-format", storage_format])
            print(
                f"run {number}/{len(cases)}: {storage_format}, CPU workers={args.cpu_workers}, writers={writer_workers}",
                flush=True,
            )
            backup_begin = time.perf_counter()
            backup = run([
                str(pbe), "backup", "--repo", str(repo), "--source", str(source),
                "--workers", str(args.cpu_workers), "--writer-workers", str(writer_workers),
                "--memory-mib", str(args.memory_mib),
            ])
            external_backup_seconds = time.perf_counter() - backup_begin
            snapshot = parse_output(r"^Snapshot:\s+(\S+)$", backup.stdout, "snapshot id")
            run_id = parse_output(r"^Run ID:\s+(\S+)$", backup.stdout, "run id")
            summary = json.loads((repo / "reports" / f"{run_id}.summary.json").read_text())
            trace_path = repo / "reports" / f"{run_id}.jsonl"
            trace_rows = [json.loads(line) for line in trace_path.read_text().splitlines() if line.strip()]
            stage_totals = {
                key: sum(row.get("observations", {}).get(key, 0) for row in trace_rows)
                for key in TRACE_KEYS
            }
            trace_name = f"run-{number}-{storage_format}.jsonl"
            shutil.copyfile(trace_path, output / trace_name)

            verify_begin = time.perf_counter()
            verified = run([str(pbe), "verify", "--repo", str(repo), "--snapshot", snapshot, "--full"])
            verify_seconds = time.perf_counter() - verify_begin
            stats = json.loads(run([str(pbe), "stats", "--repo", str(repo), "--json"]).stdout)
            packs = len(list((repo / "objects" / "packs").glob("*.pack"))) if storage_format == "pack-v2" else 0
            record = {
                "run": number,
                "storage_format": storage_format,
                "cpu_workers": args.cpu_workers,
                "writer_workers": writer_workers,
                "snapshot_id": snapshot,
                "run_id": run_id,
                "external_backup_seconds": external_backup_seconds,
                "reported_backup_seconds": summary["elapsed_seconds"],
                "full_verify_seconds": verify_seconds,
                "verified": "Verified: COMPLETE" in verified.stdout,
                "logical_bytes": stats["logical_bytes"],
                "physical_bytes": stats["physical_bytes"],
                "objects": stats["objects"],
                "referenced_objects": stats["referenced_objects"],
                "pack_files": packs,
                "trace_windows": len(trace_rows),
                "memory_credit_peak_bytes": summary["memory_credit_peak_bytes"],
                "process_rss_peak_bytes": summary["process_rss_peak_bytes"],
                "stage_totals": stage_totals,
                "trace_file": trace_name,
            }
            if not record["verified"] or stats["logical_bytes"] != logical_bytes:
                raise RuntimeError(f"run {number} failed full verification or logical-size check")
            runs.append(record)
            print(
                f"verified run {number}: backup={summary['elapsed_seconds']:.3f}s, "
                f"full verify={verify_seconds:.3f}s, pack files={packs}",
                flush=True,
            )
            shutil.rmtree(repo)

        results = {
            "date_utc": datetime.now(timezone.utc).isoformat(),
            "system": platform.platform(),
            "fixture": {
                "bytes": logical_bytes,
                "gib": logical_bytes / (1024**3),
                "files": file_count,
                "temporary_fixture_removed": True,
            },
            "design": (
                "same generated random source; alternating files-v1 (4 CPU/2 writers) and "
                "pack-v2 (4 CPU/2 writers), with one pack-v2 single-writer diagnostic run; "
                "pack-v2 streams to a 128 MiB target (256-object cap) and flushes after 2 ms idle; "
                "fresh repository and full verification for every run; "
                "stage times are summed across trace windows"
            ),
            "runs": runs,
        }
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")

        markdown = [
            "# Storage-format benchmark",
            "",
            f"Dataset: {logical_bytes:,} bytes ({logical_bytes / (1024**3):.3f} GiB), {file_count} files.",
            "",
            "Each run used the same source fixture, a fresh repository, the selected CPU/memory settings, and full object verification. The order alternated files-v1 and pack-v2 with two writers; one pack-v2 run used one writer. Pack-v2 streams toward a 128 MiB segment target, caps segments at 256 objects, and flushes after 2 ms without a new object. Stage timings are summed across worker threads.",
            "",
            "| Format | Writers | Runs | Backup median (s) | Full verify median (s) | File fsync (s) | Write operations | Objects written |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for storage_format, writer_workers in (("files-v1", 2), ("pack-v2", 1), ("pack-v2", 2)):
            subset = [r for r in runs if r["storage_format"] == storage_format and r["writer_workers"] == writer_workers]
            if not subset:
                continue
            markdown.append(
                f"| {storage_format} | {writer_workers} | {len(subset)} | "
                f"{median(r['reported_backup_seconds'] for r in subset):.3f} | "
                f"{median(r['full_verify_seconds'] for r in subset):.3f} | "
                f"{median(r['stage_totals']['object_file_sync_ms'] for r in subset) / 1000:.3f} | "
                f"{median(r['stage_totals']['object_write_operation_count'] for r in subset):.0f} | "
                f"{median(r['stage_totals']['object_write_count'] for r in subset):.0f} |"
            )
        markdown.extend([
            "",
            "## Per-run measurements",
            "",
            "| Run | Format | Writers | Backup (s) | Verify (s) | Objects | Write operations | File fsync (s) | Pack files | Peak credits (MiB) | RSS peak (MiB) | Verified |",
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|",
        ])
        for r in runs:
            stage = r["stage_totals"]
            markdown.append(
                f"| {r['run']} | {r['storage_format']} | {r['writer_workers']} | {r['reported_backup_seconds']:.3f} | "
                f"{r['full_verify_seconds']:.3f} | {stage['object_write_count']:.0f} | "
                f"{stage['object_write_operation_count']:.0f} | {stage['object_file_sync_ms'] / 1000:.3f} | "
                f"{r['pack_files']} | {r['memory_credit_peak_bytes'] / (1024**2):.2f} | "
                f"{r['process_rss_peak_bytes'] / (1024**2):.2f} | {'yes' if r['verified'] else 'no'} |"
            )
        markdown.extend([
            "",
            "Raw JSON Lines traces are in `run-*.jsonl`, with aggregate measurements in `results.json`. The temporary fixture and repositories are removed after the run. Filesystem cache and background load are uncontrolled; treat these as local measurements rather than a general performance guarantee.",
            "",
        ])
        (output / "README.md").write_text("\n".join(markdown))
        print(f"wrote benchmark report to {output}", flush=True)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


if __name__ == "__main__":
    main()
