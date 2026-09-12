#!/usr/bin/env python3
"""End-to-end CLI test using an isolated temporary repository."""

import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
from pbe_dashboard import BackupManager, read_repository


def run(pbe: Path, *arguments: str, expected: int = 0) -> str:
    result = subprocess.run([str(pbe), *arguments], text=True, capture_output=True)
    if result.returncode != expected:
        raise AssertionError(
            f"{arguments!r}: expected exit {expected}, got {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


def tree_signature(root: Path) -> dict[str, tuple[str, bytes | str | None]]:
    result = {}
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            result[relative] = ("symlink", os.readlink(path))
        elif path.is_dir():
            result[relative] = ("directory", None)
        else:
            result[relative] = ("file", path.read_bytes())
    return result


def main() -> None:
    pbe = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="pbe-cli-smoke-") as temporary:
        root = Path(temporary)
        repo, source, target = root / "repo", root / "source", root / "restore"
        (source / "folder" / "empty").mkdir(parents=True)
        (source / "資料").mkdir()
        (source / "notes.txt").write_bytes(b"PBE CLI round trip\n")
        (source / ".hidden").write_bytes(b"")
        (source / "folder" / "data.bin").write_bytes(bytes(range(256)) * 4096)
        (source / "資料" / "報告.txt").write_bytes("Unicode 路徑\n".encode("utf-8"))
        os.symlink("notes.txt", source / "notes-link")
        expected_tree = tree_signature(source)

        run(pbe, "init", "--repo", str(repo))
        run(pbe, "init", "--repo", str(repo), expected=2)
        run(pbe, "stats", "--repo", str(repo), "--workers", "9", expected=2)
        run(pbe, "backup", "--repo", str(repo), "--source", str(source), "--json", expected=2)
        backup = run(pbe, "backup", "--repo", str(repo), "--source", str(source), "--workers", "2", "--writer-workers", "2", "--memory-mib", "8")
        snapshot = re.search(r"^Snapshot:\s+(\S+)$", backup, re.MULTILINE)
        run_id = re.search(r"^Run ID:\s+(\S+)$", backup, re.MULTILINE)
        if not snapshot or not run_id:
            raise AssertionError(f"backup output omitted identifiers:\n{backup}")
        snapshot_id, trace_id = snapshot.group(1), run_id.group(1)
        trace_rows = [
            json.loads(line)
            for line in (repo / "reports" / f"{trace_id}.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        if not trace_rows or trace_rows[0]["schema_version"] != 5:
            raise AssertionError("decision trace omitted the current schema version")
        stage_metrics = trace_rows[0]["observations"]
        if not {"cpu_compute_ratio", "controlled_cpu_ms", "hash_ms", "compression_ms", "decompression_ms", "object_verify_ms", "object_io_ms", "writer_workers", "source_read_ms", "catalog_commit_ms", "object_staging_write_ms", "object_file_sync_ms", "object_publish_ms", "object_directory_sync_ms", "object_write_total_ms", "object_write_count", "object_write_operation_count"}.issubset(stage_metrics):
            raise AssertionError(f"decision trace omitted stage timing fields: {stage_metrics}")
        if stage_metrics["writer_workers"] != 2:
            raise AssertionError(f"decision trace omitted configured object-writer count: {stage_metrics}")

        listed = run(pbe, "list", "--repo", str(repo))
        if snapshot_id not in listed:
            raise AssertionError("list omitted complete snapshot")
        shown = run(pbe, "show", "--repo", str(repo), "--snapshot", snapshot_id)
        if snapshot_id not in shown or not re.search(r"Symlinks:\s+1\b", shown):
            raise AssertionError("show omitted snapshot or symlink metadata")
        verified = run(pbe, "verify", "--repo", str(repo), "--snapshot", snapshot_id, "--full")
        if "Verified:" not in verified:
            raise AssertionError("verify did not report full validation")
        stats = json.loads(run(pbe, "stats", "--repo", str(repo), "--json"))
        if stats["snapshots"] != 1 or stats["objects"] < 1:
            raise AssertionError(f"unexpected repository stats: {stats}")
        explained = run(pbe, "explain", "--repo", str(repo), "--run", trace_id)
        if trace_id not in explained or "Observed diagnoses" not in explained:
            raise AssertionError("explain omitted the decision trace")
        explained_json = json.loads(run(pbe, "explain", "--repo", str(repo), "--run", trace_id, "--json"))
        if explained_json["run_id"] != trace_id or explained_json["windows"] < 1 or not explained_json["diagnoses"]:
            raise AssertionError(f"explain --json returned incomplete data: {explained_json}")

        run(pbe, "restore", "--repo", str(repo), "--snapshot", snapshot_id, "--target", str(target))
        if tree_signature(target) != expected_tree:
            raise AssertionError("restored tree differs from source")
        run(pbe, "restore", "--repo", str(repo), "--snapshot", snapshot_id, "--target", str(target), expected=2)

        run(pbe, "show", "--repo", str(repo), "--snapshot", "missing-snapshot", expected=2)
        run(pbe, "backup", "--repo", str(repo), "--source", str(source), "--scheduler", "invalid", expected=2)
        run(pbe, "backup", "--repo", str(repo), "--source", str(root), expected=2)

        benchmark_output = root / "benchmark-output"
        run(
            pbe,
            "benchmark",
            "--dataset",
            str(source),
            "--workers",
            "1",
            "--repeat",
            "1",
            "--memory-mib",
            "8",
            "--output",
            str(benchmark_output),
        )
        if not all((benchmark_output / name).is_file() for name in ("benchmark.csv", "benchmark.json", "benchmark.md")):
            raise AssertionError("benchmark command omitted one or more report artifacts")
        metrics_repo, metrics_source = root / "metrics-repo", root / "metrics-source"
        metrics_source.mkdir()
        (metrics_source / "payload.bin").write_bytes(b"verified object timing" * 65536)
        run(pbe, "init", "--repo", str(metrics_repo))
        run(pbe, "backup", "--repo", str(metrics_repo), "--source", str(metrics_source))
        reused = run(
            pbe,
            "backup",
            "--repo",
            str(metrics_repo),
            "--source",
            str(metrics_source),
            "--trust-metadata",
        )
        reused_run_id = re.search(r"^Run ID:\s+(\S+)$", reused, re.MULTILINE)
        if not reused_run_id:
            raise AssertionError(f"metadata-reuse backup omitted run ID:\n{reused}")
        reused_trace = [
            json.loads(line)
            for line in (metrics_repo / "reports" / f"{reused_run_id.group(1)}.jsonl").read_text(encoding="utf-8").splitlines()
        ]
        observations = [row["observations"] for row in reused_trace]
        if not observations or not any(row["object_verify_ms"] > 0 and row["object_io_ms"] > 0 for row in observations):
            raise AssertionError("trusted-metadata verification did not record object verification and I/O time")

        cached_repo, cached_source = root / "cached-repo", root / "cached-source"
        cached_source.mkdir()
        (cached_source / "data.bin").write_bytes(bytes(range(256)) * 512)
        run(pbe, "init", "--repo", str(cached_repo))
        cached_backup = run(pbe, "backup", "--repo", str(cached_repo), "--source", str(cached_source))
        cached_snapshot = re.search(r"^Snapshot:\s+(\S+)$", cached_backup, re.MULTILINE)
        if not cached_snapshot:
            raise AssertionError(f"cached backup output omitted snapshot id:\n{cached_backup}")
        with sqlite3.connect(cached_repo / "catalog.sqlite") as catalog:
            object_id = catalog.execute(
                "SELECT object_id FROM entry_chunks WHERE snapshot_id=? ORDER BY ordinal LIMIT 1",
                (cached_snapshot.group(1),),
            ).fetchone()[0]
        object_path = cached_repo / "objects" / object_id[:2] / f"{object_id}.obj"
        with object_path.open("r+b") as corrupted:
            corrupted.seek(64)
            byte = corrupted.read(1)
            corrupted.seek(64)
            corrupted.write(bytes([byte[0] ^ 1]))
        damaged_reuse = subprocess.run(
            [str(pbe), "backup", "--repo", str(cached_repo), "--source", str(cached_source), "--trust-metadata"],
            text=True,
            capture_output=True,
        )
        if damaged_reuse.returncode != 4 or not any(
            message in damaged_reuse.stderr for message in ("content digest mismatch", "decompression failed")
        ):
            raise AssertionError(
                "metadata-trusted backup did not reject a corrupted cached object:\n"
                f"exit {damaged_reuse.returncode}\nstdout:\n{damaged_reuse.stdout}\nstderr:\n{damaged_reuse.stderr}"
            )
        cached_list = run(pbe, "list", "--repo", str(cached_repo))
        if len(re.findall(r"^bkp-", cached_list, re.MULTILINE)) != 1:
            raise AssertionError("backup that found a corrupt cached object published a snapshot")

        unreadable_repo, unreadable_source = root / "unreadable-repo", root / "unreadable-source"
        unreadable_source.mkdir()
        (unreadable_source / "visible.txt").write_text("must not publish partial backup", encoding="utf-8")
        denied = unreadable_source / "denied"
        denied.mkdir()
        (denied / "unreadable.txt").write_text("must be discovered or fail", encoding="utf-8")
        run(pbe, "init", "--repo", str(unreadable_repo))
        os.chmod(denied, 0)
        try:
            if not os.access(denied, os.R_OK | os.X_OK):
                scan_result = subprocess.run(
                    [str(pbe), "backup", "--repo", str(unreadable_repo), "--source", str(unreadable_source)],
                    text=True,
                    capture_output=True,
                )
                if scan_result.returncode == 0:
                    raise AssertionError("source scan error published a partial snapshot")
                unreadable_list = run(pbe, "list", "--repo", str(unreadable_repo))
                if "No complete snapshots." not in unreadable_list:
                    raise AssertionError("failed source scan left a visible complete snapshot")
        finally:
            os.chmod(denied, 0o700)
    print("PBE CLI smoke test passed")


if __name__ == "__main__":
    main()
