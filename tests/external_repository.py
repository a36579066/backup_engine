#!/usr/bin/env python3
"""Exercise real external-filesystem publication and process-crash recovery."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time


class Runner:
    def __init__(self, engine, output):
        self.engine = engine
        self.output = output
        self.sequence = 0
        self.records = []

    def run(self, label, command, expected=0):
        self.sequence += 1
        command = [str(value) for value in command]
        started = time.perf_counter()
        result = subprocess.run(command, capture_output=True, text=True, timeout=120)
        record = {
            "command": command,
            "returncode": result.returncode,
            "expected_returncode": expected,
            "wall_seconds": time.perf_counter() - started,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
        self.records.append(record)
        path = self.output / f"{self.sequence:03d}-{label}.json"
        path.write_text(json.dumps(record, indent=2) + "\n")
        if result.returncode != expected:
            raise RuntimeError(f"{label}: expected exit {expected}, got {result.returncode}: {result.stderr}")
        return result.stdout

    def engine_run(self, label, args, expected=0):
        return self.run(label, [self.engine, *args], expected)

    def backup(self, repo, source, failpoint=None):
        args = [
            "backup", "--repo", repo, "--source", source,
            "--workers", "4", "--writer-workers", "2", "--memory-mib", "16",
        ]
        if failpoint:
            args += ["--failpoint", failpoint]
        output = self.engine_run("backup-" + (failpoint or "normal"), args, 86 if failpoint else 0)
        if not failpoint:
            match = re.search(r"^Snapshot:\s+(\S+)", output, re.MULTILINE)
            if not match:
                raise RuntimeError("backup did not report a snapshot ID")
            return match.group(1)


def remove_fixture(path):
    def ignore_missing_sidecar(function, name, error):
        if not isinstance(error[1], FileNotFoundError):
            raise error[1]

    # FAT32 may remove an AppleDouble sidecar automatically with its owning file.
    shutil.rmtree(path, onerror=ignore_missing_sidecar)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", type=Path, required=True)
    parser.add_argument("--parent", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sqlite", type=Path, required=True, help="SQLite shell matching the engine library")
    args = parser.parse_args()
    engine = args.engine.resolve(strict=True)
    parent = args.parent.resolve(strict=True)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    external = Path(tempfile.mkdtemp(prefix=".paravault-recovery-", dir=parent))
    local = Path(tempfile.mkdtemp(prefix="paravault-recovery-"))
    runner = Runner(engine, output)
    results = []
    info = {
        "external_fixture": str(external),
        "local_fixture": str(local),
        "engine_sha256": hashlib.sha256(engine.read_bytes()).hexdigest(),
        "sqlite_version": runner.run("sqlite-version", [args.sqlite, "--version"]).strip(),
    }
    (output / "environment.json").write_text(json.dumps(info, indent=2) + "\n")

    def sql(repo, query):
        # Match the FAT32 query-connection policy: allow SQLite WAL housekeeping while
        # rejecting SQL metadata changes. Application verification always runs first.
        return runner.run(
            "catalog-query",
            [args.sqlite, repo / "catalog.sqlite", "PRAGMA query_only=ON; " + query],
        ).strip()

    try:
        for storage_format in ("files-v1", "pack-v2"):
            for point in (
                "after-temp-write", "after-object-publish", "before-sqlite-batch-commit",
                "before-final-publish", "after-final-publish",
            ):
                started = time.perf_counter()
                source = local / storage_format / point / "source"
                source.mkdir(parents=True)
                repo = external / storage_format / point
                first = b"A" * 1024**2
                second = b"B" * 1024**2
                (source / "data.bin").write_bytes(first + first)
                (source / "duplicate.bin").write_bytes(first)
                runner.engine_run("init", ["init", "--repo", repo, "--storage-format", storage_format])
                original = runner.backup(repo, source)
                (source / "data.bin").write_bytes(second + second)
                (source / "duplicate.bin").write_bytes(second)
                runner.backup(repo, source, point)

                # Exercise the application's query connection before an external inspector can
                # repair or rebuild SQLite side files left by the interrupted process.
                runner.engine_run("verify-original", ["verify", "--repo", repo, "--snapshot", original, "--full"])
                count = int(sql(repo, "SELECT COUNT(*) FROM snapshots WHERE status='COMPLETE'"))
                if count != (2 if point == "after-final-publish" else 1):
                    raise RuntimeError("a crash exposed an unexpected COMPLETE snapshot")
                recovered = runner.backup(repo, source)
                runner.engine_run("verify-recovered", ["verify", "--repo", repo, "--snapshot", recovered, "--full"])
                restored = source.parent / "restored"
                runner.engine_run("restore", ["restore", "--repo", repo, "--snapshot", recovered, "--target", restored])
                for path in source.iterdir():
                    expected = hashlib.sha256(path.read_bytes()).digest()
                    actual = hashlib.sha256((restored / path.name).read_bytes()).digest()
                    if actual != expected:
                        raise RuntimeError("restored bytes differ from the source")
                if int(sql(repo, "SELECT COUNT(*) FROM snapshots WHERE status='IN_PROGRESS'")):
                    raise RuntimeError("recovery left an IN_PROGRESS snapshot")
                if storage_format == "pack-v2":
                    indexed = set(sql(repo, "SELECT DISTINCT pack_id FROM objects WHERE pack_id IS NOT NULL").splitlines())
                    present = {path.stem for path in (repo / "objects/packs").glob("pack-*.pack")}
                    if indexed != present:
                        raise RuntimeError("indexed packs differ from actual repository packs")
                if any((repo / "staging").iterdir()):
                    raise RuntimeError("recovery left staging contents")
                results.append({
                    "storage_format": storage_format, "failpoint": point, "passed": True,
                    "old_snapshot_verified": True, "recovered_snapshot_verified": True,
                    "restore_sha256_match": True, "staging_empty": True,
                    "case_wall_seconds": time.perf_counter() - started,
                })
                (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
                print(f"PASS {storage_format}: {point}; verify + restore + recovery", flush=True)
    except BaseException:
        print(f"Failed test fixtures retained at {external} and {local}", flush=True)
        raise
    else:
        remove_fixture(external)
        remove_fixture(local)
        print("External-filesystem recovery matrix: 10/10 passed; owned small fixtures removed.", flush=True)


if __name__ == "__main__":
    main()
