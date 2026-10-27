#!/usr/bin/env python3
"""Validate phased handoff through real CLI restore, reuse, crash and cancellation."""

import json
import math
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from cli_smoke import tree_signature


def command(binary, *args, expected=0):
    result = subprocess.run([str(binary), *map(str, args)], capture_output=True, text=True, timeout=20)
    assert result.returncode == expected, (args, result.returncode, result.stdout, result.stderr)
    return result


def backup(binary, repo, source, *args):
    result = command(binary, "backup", "--repo", repo, "--source", source, *args)
    run = re.search(r"^Run ID:\s+(\S+)$", result.stdout, re.MULTILINE).group(1)
    summary = json.loads((repo / "reports" / f"{run}.summary.json").read_text())
    trace = [json.loads(line) for line in (repo / "reports" / f"{run}.jsonl").read_text().splitlines()]
    assert trace and all(row["io_strategy"] == summary["io_strategy"] for row in trace)
    assert all("io_draining" in row["observations"] for row in trace)
    return summary


def verify(binary, repo, summary):
    command(binary, "verify", "--repo", repo, "--snapshot", summary["snapshot_id"], "--full")


def matrix(binary, root, source):
    signature = tree_signature(source)
    logical = sum(p.stat().st_size for p in source.rglob("*") if p.is_file() and not p.is_symlink())
    for format in ("files-v1", "pack-v2"):
        baseline_repo = root / f"{format}-streaming"
        command(binary, "init", "--repo", baseline_repo, "--storage-format", format)
        baseline = backup(binary, baseline_repo, source, "--memory-mib", 8)
        assert baseline["io_strategy"] == "streaming" and baseline["io_batches"] == 0
        assert baseline["io_batch_chunks"] == 0
        verify(binary, baseline_repo, baseline)
        for memory, workers, writers, batch, scheduler in (
            (3, 1, 1, 0, "fixed"), (8, 8, 2, 0, "fixed"), (8, 4, 1, 2, "adaptive")
        ):
            repo = root / f"{format}-phased-{memory}-{workers}"
            command(binary, "init", "--repo", repo, "--storage-format", format)
            args = ("--io-strategy", "phased", "--memory-mib", memory, "--workers", workers,
                    "--writer-workers", writers, "--io-batch-mib", batch, "--scheduler", scheduler)
            result = backup(binary, repo, source, *args)
            for key in ("logical_bytes", "read_bytes", "unique_raw_bytes", "reused_chunks"):
                assert result[key] == baseline[key], (key, result, baseline)
            assert result["read_bytes"] == logical
            assert result["memory_credit_peak_bytes"] <= memory << 20
            if memory == 3:
                assert result["io_batch_chunks"] == 1
            if batch:
                assert result["io_batch_chunks"] == batch
            # Every nonempty regular-file chunk is submitted once, including duplicates.
            chunks = sum(math.ceil(p.stat().st_size / (1 << 20)) for p in source.rglob("*")
                         if p.is_file() and not p.is_symlink())
            assert result["io_batches"] == math.ceil(chunks / result["io_batch_chunks"])
            assert result["io_drain_wait_seconds"] > 0
            verify(binary, repo, result)
            target = repo.with_name(repo.name + "-restored")
            command(binary, "restore", "--repo", repo, "--snapshot", result["snapshot_id"], "--target", target)
            assert tree_signature(target) == signature
            cached = backup(binary, repo, source, *args, "--trust-metadata")
            assert cached["read_bytes"] == 0 and cached["io_batches"] == 0
            assert cached["unique_raw_bytes"] == 0
            verify(binary, repo, cached)
        # A repository can switch between policies without a format change.
        switched = backup(binary, baseline_repo, source, "--io-strategy", "phased", "--memory-mib", 3)
        assert switched["unique_raw_bytes"] == 0 and switched["read_bytes"] == logical
        verify(binary, baseline_repo, switched)
        command(binary, "backup", "--repo", baseline_repo, "--source", source,
                "--io-strategy", "unknown", expected=2)
        command(binary, "backup", "--repo", baseline_repo, "--source", source,
                "--io-batch-mib", 2, expected=2)
        # Built-in benchmark must forward policy settings and preserve them in its output.
        report = root / f"{format}-benchmark"
        command(binary, "benchmark", "--dataset", source, "--workers", 1, "--repeat", 1,
                "--storage-format", format, "--io-strategy", "phased", "--memory-mib", 3,
                "--output", report)
        bench = json.loads((report / "benchmark.json").read_text())
        assert bench["io_strategy"] == "phased" and bench["runs"][0]["io_batch_chunks"] == 1


def crash_and_cancel(binary, root):
    for format in ("files-v1", "pack-v2"):
        for failpoint in ("after-object-publish", "before-sqlite-batch-commit"):
            repo = root / f"crash-{format}-{failpoint}"
            source = root / f"source-{format}-{failpoint}"
            source.mkdir()
            (source / "data").write_bytes(b"A" * (1 << 20))
            command(binary, "init", "--repo", repo, "--storage-format", format)
            original = backup(binary, repo, source)
            (source / "data").write_bytes(b"B" * (1 << 20))
            args = ("--io-strategy", "phased", "--memory-mib", 3)
            command(binary, "backup", "--repo", repo, "--source", source, *args,
                    "--failpoint", failpoint, expected=86)
            assert len(re.findall(r"^bkp-", command(binary, "list", "--repo", repo).stdout, re.MULTILINE)) == 1
            verify(binary, repo, original)
            recovered = backup(binary, repo, source, *args)
            verify(binary, repo, recovered)
            assert not any((repo / "staging").iterdir())

    repo, source = root / "cancel-repo", root / "cancel-source"
    source.mkdir()
    (source / "data").write_bytes(os.urandom(4 << 20))
    command(binary, "init", "--repo", repo, "--storage-format", "pack-v2")
    original = backup(binary, repo, source)
    (source / "data").write_bytes(os.urandom(4 << 20))
    child = subprocess.Popen([str(binary), "backup", "--repo", str(repo), "--source", str(source),
        "--io-strategy", "phased", "--memory-mib", "3", "--test-writer-delay-ms", "10000"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            traces = [p for p in (repo / "reports").glob("*.jsonl") if p.stem != original["run_id"]]
            rows = []
            for path in traces:
                for line in path.read_text().splitlines():
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:  # Observer may currently be appending.
                        pass
            if any(row["observations"]["io_draining"] for row in rows):
                break
            assert child.poll() is None, "backup exited before the delayed durable ACK"
            time.sleep(0.02)
        else:
            raise AssertionError("did not observe the reader waiting for a durable batch")
        child.send_signal(signal.SIGINT)
        _, error = child.communicate(timeout=5)
        assert child.returncode == 8 and "cancelled" in error, (child.returncode, error)
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate()
    verify(binary, repo, original)
    assert len(re.findall(r"^bkp-", command(binary, "list", "--repo", repo).stdout, re.MULTILINE)) == 1
    assert not any((repo / "staging").iterdir())


def main():
    binary = Path(sys.argv[1]).resolve()
    with tempfile.TemporaryDirectory(prefix="pbe-io-cli-") as directory:
        root = Path(directory)
        source = root / "source"
        (source / "資料" / "empty-directory").mkdir(parents=True)
        a, b, c = (os.urandom(1 << 20) for _ in range(3))
        (source / "a.bin").write_bytes(a + a + b)
        (source / "b.bin").write_bytes(a + b"tail" * 31)
        (source / "資料" / "c.bin").write_bytes(b + c)
        (source / "empty").touch()
        for i in range(12):
            (source / f"small-{i}").write_text(f"duplicate across files {i % 4}")
        os.symlink("a.bin", source / "link")
        matrix(binary, root, source)
        crash_and_cancel(binary, root)
    print("I/O CLI matrix, restore, reuse, crash recovery and drain cancellation passed")


if __name__ == "__main__":
    main()
