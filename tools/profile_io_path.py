#!/usr/bin/env python3
"""Profile own writev wall/CPU time and compare independent POSIX I/O controls on macOS."""

import argparse
import difflib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

from benchmark_io_floor import ROOT, copy_observations, run_command, sha256
from benchmark_io_strategies import source_hashes
from benchmark_storage_formats import make_fixture, parse_output

CASES = ("baseline", "profiled", "native-copy", "read-only", "memory-write")


def build_profiled(work, output):
    source = work / "profile-source"
    for directory in ("src", "include", "tools"):
        shutil.copytree(ROOT / directory, source / directory)
    shutil.copyfile(ROOT / "CMakeLists.txt", source / "CMakeLists.txt")
    shutil.copyfile(ROOT / "tools/io_call_metrics.hpp", source / "src/storage/io_call_metrics.hpp")
    path = source / "src/storage/object_store.cpp"
    original = path.read_text()
    text = original.replace('#include "storage/object_store.hpp"',
        '#include "storage/object_store.hpp"\n#include "storage/io_call_metrics.hpp"', 1)
    counter = '''
// Diagnostic only: preserve syscall/errno and report after all workers join.
struct WriterCallProfile {
    pbe::profiling::IoCalls calls;
    pbe::profiling::ProcessIo before = pbe::profiling::ProcessIo::read();
    ~WriterCallProfile() {
        if (calls.empty()) return;
        const auto after = pbe::profiling::ProcessIo::read();
        std::cerr << std::setprecision(12) << "PBE_WRITE_PROFILE {\\"writev\\":";
        calls.json(std::cerr);
        std::cerr << ",\\"process_io\\":";
        pbe::profiling::process_io_json(std::cerr, before, after);
        std::cerr << "}\\n";
    }
};
WriterCallProfile writer_call_profile;
'''
    anchor = "constexpr std::size_t kPackHeaderSize = 16;"
    if text.count(anchor) != 1:
        raise RuntimeError("writer profile anchor changed")
    text = text.replace(anchor, anchor + "\n" + counter, 1)
    pattern = r"const ssize_t n\s*=\s*::writev\(fd,\s*vectors \+ first,\s*static_cast<int>\(count - first\)\);"
    replacement = '''const ssize_t n = pbe::profiling::measure(writer_call_profile.calls, [&] {
            return ::writev(fd, vectors + first, static_cast<int>(count - first));
        });'''
    text, count = re.subn(pattern, lambda _: replacement, text)
    if count != 1:
        raise RuntimeError("writev instrumentation anchor changed")
    path.write_text(text)
    patch = difflib.unified_diff(original.splitlines(True), text.splitlines(True),
                                fromfile="production/object_store.cpp", tofile="profiled/object_store.cpp")
    (output / "writev-profile.patch").write_text("".join(patch))
    build = work / "profile-build"
    print("Building disposable per-writev CPU/wall-time instrumentation", flush=True)
    configured = run_command(["cmake", "-S", source, "-B", build, "-DCMAKE_BUILD_TYPE=Release", "-DPBE_BUILD_TESTS=OFF"])
    compiled = run_command(["cmake", "--build", build, "--target", "paravault", "--parallel", "4"])
    (output / "profile-build.log").write_text(configured.stdout + configured.stderr + compiled.stdout + compiled.stderr)
    return build / "paravault"


def vm_snapshot():
    before = time.time()
    result = subprocess.run(["/usr/bin/vm_stat"], capture_output=True, text=True)
    snapshot = {"started_unix": before, "finished_unix": time.time(), "returncode": result.returncode}
    if result.returncode:
        snapshot["stderr"] = result.stderr
        return snapshot
    size = re.search(r"page size of (\d+) bytes", result.stdout)
    snapshot["page_size"] = int(size.group(1)) if size else None
    snapshot["values"] = {name.strip('"'): int(value)
        for name, value in re.findall(r'^([^:\n]+):\s+(\d+)\.', result.stdout, re.MULTILINE)}
    return snapshot


class VmMonitor:
    def __init__(self):
        self.stop = threading.Event()
        self.rows = []
        self.thread = threading.Thread(target=self.collect)

    def collect(self):
        while not self.stop.is_set():
            self.rows.append(vm_snapshot())
            self.stop.wait(1)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()


def run_logged(args, output, name):
    """wait4 captures only this child, excluding the background vm_stat children."""
    out_path, err_path = output / f"{name}.stdout", output / f"{name}.stderr"
    started = time.time()
    with out_path.open("w") as out, err_path.open("w") as err:
        child = subprocess.Popen(list(map(str, args)), stdout=out, stderr=err)
        try:
            _, status, usage = os.wait4(child.pid, 0)
            child.returncode = os.waitstatus_to_exitcode(status)
        except BaseException:
            # Stop the child before TemporaryDirectory removes its inputs/output.
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
            raise
    if child.returncode:
        raise RuntimeError(f"{name} failed: {out_path.read_text()}\n{err_path.read_text()}")
    return {"stdout": out_path.read_text(), "stderr": err_path.read_text(),
        "started_unix": started, "finished_unix": time.time(),
        "user_cpu_seconds": usage.ru_utime, "system_cpu_seconds": usage.ru_stime}


def brief_calls(value):
    return {key: item for key, item in value.items() if key != "samples_ns"}


def engine_case(case, repeat, binary, source, repo, output, logical):
    name = f"r{repeat}-{case}"
    run_command([binary, "init", "--repo", repo, "--storage-format", "pack-v2"])
    print(f"{name}: backup starting", flush=True)
    backup = run_logged([binary, "backup", "--repo", repo, "--source", source,
        "--workers", "4", "--writer-workers", "1", "--memory-mib", "256"], output, name)
    snapshot = parse_output(r"^Snapshot:\s+(\S+)$", backup["stdout"], "snapshot")
    run_id = parse_output(r"^Run ID:\s+(\S+)$", backup["stdout"], "run id")
    summary_path = repo / "reports" / f"{run_id}.summary.json"
    summary = json.loads(summary_path.read_text())
    shutil.copyfile(summary_path, output / f"{name}.summary.json")
    stages = copy_observations(repo, run_id, output, name)
    begin = time.perf_counter()
    verified = run_command([binary, "verify", "--repo", repo, "--snapshot", snapshot, "--full"])
    verify_seconds = time.perf_counter() - begin
    if "Verified: COMPLETE" not in verified.stdout or summary["logical_bytes"] != logical:
        raise RuntimeError("profiled engine verification failed")
    packs = list((repo / "objects/packs").glob("*.pack"))
    record = {key: backup[key] for key in ("started_unix", "finished_unix", "user_cpu_seconds", "system_cpu_seconds")}
    record.update({"case": case, "repeat": repeat, "elapsed_seconds": summary["elapsed_seconds"],
        "verify_seconds": verify_seconds, "verified": True, "logical_bytes": logical,
        "pack_files": len(packs), "pack_bytes": sum(p.stat().st_size for p in packs), "stages": stages})
    if case == "profiled":
        lines = [line[len("PBE_WRITE_PROFILE "):] for line in backup["stderr"].splitlines()
                 if line.startswith("PBE_WRITE_PROFILE ")]
        if len(lines) != 1:
            raise RuntimeError("missing own-process writer profile")
        full = json.loads(lines[0])
        (output / f"{name}.profile.json").write_text(json.dumps(full, indent=2) + "\n")
        if full["writev"]["bytes"] != record["pack_bytes"] - 16 * len(packs):
            raise RuntimeError("writev byte accounting mismatch")
        record["writev"] = brief_calls(full["writev"])
        record["process_io"] = full["process_io"]
    print(f"{name}: {record['elapsed_seconds']:.3f}s; verified", flush=True)
    return record


def native_case(case, repeat, native, source, destination, output, logical):
    name = f"r{repeat}-{case}"
    mode = {"native-copy": "copy", "read-only": "read", "memory-write": "memory-write"}[case]
    print(f"{name}: native control starting", flush=True)
    result = run_logged([native, mode, source, destination], output, name)
    full = json.loads(result["stdout"])
    if not full["verified"] or full["logical_bytes"] != logical:
        raise RuntimeError("native control validation failed")
    record = {key: value for key, value in full.items() if key not in ("read", "write", "fsync")}
    record.update({"case": case, "repeat": repeat})
    for key in ("read", "write", "fsync"):
        record[key] = brief_calls(full[key])
    print(f"{name}: {record['elapsed_seconds']:.3f}s; validated", flush=True)
    return record


def report(output, manifest, rows):
    lines = ["# Independent I/O path profiling", "",
        f"{manifest['logical_bytes']:,} input bytes, {manifest['files']} files; {manifest['repeat']} repeats per case.", "",
        "| Case | Runs (s) | Median (s) | CPU median (s) | Write-call wall (s) | Write-call CPU (s) |",
        "|---|---|---:|---:|---:|---:|"]
    for case in CASES:
        items = [r for r in rows if r["case"] == case]
        def call(r): return r.get("writev", r.get("write", {}))
        walls = [call(r)["wall_seconds"] for r in items if call(r)]
        cpus = [call(r)["thread_cpu_seconds"] for r in items if call(r)]
        wall_text = f"{median(walls):.3f}" if walls else "unmeasured"
        cpu_text = f"{median(cpus):.3f}" if cpus else "unmeasured"
        times = " / ".join(f"{r['elapsed_seconds']:.3f}" for r in items)
        lines.append(f"| {case} | {times} | {median(r['elapsed_seconds'] for r in items):.3f} | {median(r['user_cpu_seconds']+r['system_cpu_seconds'] for r in items):.3f} | {wall_text} | {cpu_text} |")
    lines.extend(["", "## Method and limitations", "",
        "- Baseline and profiled cases are complete current backups: 4 CPU workers, 1 writer, 256 MiB credits, pack-v2. The profiling-only copy measures each writev's elapsed and own-thread CPU time without changing the syscall or errno handling. Production source/binary is unchanged.",
        "- Native copy is an independent POSIX reader/writer using 16 reusable 8 MiB buffers, normal write(), 128 MiB payload segments, file fsync, no-replace link publication and directory sync. It has no engine/PackWriter/SQLite/Zstd dependency. Payloads are compared byte-for-byte with the source outside timing. It is a lower-work control, not a functional backup.",
        "- Read-only checks total bytes and source stat stability. Memory-write repeats a 128 MiB in-memory high-entropy source pool prepared before timing; it writes the full logical byte count and validates the resulting pattern. It isolates timed source reads, but its repeated payload is not a unique-data backup or a proof of physical NAND bandwidth.",
        "- Native buffer preparation is outside control timing. Native segments have no 64-byte object headers and segment counts may differ slightly from engine packs; all output controls keep file/directory sync and publication rather than using clones or sparse files.",
        "- Thread CPU is measured with CLOCK_THREAD_CPUTIME_ID. Wall minus thread CPU includes waiting and descheduling; it is not a kernel stack attribution or proof that the SSD was busy for that time.",
        "- proc_pid_rusage reports this process's OS disk-I/O accounting, not whole-device utilization. Engine process counters cover command startup through exit; native counters cover only timed control work. Counters are captured before verification.",
        "- vm_stat is sampled once per second. Its counters are system-wide, so background activity is not attributed to this program. Native timestamps identify the timed phase independently of later verification.",
        "- iostat, fs_usage, sample and diskutil were unavailable under current permissions; xctrace was unavailable in the developer tools. No elevated permissions were used. Device queue depth, utilization and kernel stack attribution remain unavailable.",
        "- All runs are retained, case order rotates, and caches/thermals/background load are uncontrolled. Owned data/builds/repositories are removed after validation.", ""])
    (output / "README.md").write_text("\n".join(lines))


def smoke_check(work, output, profiled, native):
    smoke = output / "smoke"
    smoke.mkdir()
    fixture = work / "smoke-fixture"
    fixture.mkdir()
    source, logical, _ = make_fixture(fixture, 1, 128, 2)
    rows = []
    rows.append(engine_case("profiled", 0, profiled, source, fixture / "repo", smoke, logical))
    shutil.rmtree(fixture / "repo")
    for case in ("native-copy", "read-only", "memory-write"):
        destination = fixture / case
        row = native_case(case, 0, native, source, destination, smoke, logical)
        if case == "read-only":
            assert row["read"]["bytes"] == logical and row["write"]["count"] == 0
        else:
            assert row["write"]["bytes"] == logical and row["segments"] == 2
            assert sum(p.stat().st_size for p in (destination / "objects").glob("*.bin")) == logical
        assert row["process_io"]["available"]
        rows.append(row)
        shutil.rmtree(destination)
    (smoke / "validation.json").write_text(json.dumps(rows, indent=2) + "\n")
    shutil.rmtree(fixture)
    print("Smoke checks passed: own-call metrics, byte counts and full output comparison", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repeat", type=int, default=3)
    args = parser.parse_args()
    if sys.platform != "darwin" or args.repeat < 1:
        parser.error("requires macOS and positive repeat")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()): parser.error("output must be new/empty")
    output.mkdir(parents=True, exist_ok=True)
    production, native = ROOT / "build-release/paravault", ROOT / "build-release/pbe_native_io"
    original = source_hashes()
    manifest = {"started_utc": datetime.now(timezone.utc).isoformat(), "system": platform.platform(),
        "repeat": args.repeat, "orders": [], "production_binary_sha256": sha256(production),
        "native_binary_sha256": sha256(native), "source_sha256": original}
    rows = []
    monitor = None
    try:
        with tempfile.TemporaryDirectory(prefix="paravault-io-profile-", dir="/private/tmp") as directory:
            work = Path(directory)
            manifest["owned_work_root"] = str(work)
            if shutil.disk_usage(work).free < 35 * 1024**3: raise RuntimeError("insufficient disk space")
            profiled = build_profiled(work, output)
            manifest["profiled_binary_sha256"] = sha256(profiled)
            smoke_check(work, output, profiled, native)
            source, logical, count = make_fixture(work, 20, 512, 400)
            for path in source.rglob("*"):
                if path.is_file():
                    with path.open("rb+") as file: os.fsync(file.fileno())
            manifest.update({"logical_bytes": logical, "files": count})
            monitor = VmMonitor()
            with monitor:
                print("Collecting a 5-second idle vm_stat reference", flush=True)
                manifest["idle_started_unix"] = time.time()
                time.sleep(5)
                manifest["idle_finished_unix"] = time.time()
                for repeat in range(1, args.repeat+1):
                    offset = (repeat-1)*2 % len(CASES)
                    order = CASES[offset:] + CASES[:offset]
                    manifest["orders"].append(order)
                    for case in order:
                        with tempfile.TemporaryDirectory(prefix="run-", dir=work) as destination:
                            if case in ("baseline", "profiled"):
                                binary = production if case == "baseline" else profiled
                                row = engine_case(case, repeat, binary, source, Path(destination)/"repo", output, logical)
                            else:
                                row = native_case(case, repeat, native, source, Path(destination)/"data", output, logical)
                            rows.append(row)
                            with (output/"results.jsonl").open("a") as out: out.write(json.dumps(row)+"\n")
            (output/"vm-stat.jsonl").write_text("".join(json.dumps(r)+"\n" for r in monitor.rows))
            report(output, manifest, rows)
            manifest["all_runs_validated"] = all(r["verified"] for r in rows)
        manifest["owned_work_root_removed"] = not work.exists()
    finally:
        if monitor is not None:
            (output/"vm-stat.jsonl").write_text("".join(json.dumps(r)+"\n" for r in monitor.rows))
        manifest["completed_runs"] = len(rows)
        manifest["owned_work_root_removed"] = not Path(manifest.get("owned_work_root", "/")).exists()
        manifest["production_sources_preserved"] = source_hashes() == original
        manifest["production_binary_preserved"] = sha256(production) == manifest["production_binary_sha256"]
        manifest["finished_utc"] = datetime.now(timezone.utc).isoformat()
        (output/"manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
    print(f"Completed {len(rows)} validated runs; report: {output/'README.md'}", flush=True)


if __name__ == "__main__": main()
