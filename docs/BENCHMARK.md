# Benchmark workflow

For pipeline scaling on datasets above 10 GiB, use `tools/generate_pipeline_fixture.py` and `tools/benchmark_pipeline.py`. The generator supports incompressible assets (`--kind random`), compressible unique log chunks (`--kind logs`), and many-file projects (`--small-files 4000 --small-file-bytes 4096`). The harness retains per-run traces, summaries, binary hashes, process CPU time, and pack-size counts, fully verifies every snapshot, and removes each temporary repository. It leaves the fixture in the caller's directory for before/after comparisons. See the pipeline campaign (local report; not included) and [PIPELINE.md](PIPELINE.md).

## Compare incompressible backups with an I/O reference

`tools/benchmark_io_floor.py` creates one 10 GiB random fixture and compares normal backups with 4/8 CPU workers, a diagnostic build that skips compression, and a reference that reuses the production Pack Writer while omitting timed hashing, compression, deduplication and SQLite commits. All cases use one storage writer, bounded buffers, the same pack target, writev batching, idle flush and publication/sync protocol. Actual pack counts can vary with task arrival times.

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --target paravault pbe_io_floor --parallel
python3 tools/benchmark_io_floor.py --output reports/io-reference-local --repeat 3
```

The C++ probe is an explicit diagnostic target, excluded from normal builds and installation. The harness builds the no-compression variant in a disposable source copy and preserves its patch; production compression behavior is unchanged. It verifies every output outside backup timing, retains per-run traces and input chunk digests, and removes its generated fixture, repositories and diagnostic build. A small probe check also verifies exact pack size and rejection of an altered expected digest.

This reference measures less work than a functional backup and is not a proven physical-device throughput ceiling. Source and destination share the local filesystem volume; caches, background load and thermal state remain uncontrolled. Use the result to investigate remaining pipeline costs, rather than treating its ratio as a guaranteed attainable speedup.

The 2026-11-02 campaign (local report; not included) retains all 12 runs, the compression-ablation patch, input chunk digests, stage traces, and the audit. Its normal 4-worker median was 11.294 seconds, compared with 11.124 seconds when skipping compression and 11.666 seconds for the I/O reference.

## Test macOS allocation and source-cache policies

The I/O strategy campaign (local report; not included) compares the unchanged application with two disposable diagnostic builds: pack preallocation using `F_PREALLOCATE`, and uncached source reads using `F_NOCACHE`. Both retain normal hashing, compression, deduplication, SQLite, sync/publication and full verification. Successful-application counters are audited for every pack/source file; the small-fixture check also compares stored pack lengths and object counts. Preallocation time stays inside backup timing.

```sh
cmake --build build-release --target paravault --parallel
python3 tools/benchmark_io_strategies.py --output reports/io-strategy-local --repeat 3
```

This tool requires macOS. It does not change production sources or defaults. It rotates case order, syncs the generated source files before the trials, records all runs, and removes its fixture, diagnostic builds and repositories afterward. The retained 10 GiB campaign found a baseline median of 11.165 seconds, 11.067 seconds with preallocation (less than 1% and inconsistent across repeats), and 13.243 seconds with source caching disabled. These results apply to this filesystem/workload; `F_NOCACHE` was tested with the existing buffers and read sizes, not a separate alignment/read-ahead redesign.

## Profile write waits with an independent I/O control

On macOS, `tools/profile_io_path.py` compares complete backups with a disposable build that measures each `writev` call's wall time and own-thread CPU time, plus independent POSIX copy, read-only and memory-fed write controls. The optional native target has no engine, Pack Writer, SQLite or Zstd dependency. All output controls retain file sync and publication; validation runs outside timing.

```sh
cmake --build build-release --target paravault pbe_native_io --parallel
python3 tools/profile_io_path.py --output reports/io-path-local --repeat 3
```

The harness creates and removes a 10 GiB random fixture, rotates five cases across three repeats, preserves all raw syscall timing samples, records own-process I/O counters and samples system-wide `vm_stat`. Native controls use 128 MiB reusable buffers. The memory-fed write case repeats a 128 MiB high-entropy pool, so it isolates timed source reads without modeling a complete unique-data backup. Wall time minus thread CPU includes both waiting and descheduling; it does not measure SSD utilization.

The independent I/O campaign (local report; not included) measured 11.029 seconds for a complete backup and 11.092 seconds for independent copy, compared with 3.188 seconds for read-only and 5.109 seconds for memory-fed writing. This supports investigating mixed read/write and OS storage behavior before adding CPU workers. It does not establish a physical-device ceiling or guarantee the memory-fed result is attainable for backup. Per-device utilization and kernel stacks were unavailable under the session's permissions.

The standalone native control now accepts an optional publication mode:

```sh
build-release/pbe_native_io copy SOURCE NEW_OUTPUT_DIRECTORY [auto|exclusive-rename]
```

The default `auto` retains hard-link publication where supported and falls back to macOS `RENAME_EXCL` only when hard links are unsupported. `exclusive-rename` exercises that path directly for validation. Neither method replaces an existing destination. Output must be a new or empty directory; a zero-byte regular file is rejected. Additional JSON fields separate publication and directory-sync timings and count unsupported-link fallbacks. A failed unsupported-link attempt in the raw publication samples is expected when a subsequent exclusive rename succeeds; it is not a failed data transfer.

The copy control uses one writer and 128 MiB output segments. A 100 MiB source therefore produces one synced file, with byte-for-byte verification outside transfer timing. It omits backup hashing, compression, SQLite and snapshot metadata, so it is a lower-work reference. Match writer and segment counts when comparing with a functional backup, and preserve file/directory synchronization. Internal validation and the post-reconnect experiment are recorded in the external I/O control report (local report; not included). Both 100 MiB controls passed; file synchronization dominated their measured time. The complete backup was faster in that single ordered comparison, so the native time must not be interpreted as a stable device lower bound. The earlier I/O failure cause remains unknown.

## Compare streaming and phased source I/O

Both policies are available in the production CLI and built-in `benchmark` command through `--io-strategy streaming|phased`. `--memory-mib` caps raw/compression credits for either policy. Optional `--io-batch-mib` caps raw batches in phased mode; 0 uses credit-derived capacity. Actual capacity is retained in summaries and reports. See [PIPELINE.md](PIPELINE.md#reader-的兩種-io-排程) for handoff and cancellation semantics.

```sh
cmake --build build-release --target paravault --parallel
python3 tools/benchmark_io_scheduling.py \
  --output reports/io-scheduling-local --memory-mib 64,256 --repeat 3
```

The harness generates a disposable 10 GiB random fixture and runs the two policies at each credit budget, with fixed four CPU workers, one storage writer and pack-v2. Every snapshot passes full verification outside timing. It retains all runs, summaries, traces, pack counts, CPU time, source/binary hashes and cleanup records. Input data is synced before trials; caches and background load remain uncontrolled. It rotates case order and leaves the default strategy unchanged.

Use `--dataset PATH` to preserve and reuse an existing tree, and `--repo-parent PATH` to choose the existing parent directory for disposable repositories. Separate filesystem device IDs do not by themselves prove separate physical devices. Output paths must be outside the source tree. No source dataset supplied by the caller is removed.

Phasing can reduce source/object overlap but also loses CPU/I/O overlap and may force smaller packs or more catalog transactions. Compare total elapsed time alongside actual batch capacity, pack counts and sync time; do not infer improvement merely from shorter write calls.

The 10 GiB scheduling campaign (local report; not included) retained 12 verified trials. Streaming/phased medians were 10.906/11.610 seconds at 64 MiB and 11.122/10.671 seconds at 256 MiB. Small batches reduced time inside staging writes but increased pack syncs and lost read/compute overlap. The modest 256 MiB difference is not treated as a general speedup; streaming remains the default.

## Existing fixture campaigns

Generate the seeded A-F fixtures:

```sh
python3 tools/generate_dataset.py --output fixtures --profiles A,B,C,D,E,F --size-mib 32
```

Run five fresh-repository trials for fixed and adaptive CPU admission limits:

```sh
python3 tools/run_benchmark.py --pbe build/pbe --fixtures fixtures --output reports/benchmark --repeat 5
```

Each profile produces JSON, CSV, and Markdown reports. The JSON and CSV preserve every run. The Markdown reports median, standard deviation, min/max spread, credit peak, worker-limit churn, controller time, and fixed/adaptive median differences by worker cap. The report states that filesystem cache state and machine load were not controlled. It does not claim that adaptive scheduling is always faster.

To compare object-writer concurrency, set `--writer-workers` while keeping the fixture and CPU worker count fixed. For example, run the same profile with `--writer-workers 1`, then `2`, then `4`; every trial uses fresh repositories. The default remains one writer until measurements on the target storage justify a different choice.

The F profile contains `base/` and `version-2/`; each trial backs up both trees to the same fresh repository and reports the second snapshot. The second run still reads the source in safe mode, while content addressing shows how much existing data it reuses. To exercise diagnostics with controlled pressure, pass `--test-writer-delay-ms N`, `--test-reader-delay-ms N`, or `--test-cpu-spin-ms N` to `pbe benchmark`. These inject writer/input delays or CPU work and do not model a real device.

To plot one report, install matplotlib and run `python3 tools/plot_results.py reports/benchmark/A/benchmark.csv`. The current benchmark is a local measurement harness; it does not control external disk caches, thermal state, background jobs, or OS-level I/O throttling.
