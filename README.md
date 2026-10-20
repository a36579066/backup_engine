# Parallel Backup & Deduplication Engine

PBE is a local snapshot backup engine written in C++20 for macOS and Linux. It stores immutable BLAKE3-addressed 1 MiB chunks, compresses new objects with Zstandard, deduplicates shared content, and publishes snapshots through a SQLite catalog. The engine includes bounded producer/consumer queues, a pipeline memory budget, source-change checks, safe restore paths, full-object verification, fault-injection checkpoints, JSON Lines diagnostics, and an opt-in adaptive CPU admission controller.

The implementation is a portfolio baseline: it must not be treated as the sole copy of important data before you validate it on your own storage, restore scenarios, and failure environment. It does not replace Time Machine.

## Build

Requirements: C++20 compiler, CMake 3.20+, SQLite 3, Zstandard, and POSIX file APIs. Python 3 is used by tests and benchmark tooling.

macOS:

```sh
brew install cmake ninja sqlite zstd
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

Ubuntu / Debian:

```sh
sudo apt-get install cmake ninja-build libsqlite3-dev libzstd-dev
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build
ctest --test-dir build --output-on-failure
```

The project contains a compact unkeyed BLAKE3-256 implementation based on the public BLAKE3 specification, so the build does not fetch dependencies over the network. SQLite and Zstandard are linked from the system packages.

## Source layout

```text
include/pbe/       Public engine API, BLAKE3 API, and bounded queue
src/
├── cli/           Command-line entry point and arguments
├── core/          Backup coordination and engine operations
├── storage/       SQLite catalog, repository, objects, and packs
├── concurrency/   Memory credits, deduplication claims, and worker pool
├── scheduler/     Worker admission and adaptive scheduling
├── diagnostics/   Bottleneck classification and trace serialization
├── hashing/       BLAKE3 implementation
├── pipeline/      Cross-file metadata batches and durable-result commits
├── restore/       Snapshot restoration
└── benchmark/     Benchmark orchestration and reports
```

Start with `src/cli/main.cpp`, then follow the commands into `src/core/engine.cpp`. The full file map and module responsibilities are in [ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Quick start

```sh
build/pbe init --repo ~/PBERepo
build/pbe backup --repo ~/PBERepo --source ~/Projects --workers 4
build/pbe list --repo ~/PBERepo
build/pbe verify --repo ~/PBERepo --snapshot <snapshot-id> --full
build/pbe restore --repo ~/PBERepo --snapshot <snapshot-id> --target ~/RestoreTest
build/pbe stats --repo ~/PBERepo --json
```

Adaptive scheduling and explicit trusted-metadata reuse are opt-in:

```sh
build/pbe backup --repo ~/PBERepo --source ~/Projects --scheduler adaptive --max-workers 8 --memory-mib 256
build/pbe backup --repo ~/PBERepo --source ~/Projects --trust-metadata
build/pbe explain --repo ~/PBERepo --run <run-id>
```

The default `files-v1` repository stores each unique chunk as a separate file. To experiment with batched immutable pack storage, select it when creating a new repository:

```sh
build/pbe init --repo ~/PBERepo-Packs --storage-format pack-v2
build/pbe backup --repo ~/PBERepo-Packs --source ~/Projects --workers 4 --writer-workers 1 --memory-mib 256
```

`pack-v2` streams compressed chunks into temporary packs, gathering up to eight already-available objects into a `writev()` call without an extra payload buffer. Chunk buffers and credits are released after staging, while durable acknowledgements wait for pack fsync, publication, and directory sync. The writer targets 128 MiB or 256 objects and flushes after 2 ms idle; the target is not a guaranteed size. `files-v1` remains the default. Pack-v2 requires a new repository and cannot be enabled by editing an existing repository's configuration; older PBE binaries do not understand it.

Backup runs the reader, CPU workers, storage writers, and a dedicated metadata consumer concurrently. The reader continues across file boundaries; CPU workers submit asynchronous writes or duplicate continuations and continue with new chunks. Metadata transactions span files and begin only after durable results arrive. See the [pipeline design](docs/PIPELINE.md), current pipeline scaling measurements (local report; not included), and earlier storage-format experiment (local report; not included).

On the local machine with eight logical CPUs, a generated 10.016 GiB project with 4,021 files improved from a backup median of 34.588 seconds to 11.106 seconds at the same four CPU workers and two storage writers (3.11×). A generated 10 GiB compressible log fixture scaled from 12.601 seconds with one CPU worker to 3.525 seconds with eight (3.57×), using one storage writer. The random-asset fixture scaled only about 1.20× because its writes dominated. Every trial used a fresh repository and passed full verification; the linked report preserves all runs and their limits.

`--trust-metadata` reuses a previous file's chunk map when its relative path, size, mode, and modification time match. Reused objects are fully checked; a bounded 4,096-ID recent cache avoids repeating checks for recently seen objects, and evicted IDs may be checked again on very high-cardinality backups. The option trusts source metadata and can miss a same-size source change with a restored timestamp. Use the default full-read behavior or `--paranoid` when that tradeoff is unacceptable. Restore requires a new or empty target and does not overwrite paths. Symlinks are stored as links and never followed during source scan or restore. Basic mode and mtime are saved; ACLs, xattrs, ownership, resource forks, Finder metadata, and hardlink relationships are outside this version's fidelity promise.

## Commands

- `init --repo PATH [--storage-format files-v1|pack-v2]`: initialize an empty repository; existing non-empty directories are refused. `files-v1` is the default.
- `backup --repo PATH --source PATH`: create and publish a snapshot. Configure CPU `--workers`, object-persistence `--writer-workers` (1 by default; maximum 32), `--memory-mib`, `--zstd-level`, `--scheduler fixed|adaptive`, `--max-workers`, `--skip-unsupported`, `--paranoid`, or `--trust-metadata`.
- `list`, `show`, `restore`, and `verify`: inspect, reconstruct, or fully check complete snapshots.
- `stats --json`: report logical bytes, object files, referenced objects, and physical object bytes.
- `explain --repo PATH --run RUN_ID [--json]`: summarize a decision trace and follow-up experiments as text or JSON.
- `benchmark --dataset PATH`: compare profiles using fresh temporary repositories; accepts `--storage-format files-v1|pack-v2` and writes JSON, CSV, and Markdown.
- `python3 tools/benchmark_storage_formats.py`: generate a 10 GiB random fixture, compare both formats with full verification, and save JSONL traces plus Markdown/JSON results. Temporary fixture and repositories are removed automatically.

For controlled diagnostic runs, `benchmark` also accepts `--test-writer-delay-ms N`, `--test-reader-delay-ms N`, and `--test-cpu-spin-ms N`. They inject writer/input delays or CPU work and do not simulate a particular disk or processor.

Reproduce the four controlled CPU, writer, memory, and input scenarios and preserve their JSONL traces with `python3 tools/run_diagnostic_scenarios.py --pbe build-release/pbe --output reports/diagnostics`; see [the diagnostics guide](docs/DIAGNOSTICS_SCHEMA.md).

## Local monitoring dashboard

Start the local UI after building PBE:

```sh
python3 tools/pbe_dashboard.py --pbe build-release/pbe
```

Open `http://127.0.0.1:8765` to initialize or select a repository, choose its storage format when creating it, start a backup, choose the number of object writers, watch live throughput and pipeline diagnostics, and review recent snapshots. The dashboard binds to localhost and uses only Python's standard library. See the [dashboard guide](docs/DASHBOARD.md).

The `--failpoint NAME` option is for disposable test repositories only. It abruptly exits with status 86 at a named checkpoint and models process termination, not power loss. Supported names are listed in [docs/FAILURE_MODEL.md](docs/FAILURE_MODEL.md).

## Benchmark fixtures

Create deterministic A-F dataset profiles and run the fixed/adaptive comparison suite:

```sh
python3 tools/generate_dataset.py --output fixtures --profiles A,B,C,D,E,F --size-mib 32
python3 tools/run_benchmark.py --pbe build/pbe --fixtures fixtures --output reports/benchmark --repeat 5
```

Read [docs/BENCHMARK.md](docs/BENCHMARK.md) before comparing results. PBE reports local observations; it does not claim a guaranteed speedup or a physical disk bottleneck.

## Current implementation and validation

The repository implements the P0 backup, deduplication, compression, snapshot, restore, and verify paths, plus opt-in trusted-metadata reuse, symlink metadata, metrics traces, explain output, fault injection, a conservative adaptive CPU gate, and fresh-repository benchmark tooling. The code responsibility map is in [ARCHITECTURE.md](docs/ARCHITECTURE.md). The CMake test suite covers chunk/hash/codec boundaries, Unicode paths, queue and process cancellation, worker counts 1/2/4/8, metadata preservation, corrupted objects, CLI flows, recovery after crashes, and the five process-crash checkpoints. See the detailed [test matrix](docs/TESTING.md) and validation record (local report; not included). A six-profile, five-repeat campaign was run on the local macOS arm64 machine; see the benchmark report (local report; not included). The four injected diagnostic scenarios and traces are in reports/diagnostics (local report; not included). These local runs validate the tooling and correctness path, not target-device performance. Linux CI, sanitizer support beyond UBSan, and platform-specific power-loss validation still need confirmation in their target environments.

Detailed design notes are in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md), [docs/FORMAT.md](docs/FORMAT.md), [docs/FAILURE_MODEL.md](docs/FAILURE_MODEL.md), [docs/SCHEDULER_ADR.md](docs/SCHEDULER_ADR.md), and [docs/DIAGNOSTICS_SCHEMA.md](docs/DIAGNOSTICS_SCHEMA.md).
