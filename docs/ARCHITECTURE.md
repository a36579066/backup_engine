# Architecture

PBE scans the source tree and then runs a reader, a fixed set of CPU workers, an object-writer pool, and a dedicated metadata consumer concurrently. The reader continues across file boundaries without waiting for chunk acknowledgements. CPU workers calculate BLAKE3, claim a digest, and compress new content. They submit asynchronous object writes or subscribe to an existing claim and continue with the next chunk. Storage workers publish immutable loose objects or streaming packs. The metadata consumer resolves their durable acknowledgements before committing entries and references in batches spanning multiple files.

```text
Reader -> bounded chunk queue -> CPU workers -> bounded object queue -> writers
   |                             hash/claim/zstd                       |
   | entries + chunk futures                          fsync + publish + ACK
   v                                                                 |
bounded metadata queue -> catalog consumer waits for ACKs <-----------+
                                  |
                         batch SQLite transaction
                                  |
                 coordinator joins stages and publishes COMPLETE
```

All three queues are bounded. A credit is reserved before a raw chunk is allocated. Files-v1 holds it through object publication; pack-v2 releases it after a gathered staging write has finished and its buffer has been freed. Duplicate tasks release raw buffers and credits before subscribing to the owner's result. Each credit covers a full 1 MiB raw chunk and `ZSTD_compressBound(1 MiB)`. Pack writers gather up to eight already-available chunks into `writev()` without another payload buffer. The budget bounds pipeline buffers, not whole-process RSS. Metadata has a separate bound: four queued batches, one consumer batch, and one reader batch, each with at most 128 entries and the configured chunk limit.

SQLite is the metadata authority. `IN_PROGRESS` rows are never listed or restored. Blob objects are durable before a reference is committed. The final `COMPLETE` update is a separate full-synchronous SQLite transaction. A crash can leave orphan objects or an incomplete snapshot, but the previous complete snapshots remain readable.

The adaptive controller samples every two seconds. It holds during two warmup windows, waits two cooldown windows after a decision, and changes the CPU admission limit by one. It does not create or destroy threads, change chunking, change object publishing, or modify SQLite commits. It can roll back a limit after two comparable windows show a large throughput regression; this is a heuristic observation, not a causal guarantee.

PBE uses `std::thread` with an explicit close, cancel, and join protocol because the Apple Clang/libc++ supplied in the development environment does not expose `std::jthread`. The queues and credits use mutexes, condition variables, timed cancellation checks, and RAII ownership.

## Implementation boundaries

Source files are grouped by responsibility. Public interfaces live under `include/pbe`; implementation headers live beside their module's source files.

```text
include/pbe/
├── blake3.hpp
├── bounded_queue.hpp
└── engine.hpp
src/
├── benchmark/
│   └── benchmark_runner.cpp
├── cli/
│   └── main.cpp
├── concurrency/
│   ├── claim_registry.hpp
│   ├── memory_budget.hpp
│   └── worker_pool.hpp
├── core/
│   └── engine.cpp
├── diagnostics/
│   ├── diagnostics.cpp
│   └── diagnostics.hpp
├── hashing/
│   └── blake3.cpp
├── pipeline/
│   ├── metadata_stage.cpp
│   └── metadata_stage.hpp
├── restore/
│   └── restore_engine.cpp
├── scheduler/
│   ├── active_worker_gate.hpp
│   ├── adaptive_controller.cpp
│   └── adaptive_controller.hpp
└── storage/
    ├── catalog.cpp
    ├── catalog.hpp
    ├── object_store.cpp
    ├── object_store.hpp
    ├── repository.cpp
    ├── repository.hpp
    ├── snapshot_catalog.cpp
    └── snapshot_catalog.hpp
```

| Module / file | Responsibility |
|---|---|
| `include/pbe/engine.hpp` | Public command options, summaries, and engine entry points |
| `src/cli/main.cpp` | CLI argument validation and command dispatch |
| `src/core/engine.cpp` | Backup coordination, source traversal, snapshot publishing, verification, repository statistics, explain output, and object-writer pool lifecycle |
| `src/storage/catalog.hpp` / `.cpp` | SQLite statements, transactions, repository connection, and schema creation |
| `src/storage/snapshot_catalog.hpp` / `.cpp` | Snapshot entries/chunks, path validation, and chunk-map queries |
| `src/storage/repository.hpp` / `.cpp` | Repository configuration validation, canonical paths, containment checks, and writer lock |
| `src/storage/object_store.hpp` / `.cpp` | Object and pack formats, immutable reads/writes, digest verification, bounded recent verification cache, and directory sync |
| `src/restore/restore_engine.cpp` | Safe path traversal and snapshot restoration |
| `src/concurrency/memory_budget.hpp` | RAII credits for chunk buffers and bounded memory usage |
| `src/concurrency/claim_registry.hpp` | In-flight deduplication ownership, completion, failure, and release after catalog commit |
| `src/pipeline/metadata_stage.hpp` / `.cpp` | Cross-file metadata batches, cancellable ACK waits, prepared statement reuse, and atomic commits |
| `src/concurrency/worker_pool.hpp` | Fixed worker creation and explicit joining |
| `src/scheduler/active_worker_gate.hpp` | CPU worker admission limit, waiting, and cancellation |
| `src/scheduler/adaptive_controller.hpp` / `.cpp` | Stateful admission decisions and best-effort regression rollback policy |
| `src/diagnostics/diagnostics.hpp` / `.cpp` | Window data, bottleneck classification, and versioned trace serialization |
| `src/benchmark/benchmark_runner.cpp` | Fresh-repository benchmark orchestration and JSON/CSV/Markdown reports |
| `src/hashing/blake3.cpp` / `include/pbe/blake3.hpp` | BLAKE3 hashing and digest encoding |
| `include/pbe/bounded_queue.hpp` | Reusable bounded producer/consumer queue |

Internal includes name their module, such as `storage/object_store.hpp` and `scheduler/active_worker_gate.hpp`. Public consumers include `pbe/engine.hpp`. CMake exposes `include` to consumers and keeps the `src` include root private to the engine and its internal tests.

The coordinator owns startup, recovery, shutdown, and final snapshot publication. During backup, only the metadata consumer uses the write-side catalog connection; CPU lookups and trusted-metadata queries use separate read-only connections. The consumer resolves all object futures before beginning a transaction, inserts entries before references, and releases dedup claims after commit. Both storage formats acknowledge only after durable publication. See [PIPELINE.md](PIPELINE.md) for the execution and cancellation protocol.

The monitor gathers window samples and applies the controller's returned decision to `ActiveWorkerGate`. The controller policy itself has no access to queues, storage, SQLite, or metrics, which makes warmup, admission changes, cooldown, and regression rollback directly testable.
