# Testing and validation

## Run the tests

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

CTest runs two suites. `pbe_tests` exercises the engine API and prints each case as it runs. `pbe_cli_smoke` runs the built executable through a complete init, backup, list, show, verify, stats, explain, restore, and error-handling flow.

## Engine test coverage

`pbe_tests` checks:

- BLAKE3 empty and `abc` vectors; fixed chunk sizes of 0, 1, 1 MiB−1, 1 MiB, and 1 MiB+1.
- Zstandard compression and raw fallback, byte-for-byte restoration, deduplication, empty and hidden files, Unicode names, symlinks, and worker counts 1/2/4/8.
- Cross-file pipelines with 300 files, 100 unique objects, 1/8 CPU workers, both storage formats, a one-chunk budget, batched commits, cached reuse, full verification, and restored content.
- Metadata ACK failure/cancellation, atomic cross-file commits on legacy four-column files-v1 catalogs, rollback on constraints, and single/gathered pack-write offsets and credit release.
- Duplicate continuation registration racing owner completion across concurrent subscribers.
- Memory-credit RAII/current/peak accounting; fixed worker start/join; in-flight claim sharing, completed/failed claim cleanup, and waiter access to completed state.
- CPU, writer, memory, and input bottleneck classification rules; adaptive policy warmup, single-permit adjustment, regression rollback, and fixed-mode hold behavior.
- Object-store raw round trip, read/hash timing collection, no-replace duplicate publication, and bounded verification-cache eviction.
- File and directory mode/mtime restoration; trusted metadata reuses unchanged source files; every worker result restores to the same tree.
- Full and empty bounded-queue cancellation, close wakeups, FIFO rejection/skip behavior, source mutation detection, repository/source overlap, and restore no-overwrite behavior.
- Damaged object magic, version, codec, flags, lengths, and payload; backup refuses to reuse damaged objects through both normal deduplication and trusted metadata. Malformed catalog paths and object IDs are rejected.
- Writer failure with duplicate workers waiting, SIGINT cancellation, optional diagnostic-report write failure, and recovery after each of the five process-crash checkpoints.

The CLI suite checks exit codes, snapshot visibility, JSON stats and explanation, schema v6 stage timing and writer-count fields, object verification/I/O timings during trusted-metadata reuse, benchmark report generation, exact restored paths/content, symlink handling, refusal of invalid inputs, and rejection of options that do not apply to the selected command. It also confirms that unreadable source traversal and corrupted trusted-metadata objects fail without publishing a complete snapshot. The filesystem APIs and tests use POSIX behavior; permission-specific checks skip when the process can bypass mode bits, and FIFO-specific checks run on macOS and Linux.

## Controlled diagnostics and performance

The four injected CPU, writer, memory, and input scenarios can be repeated with `tools/run_diagnostic_scenarios.py`. It preserves the JSONL windows and fully verifies every resulting snapshot. A–F fixed/adaptive comparisons use fresh repositories and preserve each run in JSON/CSV/Markdown; `--writer-workers N` compares a selected object-writer count. The original 10 GiB diagnosis is in [the bottleneck report](../reports/bottleneck-10gb/README.md), the 1/2/4 comparison in [the writer-pool report](../reports/bottleneck-10gb-writer-pool/README.md), and the per-syscall stage breakdown in [the writer-stage report](../reports/bottleneck-10gb-writer-stages/README.md). The A–F campaign is summarized in [the benchmark report](../reports/benchmark-v1.1/summary.md).

## Sanitizers and platform checks

Use the sanitizer supported by the local compiler/runtime, for example:

```sh
cmake -S . -B build-ubsan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=undefined -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=undefined"
cmake --build build-ubsan --parallel
ctest --test-dir build-ubsan --output-on-failure --timeout 180
```

CI builds and tests on macOS and Ubuntu. AddressSanitizer, ThreadSanitizer, and physical power-loss behavior remain compiler, runtime, and hardware dependent; record unavailable runtimes as untested rather than treating a normal build as equivalent evidence.
