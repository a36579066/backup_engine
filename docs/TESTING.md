# Testing and validation

## Run the tests

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build --parallel
ctest --test-dir build --output-on-failure
```

CTest runs six suites. `pbe_pipeline_runtime_tests` links only the generic runtime and thread library; its nine cases use integer queues to check registered-source execution on the caller thread, source/worker overlap, ordered draining, cancellation, source/worker/observer/preparation failures, scope-exit joining, and invalid registration/lifecycle calls. They also verify that worker failure wakes a source's private wait exactly once and that `run()` joins workers before rethrowing a source failure. `pbe_tests` exercises the engine API and prints each case as it runs. `pbe_cli_smoke` runs the built executable through a complete init, backup, list, show, verify, stats, explain, restore, and error-handling flow.

## Publication on external filesystems

`pbe_file_publication_tests` checks complete-file visibility, duplicate preservation, missing-source failure, dangling destination symlinks, and eight concurrent publishers competing for one destination. It also checks whether successful publication consumes the staging path and that catalog query connections reject metadata writes. On macOS, a filesystem that rejects hard links uses the exclusive-rename fallback; other publication errors remain fatal. Pack recovery preserves OS sidecars and unrelated filenames while removing unindexed repository packs.

To exercise the actual filesystem rather than a simulated error, pass an existing writable parent directory. The test creates and removes only its own temporary subdirectories:

```sh
build-release/pbe_file_publication_tests "/Volumes/NO NAME/test"
```

Process-crash tests and full verification still apply to both repository formats. Filesystem capability flags alone are not proof of an operation's behavior; record the actual publication and collision test results. Abrupt device removal and physical power loss require separate validation.

The optional external-filesystem integration runner exercises both formats at five crash checkpoints, verifies the old snapshot before external database inspection, starts the next writer, fully verifies and restores the recovered snapshot, and checks SHA-256 equality and orphan-pack cleanup. It removes its own small fixtures after success and retains failed fixtures for investigation. Use a SQLite shell matching the engine's library; on this Mac both the engine and `/usr/bin/sqlite3` use the system library:

```sh
python3 tests/external_repository.py --engine build-release/paravault \
  --parent "/Volumes/NO NAME/test" --sqlite /usr/bin/sqlite3 \
  --output reports/external-filesystem-check
```

On the tested FAT32 filesystem, native read-only catalog handles could report `SQLITE_IOERR_WRITE` during WAL maintenance. The engine uses a read/write handle with SQLite `query_only=ON` there. SQL metadata writes remain prohibited for reader connections; filesystem-level SQLite housekeeping is allowed.

## I/O scheduling contracts

`pbe_io_scheduler_tests` checks batch limits against worst-case credits, invalid configuration, holding chunks during the read phase, flushing before acquiring more credit, and waiting for every durable ACK even when completion order differs. It also checks propagation of a failed ACK and credit release.

`pbe_io_scheduling_cli` exercises real backups in files-v1 and pack-v2 with one-chunk and larger budgets, fixed/adaptive CPU admission, multiple writers, explicit batch caps, duplicate chunks across batches/files, empty files and symlinks. Each result is fully verified and restored byte-for-byte. It checks trusted-metadata reuse, switching policies in an existing repository, built-in benchmark option/report forwarding, crash recovery at object publication/catalog commit, and SIGINT while the Reader is observably draining a delayed batch.

```sh
ctest --test-dir build -R 'pbe_io_scheduler_tests|pbe_io_scheduling_cli' --output-on-failure
```

## Code formatting

The repository's [`.clang-format`](../.clang-format) uses four-space indentation, a 100-column target, expanded short functions/control flow, blank lines between definitions, and one constructor initializer per line. Code comments use English and explain stage responsibilities, queue handoffs, completion notifications, and resource lifetimes. UI text and Unicode path/content fixtures keep their original language.

Run this from the repository root to check the C++ source and public headers:

```sh
find src include -type f \( -name '*.cpp' -o -name '*.hpp' \) -exec clang-format --dry-run --Werror {} +
```

Replace `--dry-run --Werror` with `-i` to format in place. Use clang-format 20; if Homebrew LLVM is not on `PATH`, its formatter is available at `$(brew --prefix llvm)/bin/clang-format`.

## Test the framework on its own

```sh
cmake --build build --target pbe_pipeline_runtime_tests --parallel
ctest --test-dir build -R '^pbe_pipeline_runtime_tests$' --output-on-failure
```

These tests do not link the backup engine, SQLite, or Zstandard. End-to-end tests below also cover the numbered stages after the backup wiring registers them with the same runtime.

## Engine test coverage

`pbe_tests` checks:

- BLAKE3 empty and `abc` vectors; fixed chunk sizes of 0, 1, 1 MiB−1, 1 MiB, and 1 MiB+1.
- Zstandard compression and raw fallback, byte-for-byte restoration, deduplication, empty and hidden files, Unicode names, symlinks, and worker counts 1/2/4/8.
- Cross-file pipelines with 300 files, 100 unique objects, 1/8 CPU workers, both storage formats, a one-chunk budget, batched commits, cached reuse, full verification, and restored content.
- Pipeline scope exit with unresolved writes and duplicate claims in both storage formats: cancellation joins workers promptly, never publishes a complete snapshot, and permits a subsequent backup and full verification.
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

The four injected CPU, writer, memory, and input scenarios can be repeated with `tools/run_diagnostic_scenarios.py`. It preserves the JSONL windows and fully verifies every resulting snapshot. A–F fixed/adaptive comparisons use fresh repositories and preserve each run in JSON/CSV/Markdown; `--writer-workers N` compares a selected object-writer count. The original 10 GiB diagnosis is in the bottleneck report (local report; not included), the 1/2/4 comparison in the writer-pool report (local report; not included), and the per-syscall stage breakdown in the writer-stage report (local report; not included). The A–F campaign is summarized in the benchmark report (local report; not included).

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
