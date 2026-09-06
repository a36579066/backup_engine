# Parallel Backup & Deduplication Engine

Reconstructed architecture milestone 1: CPU workers with one object writer.

Raw chunks pass through a bounded CPU queue for BLAKE3, deduplication and Zstd. One storage thread durably publishes loose objects. The coordinator commits SQLite references after acknowledgements. Only files-v1 and one writer are supported.

The engine also includes restore/full verification, source-change checks, bounded memory credits, crash checkpoints, diagnostics, adaptive CPU admission, and benchmark tooling.

This milestone was reconstructed from the available source snapshots. Its requested Git timestamps do not represent a recovered original development log. The final milestone restores the complete current project.

Build: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release

Compile: cmake --build build --parallel

Validate: ctest --test-dir build --output-on-failure

