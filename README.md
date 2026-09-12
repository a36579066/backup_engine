# Parallel Backup & Deduplication Engine

Reconstructed architecture milestone 2: Concurrent object-writer pool.

A configurable pool of 1 to 32 storage workers durably publishes loose objects. CPU workers release admission before waiting for object ACKs. Only files-v1 is supported.

The engine also includes restore/full verification, source-change checks, bounded memory credits, crash checkpoints, diagnostics, adaptive CPU admission, and benchmark tooling.

This milestone was reconstructed from the available source snapshots. Its requested Git timestamps do not represent a recovered original development log. The final milestone restores the complete current project.

Build: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release

Compile: cmake --build build --parallel

Validate: ctest --test-dir build --output-on-failure

