# Parallel Backup & Deduplication Engine

Reconstructed architecture milestone 3: Memory-bounded batch packs.

Pack-v2 collects a bounded in-memory batch, writes its objects to one immutable pack, and acknowledges the batch after durable publication. Batch size is derived from the memory credit capacity. Files-v1 remains supported.

The engine also includes restore/full verification, source-change checks, bounded memory credits, crash checkpoints, diagnostics, adaptive CPU admission, and benchmark tooling.

This milestone was reconstructed from the available source snapshots. Its requested Git timestamps do not represent a recovered original development log. The final milestone restores the complete current project.

Build: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release

Compile: cmake --build build --parallel

Validate: ctest --test-dir build --output-on-failure

