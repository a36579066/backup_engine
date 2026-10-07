# Parallel Backup & Deduplication Engine

Reconstructed architecture milestone 4: Streaming object packs.

Pack-v2 streams objects toward a 128 MiB target or 256-object cap, releasing each payload and memory credit after staging. It publishes after reaching its limit or 2 ms idle. The coordinator still waits at each file boundary.

The engine also includes restore/full verification, source-change checks, bounded memory credits, crash checkpoints, diagnostics, adaptive CPU admission, and benchmark tooling.

This milestone was reconstructed from the available source snapshots. Its requested Git timestamps do not represent a recovered original development log. The final milestone restores the complete current project.

Build: cmake -S . -B build -DCMAKE_BUILD_TYPE=Release

Compile: cmake --build build --parallel

Validate: ctest --test-dir build --output-on-failure

