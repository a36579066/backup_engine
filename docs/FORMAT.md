# Repository and object format

## Repository

`repo.toml` fixes format version 1, 1 MiB chunks, and BLAKE3-256. `catalog.sqlite` stores snapshots, entries, object metadata, and ordered file-to-object references. SQLite uses WAL and `synchronous=FULL`. `objects/` stores immutable content-addressed files, `staging/` stores temporary writes, and `reports/` stores optional run metrics.

## PBEOBJ1

Every object starts with a 64-byte little-endian header:

| Offset | Size | Field |
|---:|---:|---|
| 0 | 8 | `PBEOBJ1\0` |
| 8 | 2 | format version 1 |
| 10 | 1 | codec: 0 raw, 1 Zstandard |
| 11 | 5 | reserved, all zero |
| 16 | 8 | raw length |
| 24 | 8 | payload length |
| 32 | 32 | BLAKE3 digest of raw bytes |
| 64 | variable | payload |

Readers cap raw length at 1 MiB and stored length at `ZSTD_compressBound(1 MiB)`, check the exact object file length, decompress when needed, and recompute the digest. The digest in the path is the lowercase hex encoding of the raw data digest.

## Snapshot publication

An object is written under `staging/`, synchronized, and linked into its final digest path without replacing an existing object. The containing object directory is synchronized before the writer acknowledges completion. SQLite references are then recorded. A snapshot becomes visible only after chunk-map checks and a committed `COMPLETE` transaction.
