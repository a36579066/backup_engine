#!/usr/bin/env python3
"""Generate random I/O pressure or compressible unique log chunks for pipeline scaling."""

import argparse
import json
import random
from pathlib import Path

from benchmark_storage_formats import make_fixture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--kind", choices=("random", "logs"), default="logs")
    parser.add_argument("--asset-count", type=int, default=20)
    parser.add_argument("--asset-mib", type=int, default=512)
    parser.add_argument("--small-files", type=int, default=400)
    parser.add_argument("--small-file-bytes", type=int, default=58)
    parser.add_argument("--seed", type=int, default=20261102)
    args = parser.parse_args()
    if min(args.asset_count, args.asset_mib) < 1 or args.small_files < 0 or args.small_file_bytes < 32:
        parser.error("asset count and size must be positive; small-files must be nonnegative; small-file-bytes must be at least 32")
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("output must be new or empty")
    args.output.mkdir(parents=True, exist_ok=True)
    if args.kind == "random":
        source, logical, files = make_fixture(args.output, args.asset_count, args.asset_mib, args.small_files)
        if args.small_file_bytes != 58:
            for i in range(args.small_files):
                prefix = f"small-file-index={i:06d}; ".encode()
                (source / "small" / f"doc-{i:06d}.txt").write_bytes(prefix + b"x" * (args.small_file_bytes - len(prefix)))
    else:
        source = args.output / "source"
        (source / "assets").mkdir(parents=True)
        rng = random.Random(args.seed)
        records = []
        for i in range(1024):
            records.append(f'2026-11-02T08:{i // 60:02d}:{i % 60:02d}Z service=backup request={i:08d} operation=read status=ok bytes={rng.randrange(4096, 1048576)} duration_us={rng.randrange(10, 2000)}\n'.encode())
        block = b"".join(records)
        repeated = block * (1048576 // len(block) + 2)
        sequence = 0
        for asset in range(args.asset_count):
            with (source / "assets" / f"asset-{asset:02d}.log").open("wb") as out:
                remaining = args.asset_mib * 1024**2
                while remaining:
                    amount = min(1048576, remaining)
                    prefix = f"unique_chunk_sequence={sequence:020d}\n".encode()
                    out.write(prefix + repeated[:amount - len(prefix)])
                    remaining -= amount
                    sequence += 1
            print(f"created unique compressible log asset {asset + 1}/{args.asset_count}", flush=True)
        (source / "small").mkdir()
        for i in range(args.small_files):
            prefix = f"small-file-index={i:06d}; ".encode()
            (source / "small" / f"doc-{i:06d}.txt").write_bytes(prefix + b"x" * (args.small_file_bytes - len(prefix)))
        prefix = b"PBE generated structured log scaling fixture\n"
        (source / "manifest.txt").write_bytes(prefix + b"m" * (341 - len(prefix)))
        paths = [path for path in source.rglob("*") if path.is_file()]
        logical = sum(path.stat().st_size for path in paths)
        files = len(paths)
    paths = [path for path in source.rglob("*") if path.is_file()]
    logical = sum(path.stat().st_size for path in paths)
    files = len(paths)
    manifest = {"kind": args.kind, "logical_bytes": logical, "files": files,
        "seed": args.seed, "asset_count": args.asset_count, "asset_mib": args.asset_mib,
        "small_files": args.small_files, "small_file_bytes": args.small_file_bytes,
        "description": "log blocks repeat structured records with a unique prefix per 1 MiB chunk" if args.kind == "logs" else "incompressible random assets with individually named small documents"}
    (args.output / "fixture.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"SOURCE={source.resolve()}; bytes={logical}; files={files}", flush=True)


if __name__ == "__main__":
    main()
