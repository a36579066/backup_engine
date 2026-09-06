#!/usr/bin/env python3
"""Create deterministic A-F PBE benchmark inputs using only Python's standard library."""

import argparse
import json
import random
import shutil
from pathlib import Path


def write_repeated(path: Path, block: bytes, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as out:
        remaining = size
        while remaining:
            chunk = block[: min(len(block), remaining)]
            out.write(chunk)
            remaining -= len(chunk)


def random_block(rng: random.Random, size: int) -> bytes:
    return rng.randbytes(size)


def write_random(path: Path, rng: random.Random, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as out:
        remaining = size
        while remaining:
            amount = min(1024 * 1024, remaining)
            out.write(rng.randbytes(amount))
            remaining -= amount


def generate(output: Path, profile: str, seed: int, size_mib: int, small_files: int, force: bool) -> None:
    root = output / profile
    if root.exists():
        if not force:
            raise FileExistsError(f"refusing to replace existing dataset: {root}; choose another output path or pass --force")
        shutil.rmtree(root)
    root.mkdir(parents=True)
    rng = random.Random(seed)
    size = size_mib * 1024 * 1024
    block = 64 * 1024

    if profile == "A":
        text = (b"# deterministic source fixture\nint backup_chunk = 1048576;\n" * 1024)
        write_repeated(root / "src" / "source.txt", text, size)
        for index in range(16):
            write_repeated(root / "logs" / f"run-{index:02d}.log", text, max(1024, size // 16))
    elif profile == "B":
        write_random(root / "random.bin", rng, size)
        for index in range(4):
            write_random(root / "parts" / f"part-{index}.bin", rng, max(block, size // 8))
    elif profile == "C":
        common = random_block(rng, 1024 * 1024)
        for index in range(32):
            write_repeated(root / "copies" / f"copy-{index:03d}.bin", common, max(1, size // 32))
    elif profile == "D":
        per_file = max(64, size // max(1, small_files))
        for index in range(small_files):
            body = f"fixture={index:06d}\n".encode("utf-8")
            write_repeated(root / "small" / f"f-{index:06d}.txt", body, per_file)
    elif profile == "E":
        text = (b"mixed profile text payload, stable and compressible\n" * 256)
        write_repeated(root / "text" / "notes.txt", text, size // 4)
        write_random(root / "random" / "noise.bin", rng, size // 4)
        common = random_block(rng, 1024 * 1024)
        for index in range(4):
            write_repeated(root / "duplicates" / f"duplicate-{index}.bin", common, size // 8)
        (root / "small").mkdir(parents=True, exist_ok=True)
        for index in range(100):
            (root / "small" / f"s-{index:03d}.txt").write_text(f"mixed small file {index}\n", encoding="utf-8")
    elif profile == "F":
        base = root / "base"
        version = root / "version-2"
        version.mkdir(parents=True, exist_ok=True)
        per_file = max(1024 * 1024, size // 12)
        for index in range(12):
            write_random(base / f"file-{index:02d}.bin", rng, per_file)
            shutil.copyfile(base / f"file-{index:02d}.bin", version / f"file-{index:02d}.bin")
        changed = version / "file-00.bin"
        with changed.open("r+b") as out:
            out.seek(0)
            out.write(b"version-two-changed-prefix")
    else:
        raise ValueError(f"unknown profile {profile}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("fixtures"))
    parser.add_argument("--profiles", default="A,B,C,D,E,F", help="comma-separated subset of A,B,C,D,E,F")
    parser.add_argument("--seed", type=int, default=20261101)
    parser.add_argument("--size-mib", type=int, default=32)
    parser.add_argument("--small-files", type=int, default=1000)
    parser.add_argument("--force", action="store_true", help="replace existing generated profile directories")
    args = parser.parse_args()
    if args.size_mib < 1 or args.small_files < 1:
        parser.error("size and small-files must be positive")
    profiles = [value.strip().upper() for value in args.profiles.split(",") if value.strip()]
    if any(profile not in "ABCDEF" or len(profile) != 1 for profile in profiles):
        parser.error("profiles must be selected from A,B,C,D,E,F")
    args.output.mkdir(parents=True, exist_ok=True)
    existing_profiles = [args.output / profile for profile in profiles if (args.output / profile).exists()]
    if existing_profiles and not args.force:
        parser.error("refusing to replace existing datasets: " + ", ".join(map(str, existing_profiles)) + "; pass --force or choose another output path")
    if (args.output / "manifest.json").exists() and not args.force:
        parser.error(f"refusing to replace existing manifest at {args.output / 'manifest.json'}; pass --force or choose another output path")
    for index, profile in enumerate(profiles):
        generate(args.output, profile, args.seed + index, args.size_mib, args.small_files, args.force)
        print(f"created {args.output / profile}")
    manifest = {
        "profiles": profiles,
        "seed": args.seed,
        "profile_seeds": {profile: args.seed + index for index, profile in enumerate(profiles)},
        "size_mib": args.size_mib,
        "small_files": args.small_files,
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
