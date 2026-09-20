#!/usr/bin/env python3
"""Generate A-F datasets and run fresh-repository fixed/adaptive comparisons."""

import argparse
import json
import platform
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pbe", type=Path, default=Path("build/pbe"))
    parser.add_argument("--fixtures", type=Path, default=Path("fixtures"))
    parser.add_argument("--output", type=Path, default=Path("reports/benchmark"))
    parser.add_argument("--profiles", default="A,B,C,D,E,F")
    parser.add_argument("--size-mib", type=int, default=32)
    parser.add_argument("--small-files", type=int, default=1000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--writer-workers", type=int, default=1, help="number of concurrent object writers")
    parser.add_argument("--storage-format", choices=("files-v1", "pack-v2"), default="files-v1")
    parser.add_argument("--memory-mib", type=int, default=256)
    parser.add_argument("--seed", type=int, default=20261101)
    parser.add_argument("--test-cpu-spin-ms", type=int, default=0,
                        help="inject controlled CPU work per chunk for diagnostics")
    parser.add_argument("--force-fixtures", action="store_true", help="replace generated profile directories in --fixtures")
    args = parser.parse_args()
    pbe = args.pbe.resolve()
    fixtures = args.fixtures.resolve()
    output = args.output.resolve()
    profiles = [value.strip().upper() for value in args.profiles.split(",") if value.strip()]
    subprocess.run([
        "python3", str(Path(__file__).with_name("generate_dataset.py")),
        "--output", str(fixtures), "--profiles", ",".join(profiles),
        "--size-mib", str(args.size_mib), "--small-files", str(args.small_files),
        "--seed", str(args.seed),
        *( ["--force"] if args.force_fixtures else [] ),
    ], check=True)
    output.mkdir(parents=True, exist_ok=True)
    manifest = {
        "dataset_profiles": profiles,
        "seed": args.seed,
        "size_mib": args.size_mib,
        "small_files": args.small_files,
        "workers": [1, 2, 4, 8],
        "schedulers": ["fixed", "adaptive"],
        "repeat": args.repeat,
        "writer_workers": args.writer_workers,
        "storage_format": args.storage_format,
        "memory_mib": args.memory_mib,
        "test_cpu_spin_ms": args.test_cpu_spin_ms,
        "system": platform.platform(),
        "pbe_executable": str(pbe),
    }
    (output / "suite.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    for index, profile in enumerate(profiles):
        profile_output = output / profile
        subprocess.run([
            str(pbe), "benchmark", "--dataset", str(fixtures / profile),
            "--workers", "1,2,4,8", "--repeat", str(args.repeat),
            "--compare-schedulers", "fixed,adaptive", "--memory-mib", str(args.memory_mib),
            "--writer-workers", str(args.writer_workers),
            "--storage-format", args.storage_format,
            "--test-cpu-spin-ms", str(args.test_cpu_spin_ms),
            "--output", str(profile_output),
        ], check=True)
        report = profile_output / "benchmark.json"
        data = json.loads(report.read_text(encoding="utf-8"))
        data["dataset_profile"] = profile
        data["dataset_seed"] = args.seed + index
        data["fixture_size_mib"] = args.size_mib
        data["small_files"] = args.small_files
        report.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
