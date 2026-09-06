#!/usr/bin/env python3
"""Run four controlled PBE bottleneck scenarios and preserve their traces."""

import argparse
import json
import platform
import random
import re
import shutil
import subprocess
import tempfile
from pathlib import Path


SCENARIOS = [
    ("cpu-heavy", "CPU_LIMITED", ["--scheduler", "adaptive", "--workers", "8", "--max-workers", "8", "--memory-mib", "64", "--test-cpu-spin-ms", "250"]),
    ("writer-throttled", "WRITER_LIMITED", ["--scheduler", "adaptive", "--workers", "4", "--max-workers", "4", "--memory-mib", "64", "--test-writer-delay-ms", "300"]),
    ("memory-limited", "MEMORY_LIMITED", ["--scheduler", "adaptive", "--workers", "8", "--max-workers", "8", "--memory-mib", "16", "--test-writer-delay-ms", "300"]),
    ("input-limited", "INPUT_LIMITED", ["--scheduler", "fixed", "--workers", "4", "--max-workers", "4", "--memory-mib", "64", "--test-reader-delay-ms", "100"]),
]


def run(command: list[str]) -> str:
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    return result.stdout


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pbe", type=Path, default=Path("build-release/pbe"))
    parser.add_argument("--output", type=Path, default=Path("reports/diagnostics"))
    parser.add_argument("--size-mib", type=int, default=32)
    parser.add_argument("--seed", type=int, default=20261117)
    args = parser.parse_args()
    if args.size_mib < 16:
        parser.error("--size-mib must be at least 16 MiB for stable controller windows")
    pbe = args.pbe.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)

    results = []
    with tempfile.TemporaryDirectory(prefix="pbe-diagnostics-") as temporary_name:
        temporary = Path(temporary_name)
        dataset = temporary / "random.bin"
        rng = random.Random(args.seed)
        with dataset.open("wb") as file:
            for _ in range(args.size_mib):
                file.write(rng.randbytes(1024 * 1024))
        cpu_fixture_root = temporary / "cpu-fixtures"
        cpu_nominal_size = max(args.size_mib, 32)
        subprocess.run([
            "python3", str(Path(__file__).with_name("generate_dataset.py")),
            "--output", str(cpu_fixture_root), "--profiles", "B",
            "--size-mib", str(cpu_nominal_size), "--seed", str(args.seed),
        ], check=True, capture_output=True, text=True)
        cpu_fixture = cpu_fixture_root / "B"
        cpu_fixture_bytes = sum(path.stat().st_size for path in cpu_fixture.rglob("*") if path.is_file())
        cpu_fixture_size_mib = (cpu_fixture_bytes + (1 << 20) - 1) // (1 << 20)

        for name, expected, options in SCENARIOS:
            repo = temporary / f"repo-{name}"
            source = temporary / f"source-{name}"
            if name == "cpu-heavy":
                shutil.copytree(cpu_fixture, source)
                fixture_size_mib = cpu_fixture_size_mib
            else:
                source.mkdir()
                shutil.copyfile(dataset, source / "random.bin")
                fixture_size_mib = args.size_mib
            run([str(pbe), "init", "--repo", str(repo)])
            stdout = run([str(pbe), "backup", "--repo", str(repo), "--source", str(source), *options])
            match = re.search(r"^Run ID:\s+(\S+)$", stdout, re.MULTILINE)
            if not match:
                raise RuntimeError(f"could not find run ID in {name} backup output")
            run_id = match.group(1)
            run([str(pbe), "verify", "--repo", str(repo), "--snapshot", re.search(r"^Snapshot:\s+(\S+)$", stdout, re.MULTILINE).group(1), "--full"])
            repo_reports = repo / "reports"
            rows = [json.loads(line) for line in (repo_reports / f"{run_id}.jsonl").read_text(encoding="utf-8").splitlines()]
            summary = json.loads((repo_reports / f"{run_id}.summary.json").read_text(encoding="utf-8"))
            shutil.copyfile(repo_reports / f"{run_id}.jsonl", output / f"{name}.jsonl")
            diagnoses = sorted({row["diagnosis"] for row in rows})
            actions = sorted({row["action"] for row in rows})
            limit_changes = sum(row["action"] in {"INCREASE_CPU_ADMISSION", "DECREASE_CPU_ADMISSION", "ROLLBACK_CPU_ADMISSION"} for row in rows)
            results.append({
                "scenario": name,
                "fixture_size_mib": fixture_size_mib,
                "expected_diagnosis": expected,
                "observed_diagnoses": diagnoses,
                "observed_actions": actions,
                "cpu_limit_changes": limit_changes,
                "expected_diagnosis_observed": expected in diagnoses,
                "run_id": run_id,
                "trace_file": f"{name}.jsonl",
                "snapshot_verified_full": True,
                **summary,
            })

    report = {
        "schema_version": 1,
        "system": platform.platform(),
        "pbe_executable": str(pbe),
        "fixture": {"kind": "seeded random bytes", "seed": args.seed, "base_size_mib": args.size_mib,
                    "cpu_profile": "B", "cpu_nominal_size_mib": cpu_nominal_size, "cpu_actual_size_mib": cpu_fixture_size_mib},
        "scenarios": results,
    }
    (output / "diagnostics.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# Controlled diagnostic scenarios",
        "",
        f"Environment: `{platform.platform()}`; seeded random fixtures use seed `{args.seed}`. CPU-heavy uses profile B ({cpu_nominal_size} MiB nominal, {cpu_fixture_size_mib} MiB actual); other scenarios use {args.size_mib} MiB single-file data.",
        "Each run used a fresh repository and passed full snapshot verification. The injected limits are test controls, not models of specific hardware.",
        "",
        "| Scenario | Expected | Observed diagnoses | Expected observed | CPU limit changes | Credit peak / budget |",
        "|---|---|---|---|---:|---:|",
    ]
    for item in results:
        lines.append(
            f"| [{item['scenario']}]({item['trace_file']}) ({item['fixture_size_mib']} MiB) | `{item['expected_diagnosis']}` | `{', '.join(item['observed_diagnoses'])}` | "
            f"{'yes' if item['expected_diagnosis_observed'] else 'no'} | {item['cpu_limit_changes']} | "
            f"{item['memory_credit_peak_bytes'] / 1048576:.2f} / {item['memory_budget_bytes'] / 1048576:.0f} MiB |"
        )
    lines.extend(["", "Raw decision windows are in the adjacent JSONL files; `diagnostics.json` records run IDs and full-run metrics.", ""])
    (output / "README.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote controlled diagnostics to {output}")


if __name__ == "__main__":
    main()
