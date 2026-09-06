#!/usr/bin/env python3
"""Plot benchmark.csv profiles to PNG; requires matplotlib."""

import argparse
import csv
from collections import defaultdict
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path)
    parser.add_argument("--output", type=Path, default=Path("benchmark.png"))
    args = parser.parse_args()
    try:
        import matplotlib.pyplot as plt
    except ImportError as error:
        raise SystemExit("install matplotlib to create plots: python3 -m pip install matplotlib") from error
    groups = defaultdict(list)
    with args.csv.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            groups[(row["scheduler"], int(row["workers"]))].append(float(row["elapsed_seconds"]))
    fig, ax = plt.subplots(figsize=(8, 5))
    for scheduler in sorted({key[0] for key in groups}):
        workers = sorted(key[1] for key in groups if key[0] == scheduler)
        medians = [sorted(groups[(scheduler, worker)])[len(groups[(scheduler, worker)]) // 2] for worker in workers]
        ax.plot(workers, medians, marker="o", label=scheduler)
    ax.set(xlabel="CPU worker cap", ylabel="Median backup seconds", title="PBE fixed and adaptive benchmark")
    ax.set_xticks(sorted({key[1] for key in groups}))
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.output, dpi=160)
    print(args.output)


if __name__ == "__main__":
    main()
