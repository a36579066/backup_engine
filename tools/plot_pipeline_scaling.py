#!/usr/bin/env python3
"""Plot retained pipeline campaign results; requires matplotlib."""

import argparse
import json
from pathlib import Path
from statistics import median

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def read_rows(folder):
    return [json.loads(line) for line in (folder / "results.jsonl").read_text().splitlines()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("reports/pipeline-scaling"))
    args = parser.parse_args()
    root = args.root
    fig, (left, right) = plt.subplots(1, 2, figsize=(12.6, 5.2), gridspec_kw={"width_ratios": [0.85, 1.35]})
    fig.suptitle("Measured backup pipeline acceleration", fontsize=17, fontweight="bold", x=0.08, ha="left")
    old = [row["backup_seconds"] for row in read_rows(root / "project-before")]
    new = [row["backup_seconds"] for row in read_rows(root / "final-project")]
    values = [median(old), median(new)]
    for index, (samples, value, color) in enumerate(zip((old, new), values, ("#64748b", "#16a34a"))):
        left.bar(index, value, width=0.58, color=color, zorder=2)
        left.errorbar(index, value, yerr=[[value - min(samples)], [max(samples) - value]], color="#0f172a", capsize=5, zorder=3)
        left.text(index, max(samples) + 0.8, f"{value:.2f} s", ha="center", fontsize=11)
    left.set_xticks([0, 1], ["Before", "Pipeline"])
    left.set_ylim(0, max(old) * 1.22)
    left.set_ylabel("Backup wall time (seconds)")
    left.set_title(f"10.016 GiB project: {values[0] / values[1]:.2f}x faster\n4,021 files; 4 CPU / 2 storage writers", fontsize=11, loc="left", pad=18)
    for folder, label, color in (
        ("writev-one-writer", "Random assets + small files", "#2563eb"),
        ("logs-after", "Unique compressible log chunks", "#16a34a"),
    ):
        rows = read_rows(root / folder)
        workers = sorted({row["workers"] for row in rows})
        one = median(row["backup_seconds"] for row in rows if row["workers"] == 1)
        speeds, lower, upper = [], [], []
        for count in workers:
            times = [row["backup_seconds"] for row in rows if row["workers"] == count]
            speed = one / median(times)
            speeds.append(speed)
            lower.append(speed - one / max(times))
            upper.append(one / min(times) - speed)
        right.errorbar(workers, speeds, yerr=[lower, upper], label=label, color=color,
                       marker="o", linewidth=2.2, capsize=4, markersize=6, zorder=3)
        right.annotate(f"{speeds[-1]:.2f}x", (workers[-1], speeds[-1]), xytext=(9, 4), textcoords="offset points", color=color, fontweight="bold")
    right.set_xscale("log", base=2)
    right.set_xticks([1, 2, 4, 8], ["1", "2", "4", "8"])
    right.set_xlim(0.9, 10.5)
    right.set_ylim(0.8, 4.3)
    right.set_xlabel("CPU workers")
    right.set_ylabel("Backup speedup vs 1 worker")
    right.set_title("10 GiB fixtures: CPU scaling depends on I/O\n1 storage writer; speedup normalized within each fixture", fontsize=11, loc="left", pad=18)
    right.legend(loc="upper left", fontsize=9, frameon=False)
    for ax in (left, right):
        ax.spines[["top", "right"]].set_visible(False)
        ax.grid(axis="y", alpha=0.18, zorder=0)
        ax.set_axisbelow(True)
    fig.text(0.08, 0.035, "macOS arm64, 8 logical CPUs; Zstd level 3; 256 MiB buffer budget. Two runs/setting; whiskers show run range.\nFresh repository and full verification per run. Local filesystem caches and background load were uncontrolled.", fontsize=8, color="#475569")
    fig.subplots_adjust(left=0.08, right=0.95, bottom=0.21, top=0.76, wspace=0.36)
    fig.savefig(root / "pipeline-scaling.png", dpi=180)
    fig.savefig(root / "pipeline-scaling.svg")
    plt.close(fig)
    print(root / "pipeline-scaling.png")


if __name__ == "__main__":
    main()
