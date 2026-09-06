#!/usr/bin/env python3
"""Summarize fixed/adaptive benchmark.json files by worker cap."""

import argparse
import json
import statistics
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    args = parser.parse_args()
    for report in args.reports:
        data = json.loads(report.read_text(encoding="utf-8"))
        groups = {}
        for run in data["runs"]:
            groups.setdefault((run["scheduler"], run["workers"]), []).append(run["elapsed_seconds"])
        print(f"## {report.parent.name}: {data.get('dataset', 'dataset')}")
        print("workers,fixed_median_s,adaptive_median_s,adaptive_delta_percent")
        workers = sorted({worker for _, worker in groups})
        for worker in workers:
            fixed = groups.get(("fixed", worker), [])
            adaptive = groups.get(("adaptive", worker), [])
            fixed_median = statistics.median(fixed) if fixed else None
            adaptive_median = statistics.median(adaptive) if adaptive else None
            delta = 100 * (adaptive_median / fixed_median - 1) if fixed_median and adaptive_median is not None else None
            print(f"{worker},{fixed_median if fixed_median is not None else 'n/a'},{adaptive_median if adaptive_median is not None else 'n/a'},{delta if delta is not None else 'n/a'}")


if __name__ == "__main__":
    main()
