# Benchmark workflow

For pipeline scaling on datasets above 10 GiB, use `tools/generate_pipeline_fixture.py` and `tools/benchmark_pipeline.py`. The generator supports incompressible assets (`--kind random`), compressible unique log chunks (`--kind logs`), and many-file projects (`--small-files 4000 --small-file-bytes 4096`). The harness retains per-run traces, summaries, binary hashes, process CPU time, and pack-size counts, fully verifies every snapshot, and removes each temporary repository. It leaves the fixture in the caller's directory for before/after comparisons. See [the pipeline campaign](../reports/pipeline-scaling/README.md) and [PIPELINE.md](PIPELINE.md).

Generate the seeded A-F fixtures:

```sh
python3 tools/generate_dataset.py --output fixtures --profiles A,B,C,D,E,F --size-mib 32
```

Run five fresh-repository trials for fixed and adaptive CPU admission limits:

```sh
python3 tools/run_benchmark.py --pbe build/pbe --fixtures fixtures --output reports/benchmark --repeat 5
```

Each profile produces JSON, CSV, and Markdown reports. The JSON and CSV preserve every run. The Markdown reports median, standard deviation, min/max spread, credit peak, worker-limit churn, controller time, and fixed/adaptive median differences by worker cap. The report states that filesystem cache state and machine load were not controlled. It does not claim that adaptive scheduling is always faster.

To compare object-writer concurrency, set `--writer-workers` while keeping the fixture and CPU worker count fixed. For example, run the same profile with `--writer-workers 1`, then `2`, then `4`; every trial uses fresh repositories. The default remains one writer until measurements on the target storage justify a different choice.

The F profile contains `base/` and `version-2/`; each trial backs up both trees to the same fresh repository and reports the second snapshot. The second run still reads the source in safe mode, while content addressing shows how much existing data it reuses. To exercise diagnostics with controlled pressure, pass `--test-writer-delay-ms N`, `--test-reader-delay-ms N`, or `--test-cpu-spin-ms N` to `pbe benchmark`. These inject writer/input delays or CPU work and do not model a real device.

To plot one report, install matplotlib and run `python3 tools/plot_results.py reports/benchmark/A/benchmark.csv`. The current benchmark is a local measurement harness; it does not control external disk caches, thermal state, background jobs, or OS-level I/O throttling.
