# ParaVault monitoring dashboard

The dashboard is a local web app for starting and monitoring ParaVault backups. It uses Python's standard library and the existing ParaVault CLI; it does not add a runtime package dependency.

Build ParaVault, then start the dashboard from the project directory:

```sh
python3 tools/paravault_dashboard.py --engine build-release/paravault
```

Open [http://127.0.0.1:8765](http://127.0.0.1:8765). The server binds to a loopback address only. Press Ctrl-C in the terminal to stop the dashboard server.

Enter a repository path and choose `files-v1` or `pack-v2` when initializing a new repository. The storage format is fixed for that repository. Enter a source folder, choose fixed or adaptive scheduling, set CPU worker count, object writer count, and pipeline memory budget, then start a backup. The dashboard polls the repository and diagnostic trace every 1.5 seconds. It shows completed files and chunks, processed bytes, recent throughput, the latest bottleneck diagnosis, queue and memory signals, the repository storage format, and recent complete snapshots. The total source file count is not available while the engine is processing; the display reports completed files without inventing a percentage.

The dashboard can also pick up an active CLI backup by checking the repository lock and its `IN_PROGRESS` snapshot. Backups started by the dashboard can be safely cancelled with the stop button, which sends SIGINT to ParaVault so it can close queues and clean up. A backup already started from another terminal cannot be stopped from the dashboard.

The dashboard reads `catalog.sqlite` in read-only mode and reads the existing JSONL reports. Starting, stopping, or initializing a repository runs the local ParaVault CLI as the same operating-system user. No source data is sent to another machine.
