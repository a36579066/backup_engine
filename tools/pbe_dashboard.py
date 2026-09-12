#!/usr/bin/env python3
"""Run the local PBE backup monitoring dashboard (Python standard library only)."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.parse
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
STATIC = Path(__file__).resolve().parent / "dashboard"
LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}
RUN_ID_RE = re.compile(r"^Run ID:\s+(\S+)\s*$", re.MULTILINE)


def json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False).encode("utf-8")


def parse_trace(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as trace:
            for line in trace:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(row, dict) and isinstance(row.get("observations"), dict):
                    rows.append(row)
    except OSError:
        return None
    if not rows:
        return None

    totals: dict[str, float] = {
        "completed_chunks": 0,
        "completed_raw_bytes": 0,
        "completed_unique_raw_bytes": 0,
        "progress_file_count": 0,
    }
    for row in rows:
        observations = row["observations"]
        for key in totals:
            totals[key] += float(observations.get(key, 0) or 0)
    latest = rows[-1]
    observations = latest["observations"]
    return {
        "run_id": latest.get("run_id", path.stem),
        "window_count": len(rows),
        "latest": latest,
        "totals": {key: int(value) for key, value in totals.items()},
        "throughput_history": [
            float(row.get("observations", {}).get("throughput_bytes_per_sec", 0) or 0)
            for row in rows[-36:]
        ],
        "last_seen_epoch": path.stat().st_mtime,
        "active_limit": latest.get("active_limit_after"),
        "max_workers": None,
        "diagnosis": latest.get("diagnosis", "UNKNOWN"),
        "action": latest.get("action", "HOLD"),
        "outcome": latest.get("outcome", "OBSERVED"),
        "input_queue_fill": float(observations.get("input_queue_fill_ratio", 0) or 0),
        "object_queue_fill": float(observations.get("object_queue_fill_ratio", 0) or 0),
        "writer_busy": float(observations.get("writer_busy_ratio", 0) or 0),
        "cpu_compute": float(observations.get("cpu_compute_ratio", 0) or 0),
        "memory_used": float(observations.get("memory_credit_used_ratio", 0) or 0),
        "memory_wait": float(observations.get("memory_credit_wait_ratio", 0) or 0),
        "elapsed_ms": float(latest.get("elapsed_ms", 0) or 0),
    }


def read_repository(repo: Path) -> dict[str, Any]:
    database = repo / "catalog.sqlite"
    storage_format = "files-v1"
    config = repo / "repo.toml"
    if config.is_file():
        match = re.search(r'^\s*storage_format\s*=\s*"([^"]+)"\s*$', config.read_text(encoding="utf-8"), re.MULTILINE)
        if match:
            storage_format = match.group(1)
    result: dict[str, Any] = {
        "path": str(repo),
        "initialized": (database.is_file() and (repo / "repo.toml").is_file()),
        "storage_format": storage_format,
        "snapshots": [],
        "stats": {"snapshot_count": 0, "object_count": 0, "stored_bytes": 0},
        "in_progress": None,
        "error": None,
    }
    if not result["initialized"]:
        return result
    try:
        uri = database.as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=1.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        result["stats"]["snapshot_count"] = connection.execute(
            "SELECT COUNT(*) FROM snapshots WHERE status='COMPLETE'"
        ).fetchone()[0]
        objects = connection.execute(
            "SELECT COUNT(*), COALESCE(SUM(stored_bytes),0) FROM objects"
        ).fetchone()
        result["stats"]["object_count"] = objects[0]
        result["stats"]["stored_bytes"] = objects[1]
        active = connection.execute(
            "SELECT id,created_ns,source_root FROM snapshots "
            "WHERE status='IN_PROGRESS' ORDER BY created_ns DESC LIMIT 1"
        ).fetchone()
        if active:
            result["in_progress"] = dict(active)
        snapshots = connection.execute(
            "SELECT s.id,s.created_ns,s.source_root,"
            "(SELECT COUNT(*) FROM entries e WHERE e.snapshot_id=s.id AND e.kind='file') AS files,"
            "(SELECT COUNT(*) FROM entries e WHERE e.snapshot_id=s.id AND e.kind='directory') AS directories,"
            "(SELECT COUNT(*) FROM entries e WHERE e.snapshot_id=s.id AND e.kind='symlink') AS symlinks,"
            "(SELECT COUNT(*) FROM entry_chunks c WHERE c.snapshot_id=s.id) AS chunks,"
            "(SELECT COALESCE(SUM(e.size_bytes),0) FROM entries e WHERE e.snapshot_id=s.id AND e.kind='file') AS logical_bytes "
            "FROM snapshots s WHERE s.status='COMPLETE' ORDER BY s.created_ns DESC LIMIT 8"
        ).fetchall()
        result["snapshots"] = [dict(row) for row in snapshots]
        connection.close()
    except (sqlite3.Error, OSError) as error:
        result["error"] = str(error)
    return result


class BackupManager:
    def __init__(self, pbe: Path):
        self.pbe = pbe
        self.lock = threading.RLock()
        self.job: dict[str, Any] | None = None

    @staticmethod
    def _repo_path(value: str) -> Path:
        if not value.strip():
            raise ValueError("請先輸入備份儲存庫路徑。")
        return Path(value).expanduser().resolve()

    def initialize(self, payload: dict[str, Any]) -> dict[str, Any]:
        repo = self._repo_path(str(payload.get("repo", "")))
        storage_format = str(payload.get("storage_format", "files-v1"))
        if storage_format not in {"files-v1"}:
            raise ValueError("不支援的儲存格式。")
        if (repo / "catalog.sqlite").exists():
            raise ValueError("這個路徑已經是 PBE 儲存庫。")
        completed = subprocess.run(
            [str(self.pbe), "init", "--repo", str(repo), "--storage-format", storage_format],
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if completed.returncode != 0:
            message = completed.stderr.strip() or completed.stdout.strip() or "無法建立儲存庫。"
            raise ValueError(message)
        return {"message": completed.stdout.strip(), "repo": str(repo), "storage_format": storage_format}

    def start(self, payload: dict[str, Any]) -> dict[str, Any]:
        repo = self._repo_path(str(payload.get("repo", "")))
        source_text = str(payload.get("source", ""))
        if not source_text.strip():
            raise ValueError("請先輸入來源資料夾路徑。")
        try:
            source = Path(source_text).expanduser().resolve(strict=True)
        except OSError as error:
            raise ValueError(f"找不到來源資料夾：{error}") from error
        if not source.is_dir():
            raise ValueError("來源路徑必須是資料夾。")
        if not (repo / "catalog.sqlite").is_file() or not (repo / "repo.toml").is_file():
            raise ValueError("儲存庫尚未初始化，請先按「初始化儲存庫」。")
        if source == repo or source.is_relative_to(repo) or repo.is_relative_to(source):
            raise ValueError("來源與儲存庫不能互相包含。")
        repository = read_repository(repo)
        if self._external_job(repo, repository.get("in_progress")):
            raise ValueError("這個儲存庫已有 CLI 備份正在執行，請等它完成後再啟動新工作。")

        try:
            workers = int(payload.get("workers", 4))
            writer_workers = int(payload.get("writer_workers", 1))
            memory_mib = int(payload.get("memory_mib", 256))
        except (TypeError, ValueError) as error:
            raise ValueError("Workers、Writer 和記憶體大小必須是整數。") from error
        scheduler = str(payload.get("scheduler", "fixed"))
        if scheduler not in {"fixed", "adaptive"}:
            raise ValueError("排程方式必須是 fixed 或 adaptive。")
        if not 1 <= workers <= 256:
            raise ValueError("Workers 必須介於 1 到 256。")
        if not 1 <= writer_workers <= 32:
            raise ValueError("Object Writer 數量必須介於 1 到 32。")
        if not 3 <= memory_mib <= 1_048_576:
            raise ValueError("記憶體額度必須至少 3 MiB。")

        args = [
            str(self.pbe), "backup", "--repo", str(repo), "--source", str(source),
            "--workers", str(workers), "--max-workers", str(workers), "--writer-workers", str(writer_workers),
            "--memory-mib", str(memory_mib), "--scheduler", scheduler,
        ]
        if payload.get("paranoid"):
            args.append("--paranoid")
        with self.lock:
            if self.job and self.job["state"] in {"running", "stopping"}:
                raise ValueError("目前已有一個備份工作正在執行。")
            reports = repo / "reports"
            previous_reports = {p.name for p in reports.glob("run-*.jsonl")} if reports.is_dir() else set()
            process = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            job = {
                "job_id": uuid.uuid4().hex,
                "state": "running",
                "repo": str(repo),
                "source": str(source),
                "scheduler": scheduler,
                "workers": workers,
                "writer_workers": writer_workers,
                "memory_mib": memory_mib,
                "started_epoch": time.time(),
                "finished_epoch": None,
                "process": process,
                "previous_reports": previous_reports,
                "trace_path": None,
                "run_id": None,
                "return_code": None,
                "error": None,
                "stdout": "",
                "summary": None,
                "can_stop": True,
            }
            self.job = job
            threading.Thread(target=self._collect, args=(job,), daemon=True).start()
        return {key: job[key] for key in ("job_id", "state", "repo", "source", "scheduler", "workers", "writer_workers", "memory_mib")}

    def _collect(self, job: dict[str, Any]) -> None:
        process: subprocess.Popen[str] = job["process"]
        stdout, stderr = process.communicate()
        now = time.time()
        run_match = RUN_ID_RE.search(stdout)
        run_id = run_match.group(1) if run_match else None
        summary: dict[str, Any] | None = None
        if run_id:
            summary_path = Path(job["repo"]) / "reports" / f"{run_id}.summary.json"
            try:
                summary = json.loads(summary_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        with self.lock:
            job["run_id"] = run_id
            job["stdout"] = stdout[-4000:]
            job["summary"] = summary
            job["return_code"] = process.returncode
            job["finished_epoch"] = now
            if process.returncode == 0:
                job["state"] = "complete"
            elif job["state"] == "stopping" or process.returncode in (-signal.SIGINT, 130):
                job["state"] = "cancelled"
            else:
                job["state"] = "failed"
                job["error"] = stderr.strip() or stdout.strip() or f"PBE exited with code {process.returncode}"

    def stop(self) -> dict[str, Any]:
        with self.lock:
            job = self.job
            if not job or job["state"] not in {"running", "stopping"}:
                raise ValueError("目前沒有可停止的備份工作。")
            if job["state"] == "running":
                job["state"] = "stopping"
                try:
                    os.killpg(job["process"].pid, signal.SIGINT)
                except ProcessLookupError:
                    pass
            return {"state": job["state"], "message": "已送出安全取消訊號，正在整理工作執行緒。"}

    def _managed_job(self, repo: Path) -> dict[str, Any] | None:
        with self.lock:
            job = self.job
            if not job:
                return None
            if job["repo"] != str(repo) and job["state"] not in {"running", "stopping"}:
                return None
            if not job["trace_path"]:
                reports = Path(job["repo"]) / "reports"
                candidates = [
                    path for path in reports.glob("run-*.jsonl")
                    if path.name not in job["previous_reports"]
                ] if reports.is_dir() else []
                if candidates:
                    job["trace_path"] = str(max(candidates, key=lambda path: path.stat().st_mtime))
            trace_path = Path(job["trace_path"]) if job.get("trace_path") else None
            data = {
                key: job.get(key)
                for key in (
                    "job_id", "state", "repo", "source", "scheduler", "workers", "memory_mib",
                    "writer_workers",
                    "started_epoch", "finished_epoch", "run_id", "error", "summary", "can_stop",
                )
            }
        metrics = parse_trace(trace_path)
        if metrics and not data.get("run_id"):
            data["run_id"] = metrics.get("run_id")
        data["metrics"] = metrics
        if data["state"] in {"running", "stopping"}:
            data["elapsed_seconds"] = max(0, time.time() - float(data["started_epoch"] or time.time()))
        else:
            data["elapsed_seconds"] = max(
                0,
                float(data["finished_epoch"] or time.time()) - float(data["started_epoch"] or time.time()),
            )
        return data

    @staticmethod
    def _repository_locked(repo: Path) -> bool:
        try:
            descriptor = os.open(repo / ".repo.lock", os.O_RDWR | os.O_CLOEXEC)
        except OSError:
            return False
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            return False
        finally:
            os.close(descriptor)

    @staticmethod
    def _external_job(repo: Path, in_progress: dict[str, Any] | None) -> dict[str, Any] | None:
        if not in_progress or not BackupManager._repository_locked(repo):
            return None
        reports = repo / "reports"
        created = int(in_progress.get("created_ns", 0)) / 1_000_000_000
        candidates = []
        if reports.is_dir():
            for path in reports.glob("run-*.jsonl"):
                try:
                    if path.stat().st_mtime >= created - 5:
                        candidates.append(path)
                except OSError:
                    continue
        trace_path = max(candidates, key=lambda path: path.stat().st_mtime) if candidates else None
        metrics = parse_trace(trace_path)
        run_id = metrics.get("run_id") if metrics else trace_path.stem if trace_path else "scanning"
        return {
            "job_id": "external-" + run_id,
            "state": "external",
            "repo": str(repo),
            "source": in_progress.get("source_root", ""),
            "scheduler": metrics["latest"].get("scheduler_mode", "unknown") if metrics else "unknown",
            "workers": None,
            "writer_workers": metrics.get("latest", {}).get("observations", {}).get("writer_workers") if metrics else None,
            "memory_mib": None,
            "started_epoch": created,
            "finished_epoch": None,
            "elapsed_seconds": max(0, time.time() - created),
            "run_id": run_id,
            "error": None,
            "summary": None,
            "can_stop": False,
            "metrics": metrics,
        }

    def status(self, repo_text: str) -> dict[str, Any]:
        try:
            repo = self._repo_path(repo_text) if repo_text.strip() else Path(".").resolve()
            repository = read_repository(repo)
            job = self._managed_job(repo)
            if not job:
                external = self._external_job(repo, repository.get("in_progress"))
                if external:
                    job = external
            return {"repository": repository, "job": job, "server_time": time.time()}
        except OSError as error:
            return {
                "repository": {"path": repo_text, "initialized": False, "error": str(error), "snapshots": [],
                               "stats": {"snapshot_count": 0, "object_count": 0, "stored_bytes": 0}},
                "job": None,
                "server_time": time.time(),
            }


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "PBE-Dashboard/1.0"

    def __init__(self, *args: Any, manager: BackupManager, **kwargs: Any):
        self.manager = manager
        super().__init__(*args, **kwargs)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[dashboard] {self.address_string()} {fmt % args}")

    def _headers(self, content_type: str, length: int) -> None:
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(length))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'")

    def _json(self, status: int, value: Any) -> None:
        body = json_bytes(value)
        self.send_response(status)
        self._headers("application/json; charset=utf-8", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as error:
            raise ValueError("Invalid request body.") from error
        if length < 0 or length > 16_384:
            raise ValueError("Request body is too large.")
        try:
            value = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as error:
            raise ValueError("Request body must be valid JSON.") from error
        if not isinstance(value, dict):
            raise ValueError("Request body must be a JSON object.")
        return value

    def _check_origin(self) -> None:
        origin = self.headers.get("Origin")
        if not origin:
            return
        parsed = urllib.parse.urlparse(origin)
        try:
            valid_origin = parsed.hostname in LOCAL_HOSTS and parsed.port == self.server.server_port
        except ValueError:
            valid_origin = False
        if not valid_origin:
            raise PermissionError("Cross-origin requests are not allowed.")

    def _check_host(self) -> None:
        host = self.headers.get("Host", "")
        parsed = urllib.parse.urlsplit("//" + host)
        try:
            valid_host = parsed.hostname in LOCAL_HOSTS and parsed.port == self.server.server_port
        except ValueError:
            valid_host = False
        if not valid_host:
            raise PermissionError("Requests must use the local dashboard address.")

    def do_GET(self) -> None:
        try:
            self._check_host()
        except PermissionError as error:
            self._json(403, {"error": str(error)})
            return
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/status":
            query = urllib.parse.parse_qs(parsed.query)
            repo = query.get("repo", [""])[0]
            self._json(200, self.manager.status(repo))
            return
        asset_names = {"/": "index.html", "/app.css": "app.css", "/app.js": "app.js"}
        asset = asset_names.get(parsed.path)
        if not asset:
            self._json(404, {"error": "Not found."})
            return
        file_path = STATIC / asset
        try:
            body = file_path.read_bytes()
        except OSError:
            self._json(404, {"error": "Dashboard asset is missing."})
            return
        content_type = "text/html; charset=utf-8" if asset.endswith(".html") else (
            "text/css; charset=utf-8" if asset.endswith(".css") else "text/javascript; charset=utf-8"
        )
        self.send_response(200)
        self._headers(content_type, len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        try:
            self._check_host()
            self._check_origin()
            payload = self._body()
            if self.path == "/api/start":
                self._json(202, {"job": self.manager.start(payload)})
            elif self.path == "/api/stop":
                self._json(200, self.manager.stop())
            elif self.path == "/api/init":
                self._json(201, self.manager.initialize(payload))
            else:
                self._json(404, {"error": "Not found."})
        except PermissionError as error:
            self._json(403, {"error": str(error)})
        except ValueError as error:
            self._json(400, {"error": str(error)})
        except subprocess.TimeoutExpired:
            self._json(504, {"error": "Repository initialization timed out."})
        except OSError as error:
            self._json(500, {"error": str(error)})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pbe", type=Path, help="PBE CLI executable (defaults to build/pbe or build-release/pbe)")
    parser.add_argument("--host", default="127.0.0.1", help="Loopback address to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765, help="Local dashboard port (default: 8765)")
    args = parser.parse_args()
    if args.host not in LOCAL_HOSTS:
        parser.error("the dashboard only binds to localhost addresses")
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    if args.pbe:
        args.pbe = args.pbe.expanduser().resolve()
    else:
        candidates = [ROOT / "build" / "pbe", ROOT / "build-release" / "pbe"]
        args.pbe = next((candidate for candidate in candidates if candidate.is_file()), candidates[0])
    if not args.pbe.is_file() or not os.access(args.pbe, os.X_OK):
        parser.error(f"PBE executable is missing or not executable: {args.pbe}; build the project or pass --pbe")
    return args


def main() -> None:
    args = parse_args()
    manager = BackupManager(args.pbe)
    handler = lambda *handler_args, **handler_kwargs: DashboardHandler(  # noqa: E731
        *handler_args, manager=manager, **handler_kwargs
    )
    server = ThreadingHTTPServer((args.host, args.port), handler)
    server.daemon_threads = True
    print(f"PBE backup dashboard: http://{args.host}:{args.port}")
    print(f"PBE executable: {args.pbe}")
    print("Press Ctrl-C to stop the dashboard server.")
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        print("\nStopping dashboard server.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
