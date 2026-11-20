"""
Lightweight REST API management server built on standard library http.server.
Allows external clients or monitoring dashboards to inspect queues, submit jobs,
and replay dead-letter messages.
"""

from __future__ import annotations
import json
import logging
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from typing import Optional
from pulsequeue.core.queue import PriorityTaskQueue
from pulsequeue.core.dlq import DeadLetterQueue
from pulsequeue.core.task import Task
from pulsequeue.storage.base import StorageBackend
from pulsequeue.worker.pool import WorkerPool

logger = logging.getLogger("pulsequeue.server")


def make_handler(
    queue: PriorityTaskQueue,
    dlq: DeadLetterQueue,
    worker_pool: WorkerPool,
    storage: Optional[StorageBackend],
):
    class PulseRequestHandler(BaseHTTPRequestHandler):
        def _send_json(self, status_code: int, data: dict) -> None:
            body = json.dumps(data, indent=2).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/")

            if path == "/health" or path == "":
                self._send_json(200, {"status": "UP", "service": "PulseQueue"})
                return

            if path == "/metrics":
                stats = worker_pool.get_stats()
                storage_metrics = storage.get_metrics() if storage else {}
                self._send_json(
                    200,
                    {
                        "queue_depth": queue.size(),
                        "dlq_depth": dlq.size(),
                        "active_workers": worker_pool.get_active_workers(),
                        "worker_stats": stats,
                        "storage_breakdown": storage_metrics,
                    },
                )
                return

            if path == "/dlq":
                items = dlq.list_tasks(limit=50)
                self._send_json(200, {"count": len(items), "tasks": items})
                return

            if path.startswith("/tasks/"):
                task_id = path.split("/tasks/")[-1]
                task = storage.get(task_id) if storage else None
                if task:
                    self._send_json(200, task.to_dict())
                else:
                    self._send_json(404, {"error": f"Task '{task_id}' not found"})
                return

            self._send_json(404, {"error": "Not Found"})

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/")

            content_len = int(self.headers.get("Content-Length", 0))
            payload = json.loads(self.rfile.read(content_len).decode("utf-8")) if content_len > 0 else {}

            if path == "/tasks":
                task = Task.from_dict(payload)
                if storage:
                    storage.save(task)
                queue.put(task)
                self._send_json(201, {"message": "Task enqueued", "task_id": task.task_id})
                return

            if path.startswith("/dlq/") and path.endswith("/replay"):
                # e.g., /dlq/<task_id>/replay
                parts = path.split("/")
                task_id = parts[2]
                replayed = dlq.replay(task_id, queue)
                if replayed:
                    self._send_json(200, {"message": f"Task {task_id} successfully replayed to queue"})
                else:
                    self._send_json(404, {"error": f"Task {task_id} not found in DLQ"})
                return

            if path == "/dlq/replay-all":
                count = dlq.replay_all(queue)
                self._send_json(200, {"message": f"Replayed {count} tasks from DLQ"})
                return

            self._send_json(404, {"error": "Endpoint not recognized"})

        def log_message(self, format, *args):
            # Suppress noisy standard request logging during tests
            pass

    return PulseRequestHandler


class PulseHTTPServer:
    def __init__(
        self,
        queue: PriorityTaskQueue,
        dlq: DeadLetterQueue,
        worker_pool: WorkerPool,
        storage: Optional[StorageBackend] = None,
        host: str = "127.0.0.1",
        port: int = 8080,
    ):
        self.host = host
        self.port = port
        handler = make_handler(queue, dlq, worker_pool, storage)
        self.server = HTTPServer((host, port), handler)
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(f"PulseQueue HTTP API running at http://{self.host}:{self.port}")

    def stop(self) -> None:
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        logger.info("PulseQueue HTTP API stopped.")
