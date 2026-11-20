"""In-memory thread-safe storage backend implementation."""

from __future__ import annotations
import threading
from typing import Dict, List, Optional
from pulsequeue.core.task import Task, TaskStatus
from pulsequeue.storage.base import StorageBackend


class MemoryStorage(StorageBackend):
    def __init__(self):
        self._store: Dict[str, Task] = {}
        self._lock = threading.RLock()

    def save(self, task: Task) -> None:
        with self._lock:
            self._store[task.task_id] = task

    def get(self, task_id: str) -> Optional[Task]:
        with self._lock:
            return self._store.get(task_id)

    def update_status(self, task_id: str, status: TaskStatus, error: Optional[str] = None) -> bool:
        with self._lock:
            task = self._store.get(task_id)
            if not task:
                return False
            task.status = status
            if error:
                task.last_error = error
            return True

    def delete(self, task_id: str) -> bool:
        with self._lock:
            return self._store.pop(task_id, None) is not None

    def list_by_status(self, status: TaskStatus, limit: int = 100) -> List[Task]:
        with self._lock:
            return [t for t in self._store.values() if t.status == status][:limit]

    def get_metrics(self) -> Dict[str, int]:
        with self._lock:
            metrics: Dict[str, int] = {s.value: 0 for s in TaskStatus}
            for t in self._store.values():
                metrics[t.status.value] = metrics.get(t.status.value, 0) + 1
            return metrics

    def close(self) -> None:
        with self._lock:
            self._store.clear()
