"""
Dead Letter Queue (DLQ) for isolating poison pills and permanently failed tasks.
Provides inspection, diagnostic logging, and replay mechanics.
"""

from __future__ import annotations
import threading
import time
from typing import Dict, List, Optional
from pulsequeue.core.task import Task, TaskStatus


class DeadLetterQueue:
    """Thread-safe store for tasks that exceeded maximum retry attempts."""

    def __init__(self, maxsize: int = 1000):
        self.maxsize = maxsize
        self._dlq_map: Dict[str, Task] = {}
        self._reasons: Dict[str, str] = {}
        self._timestamps: Dict[str, float] = {}
        self._lock = threading.RLock()

    def add(self, task: Task, failure_reason: str) -> None:
        with self._lock:
            # If at maxsize, evict the oldest entry
            if len(self._dlq_map) >= self.maxsize:
                oldest_id = min(self._timestamps, key=lambda k: self._timestamps[k])
                self._dlq_map.pop(oldest_id, None)
                self._reasons.pop(oldest_id, None)
                self._timestamps.pop(oldest_id, None)

            task.status = TaskStatus.DEAD_LETTER
            task.completed_at = time.time()
            self._dlq_map[task.task_id] = task
            self._reasons[task.task_id] = failure_reason
            self._timestamps[task.task_id] = time.time()

    def get(self, task_id: str) -> Optional[Task]:
        with self._lock:
            return self._dlq_map.get(task_id)

    def get_reason(self, task_id: str) -> Optional[str]:
        with self._lock:
            return self._reasons.get(task_id)

    def list_tasks(self, limit: int = 50, offset: int = 0) -> List[Dict]:
        with self._lock:
            items = list(self._dlq_map.values())[offset : offset + limit]
            return [
                {
                    **task.to_dict(),
                    "dlq_reason": self._reasons.get(task.task_id),
                    "dlq_timestamp": self._timestamps.get(task.task_id),
                }
                for task in items
            ]

    def replay(self, task_id: str, target_queue) -> bool:
        """Replay a failed task by resetting its status and injecting it back into the active queue."""
        with self._lock:
            if task_id not in self._dlq_map:
                return False

            task = self._dlq_map.pop(task_id)
            self._reasons.pop(task_id, None)
            self._timestamps.pop(task_id, None)

            task.status = TaskStatus.PENDING
            task.current_attempt = 0
            task.last_error = None
            task.run_at = time.time()
            return target_queue.put(task)

    def replay_all(self, target_queue) -> int:
        """Replay all dead-lettered tasks."""
        with self._lock:
            count = 0
            task_ids = list(self._dlq_map.keys())
            for tid in task_ids:
                if self.replay(tid, target_queue):
                    count += 1
            return count

    def purge(self) -> int:
        with self._lock:
            total = len(self._dlq_map)
            self._dlq_map.clear()
            self._reasons.clear()
            self._timestamps.clear()
            return total

    def size(self) -> int:
        with self._lock:
            return len(self._dlq_map)
