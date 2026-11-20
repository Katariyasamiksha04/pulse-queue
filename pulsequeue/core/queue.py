"""
Thread-safe Dual-Heap Priority Task Queue.
Uses two decoupled min-heaps:
1. _ready_heap: Sorted by (priority, created_at) for tasks immediately ready for execution.
2. _delayed_heap: Sorted by (run_at, priority) for future scheduled tasks.
This architecture prevents future tasks from head-of-line blocking higher-priority immediate tasks.
"""

from __future__ import annotations
import heapq
import threading
import time
from typing import Dict, List, Optional
from pulsequeue.core.task import Task, TaskStatus


class PriorityTaskQueue:
    def __init__(self, maxsize: int = 0):
        self.maxsize = maxsize
        self._ready_heap: List[Task] = []
        self._delayed_heap: List[tuple[float, Task]] = []
        self._index: Dict[str, Task] = {}
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

    def _promote_delayed_tasks(self, now: float) -> None:
        """Promote any delayed tasks whose run_at <= now into the ready heap."""
        while self._delayed_heap and self._delayed_heap[0][0] <= now:
            _, task = heapq.heappop(self._delayed_heap)
            if task.task_id in self._index:
                heapq.heappush(self._ready_heap, task)

    def put(self, task: Task, block: bool = True, timeout: Optional[float] = None) -> bool:
        """Enqueue a task thread-safely."""
        with self._not_full:
            if self.maxsize > 0:
                if not block:
                    if len(self._index) >= self.maxsize:
                        return False
                else:
                    end_time = time.time() + timeout if timeout is not None else None
                    while len(self._index) >= self.maxsize:
                        if timeout is not None:
                            remaining = end_time - time.time()
                            if remaining <= 0:
                                return False
                            self._not_full.wait(remaining)
                        else:
                            self._not_full.wait()

            now = time.time()
            self._index[task.task_id] = task

            if task.run_at <= now:
                heapq.heappush(self._ready_heap, task)
            else:
                heapq.heappush(self._delayed_heap, (task.run_at, task))

            self._not_empty.notify_all()
            return True

    def get(self, block: bool = True, timeout: Optional[float] = None) -> Optional[Task]:
        """
        Dequeue the highest-priority task ready to run.
        """
        with self._not_empty:
            end_time = time.time() + timeout if timeout is not None else None

            while True:
                now = time.time()
                self._promote_delayed_tasks(now)

                # Purge any cancelled tasks from the top of ready_heap
                while self._ready_heap and self._ready_heap[0].task_id not in self._index:
                    heapq.heappop(self._ready_heap)

                if self._ready_heap:
                    task = heapq.heappop(self._ready_heap)
                    self._index.pop(task.task_id, None)
                    with self._not_full:
                        self._not_full.notify()
                    return task

                if not block:
                    return None

                # Determine next wakeup time based on earliest delayed task
                if timeout is not None:
                    remaining = end_time - now
                    if remaining <= 0:
                        return None
                    wait_time = remaining
                    if self._delayed_heap:
                        earliest_delayed = self._delayed_heap[0][0] - now
                        wait_time = min(wait_time, max(0.01, earliest_delayed))
                else:
                    wait_time = None
                    if self._delayed_heap:
                        earliest_delayed = self._delayed_heap[0][0] - now
                        wait_time = max(0.01, earliest_delayed)

                self._not_empty.wait(wait_time)

    def peek(self) -> Optional[Task]:
        """Inspect the highest-priority ready task without removing it."""
        with self._lock:
            now = time.time()
            self._promote_delayed_tasks(now)
            while self._ready_heap:
                top = self._ready_heap[0]
                if top.task_id in self._index:
                    return top
                heapq.heappop(self._ready_heap)
            return None

    def cancel(self, task_id: str) -> bool:
        """Cancel a pending task by ID."""
        with self._lock:
            if task_id in self._index:
                task = self._index.pop(task_id)
                task.status = TaskStatus.CANCELLED
                return True
            return False

    def size(self) -> int:
        with self._lock:
            return len(self._index)

    def is_empty(self) -> bool:
        with self._lock:
            return len(self._index) == 0

    def get_all(self) -> List[Task]:
        with self._lock:
            return list(self._index.values())
