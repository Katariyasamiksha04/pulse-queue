"""
Multi-threaded concurrent worker pool with graceful lifecycle handling,
exponential retry dispatch, and dead-letter queue routing.
"""

from __future__ import annotations
import logging
import threading
import time
from typing import Dict, List, Optional
from pulsequeue.core.dlq import DeadLetterQueue
from pulsequeue.core.queue import PriorityTaskQueue
from pulsequeue.core.retry import RetryPolicy, ExponentialBackoffRetry
from pulsequeue.core.task import Task, TaskResult, TaskStatus
from pulsequeue.storage.base import StorageBackend
from pulsequeue.worker.executor import TaskExecutor

logger = logging.getLogger("pulsequeue.worker")


class WorkerPool:
    def __init__(
        self,
        queue: PriorityTaskQueue,
        executor: TaskExecutor,
        storage: Optional[StorageBackend] = None,
        dlq: Optional[DeadLetterQueue] = None,
        retry_policy: Optional[RetryPolicy] = None,
        num_workers: int = 4,
    ):
        self.queue = queue
        self.executor = executor
        self.storage = storage
        self.dlq = dlq or DeadLetterQueue()
        self.retry_policy = retry_policy or ExponentialBackoffRetry()
        self.num_workers = max(1, num_workers)

        self._threads: List[threading.Thread] = []
        self._stop_event = threading.Event()
        self._active_workers: Dict[str, Optional[str]] = {}  # worker_name -> task_id
        self._stats_lock = threading.Lock()
        self._stats = {
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
            "retried": 0,
            "dead_lettered": 0,
        }

    def start(self) -> None:
        """Spawn worker threads."""
        self._stop_event.clear()
        for i in range(self.num_workers):
            name = f"PulseWorker-{i+1}"
            t = threading.Thread(target=self._worker_loop, name=name, args=(name,), daemon=True)
            self._threads.append(t)
            self._active_workers[name] = None
            t.start()
        logger.info(f"WorkerPool started with {self.num_workers} workers.")

    def _worker_loop(self, worker_name: str) -> None:
        while not self._stop_event.is_set():
            task = self.queue.get(block=True, timeout=0.05)
            if task is None:
                continue

            self._active_workers[worker_name] = task.task_id
            task.record_attempt()

            if self.storage:
                self.storage.update_status(task.task_id, TaskStatus.RUNNING)

            result: TaskResult = self.executor.execute(task)

            with self._stats_lock:
                self._stats["processed"] += 1

            if result.status == TaskStatus.COMPLETED:
                task.mark_completed()
                if self.storage:
                    self.storage.update_status(task.task_id, TaskStatus.COMPLETED)
                with self._stats_lock:
                    self._stats["succeeded"] += 1
            else:
                # Failure case
                if task.has_retries_left:
                    delay = self.retry_policy.calculate_delay(task.current_attempt)
                    task.status = TaskStatus.RETRYING
                    task.run_at = time.time() + delay
                    task.last_error = result.error

                    if self.storage:
                        self.storage.update_status(task.task_id, TaskStatus.RETRYING, error=result.error)

                    with self._stats_lock:
                        self._stats["retried"] += 1

                    # Re-enqueue with delay
                    self.queue.put(task)
                else:
                    task.mark_failed(result.error or "Unknown failure")
                    self.dlq.add(task, failure_reason=result.error or "Max retries exceeded")

                    if self.storage:
                        self.storage.update_status(task.task_id, TaskStatus.DEAD_LETTER, error=result.error)

                    with self._stats_lock:
                        self._stats["failed"] += 1
                        self._stats["dead_lettered"] += 1

            self._active_workers[worker_name] = None

    def stop(self, timeout: float = 5.0) -> None:
        """Signal all workers to stop and wait for join."""
        self._stop_event.set()
        for t in self._threads:
            t.join(timeout=timeout)
        self._threads.clear()
        logger.info("WorkerPool stopped.")

    def get_stats(self) -> Dict[str, int]:
        with self._stats_lock:
            return dict(self._stats)

    def get_active_workers(self) -> Dict[str, Optional[str]]:
        return dict(self._active_workers)
