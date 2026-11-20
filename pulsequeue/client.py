"""
High-level Python SDK client for PulseQueue.
"""

from __future__ import annotations
import time
from typing import Any, Callable, Dict, Optional
from pulsequeue.core.dlq import DeadLetterQueue
from pulsequeue.core.queue import PriorityTaskQueue
from pulsequeue.core.retry import ExponentialBackoffRetry, RetryPolicy
from pulsequeue.core.task import Task, TaskPriority, TaskStatus
from pulsequeue.scheduler.scheduler import TaskScheduler
from pulsequeue.storage.base import StorageBackend
from pulsequeue.storage.sqlite_wal import SQLiteWALStorage
from pulsequeue.worker.executor import TaskExecutor
from pulsequeue.worker.pool import WorkerPool


class PulseQueueClient:
    """
    Unified client interface for initializing, dispatching, and managing PulseQueue.
    """

    def __init__(
        self,
        db_path: str = "pulsequeue.db",
        num_workers: int = 4,
        retry_policy: Optional[RetryPolicy] = None,
    ):
        self.queue = PriorityTaskQueue()
        self.dlq = DeadLetterQueue()
        self.storage: StorageBackend = SQLiteWALStorage(db_path=db_path)
        self.executor = TaskExecutor()
        self.retry_policy = retry_policy or ExponentialBackoffRetry()
        self.pool = WorkerPool(
            queue=self.queue,
            executor=self.executor,
            storage=self.storage,
            dlq=self.dlq,
            retry_policy=self.retry_policy,
            num_workers=num_workers,
        )
        self.scheduler = TaskScheduler(queue=self.queue, storage=self.storage)

    def task(self, name: Optional[str] = None):
        """Decorator to register a worker task handler."""
        def decorator(fn: Callable[..., Any]):
            task_name = name or fn.__name__
            self.executor.register_handler(task_name, fn)
            return fn
        return decorator

    def enqueue(
        self,
        name: str,
        payload: Dict[str, Any],
        priority: TaskPriority = TaskPriority.MEDIUM,
        max_retries: int = 3,
        delay_seconds: float = 0.0,
    ) -> str:
        """Enqueue a task and return its task_id."""
        run_at = time.time() + max(0.0, delay_seconds)
        status = TaskStatus.SCHEDULED if delay_seconds > 0 else TaskStatus.PENDING
        task = Task(
            name=name,
            payload=payload,
            priority=priority,
            run_at=run_at,
            max_retries=max_retries,
            status=status,
        )
        self.storage.save(task)
        self.queue.put(task)
        return task.task_id

    def get_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        task = self.storage.get(task_id)
        return task.to_dict() if task else None

    def start(self) -> None:
        """Start worker threads and background scheduler."""
        self.pool.start()
        self.scheduler.start()

    def stop(self) -> None:
        """Gracefully stop scheduler, worker pool, and storage connections."""
        self.scheduler.stop()
        self.pool.stop()
        self.storage.close()
