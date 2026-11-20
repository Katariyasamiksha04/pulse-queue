"""
Task scheduler for recurring intervals and precise future execution.
"""

from __future__ import annotations
import copy
import logging
import threading
import time
import uuid
from typing import Callable, Dict, Optional
from pulsequeue.core.queue import PriorityTaskQueue
from pulsequeue.core.task import Task, TaskPriority, TaskStatus
from pulsequeue.storage.base import StorageBackend

logger = logging.getLogger("pulsequeue.scheduler")


class TaskScheduler:
    def __init__(self, queue: PriorityTaskQueue, storage: Optional[StorageBackend] = None):
        self.queue = queue
        self.storage = storage
        self._recurring_jobs: Dict[str, Dict] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._scheduler_loop, name="PulseScheduler", daemon=True)
        self._thread.start()
        logger.info("TaskScheduler background loop started.")

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None
        logger.info("TaskScheduler stopped.")

    def schedule_delayed(
        self,
        name: str,
        payload: Dict,
        delay_seconds: float,
        priority: TaskPriority = TaskPriority.MEDIUM,
        max_retries: int = 3,
    ) -> Task:
        """Schedule a task for single execution after a given delay."""
        run_at = time.time() + max(0.0, delay_seconds)
        task = Task(
            name=name,
            payload=payload,
            priority=priority,
            run_at=run_at,
            max_retries=max_retries,
            status=TaskStatus.SCHEDULED,
        )
        if self.storage:
            self.storage.save(task)
        self.queue.put(task)
        return task

    def schedule_recurring(
        self,
        name: str,
        payload: Dict,
        interval_seconds: float,
        job_id: Optional[str] = None,
        priority: TaskPriority = TaskPriority.MEDIUM,
    ) -> str:
        """Register a repeating job executed every interval_seconds."""
        jid = job_id or f"recurring-{uuid.uuid4().hex[:8]}"
        with self._lock:
            self._recurring_jobs[jid] = {
                "name": name,
                "payload": payload,
                "interval": max(0.5, interval_seconds),
                "next_run": time.time() + interval_seconds,
                "priority": priority,
                "active": True,
            }
        return jid

    def cancel_recurring(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self._recurring_jobs:
                self._recurring_jobs[job_id]["active"] = False
                del self._recurring_jobs[job_id]
                return True
            return False

    def _scheduler_loop(self) -> None:
        while not self._stop_event.is_set():
            now = time.time()
            with self._lock:
                for jid, job in list(self._recurring_jobs.items()):
                    if not job["active"]:
                        continue
                    if now >= job["next_run"]:
                        task = Task(
                            name=job["name"],
                            payload=copy.deepcopy(job["payload"]),
                            priority=job["priority"],
                            run_at=now,
                            metadata={"job_id": jid, "recurring": True},
                        )
                        if self.storage:
                            self.storage.save(task)
                        self.queue.put(task)
                        job["next_run"] = now + job["interval"]
            time.sleep(0.2)
