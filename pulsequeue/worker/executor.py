"""
Task executor with dynamic handler registry and execution time tracking.
"""

from __future__ import annotations
import time
import traceback
from typing import Any, Callable, Dict
from pulsequeue.core.task import Task, TaskResult, TaskStatus


class TaskExecutor:
    """Dispatches tasks to registered handler functions and collects execution telemetry."""

    def __init__(self):
        self._handlers: Dict[str, Callable[..., Any]] = {}

    def register_handler(self, task_name: str, fn: Callable[..., Any]) -> None:
        """Register a callable worker function for a given task name."""
        self._handlers[task_name] = fn

    def execute(self, task: Task) -> TaskResult:
        """Execute the assigned handler with task payload and return structured TaskResult."""
        start_time = time.perf_counter()
        handler = self._handlers.get(task.name)

        if handler is None:
            err = f"No handler registered for task type: '{task.name}'"
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error=err,
                execution_time_ms=(time.perf_counter() - start_time) * 1000,
                attempts=task.current_attempt,
            )

        try:
            # Pass payload dictionary as keyword arguments or single payload
            res = handler(**task.payload) if isinstance(task.payload, dict) else handler(task.payload)
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                result=res,
                execution_time_ms=elapsed_ms,
                attempts=task.current_attempt,
                completed_at=time.time(),
            )
        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            err_msg = f"{type(exc).__name__}: {str(exc)}\n{traceback.format_exc()}"
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                error=err_msg,
                execution_time_ms=elapsed_ms,
                attempts=task.current_attempt,
                completed_at=time.time(),
            )
