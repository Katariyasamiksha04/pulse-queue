"""Abstract base interface for task persistence backends."""

from __future__ import annotations
import abc
from typing import Dict, List, Optional
from pulsequeue.core.task import Task, TaskStatus


class StorageBackend(abc.ABC):
    @abc.abstractmethod
    def save(self, task: Task) -> None:
        """Persist or update a task record."""
        pass

    @abc.abstractmethod
    def get(self, task_id: str) -> Optional[Task]:
        """Retrieve a task by ID."""
        pass

    @abc.abstractmethod
    def update_status(self, task_id: str, status: TaskStatus, error: Optional[str] = None) -> bool:
        """Atomically update status of a task."""
        pass

    @abc.abstractmethod
    def delete(self, task_id: str) -> bool:
        """Delete a task record."""
        pass

    @abc.abstractmethod
    def list_by_status(self, status: TaskStatus, limit: int = 100) -> List[Task]:
        """Fetch tasks filtered by status."""
        pass

    @abc.abstractmethod
    def get_metrics(self) -> Dict[str, int]:
        """Return counts of tasks by status."""
        pass

    @abc.abstractmethod
    def close(self) -> None:
        """Cleanly close database or storage connections."""
        pass
