"""Task data structures and lifecycle states."""

from __future__ import annotations
import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


class TaskPriority(enum.IntEnum):
    """Lower numeric value indicates higher execution priority (Min-Heap ordering)."""
    CRITICAL = 0
    HIGH = 1
    MEDIUM = 2
    LOW = 3


class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    RETRYING = "RETRYING"
    DEAD_LETTER = "DEAD_LETTER"
    CANCELLED = "CANCELLED"


@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus
    result: Any = None
    error: Optional[str] = None
    execution_time_ms: float = 0.0
    attempts: int = 0
    completed_at: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "status": self.status.value,
            "result": self.result,
            "error": self.error,
            "execution_time_ms": round(self.execution_time_ms, 2),
            "attempts": self.attempts,
            "completed_at": self.completed_at,
        }


@dataclass(order=True)
class Task:
    """
    Unit of execution in PulseQueue.
    Order comparison is based on priority, scheduled run_at timestamp, and creation order.
    """
    priority: TaskPriority = field(default=TaskPriority.MEDIUM)
    run_at: float = field(default_factory=time.time)
    created_at: float = field(default_factory=time.time, compare=True)
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()), compare=False)
    name: str = field(default="default_task", compare=False)
    payload: Dict[str, Any] = field(default_factory=dict, compare=False)
    status: TaskStatus = field(default=TaskStatus.PENDING, compare=False)
    max_retries: int = field(default=3, compare=False)
    current_attempt: int = field(default=0, compare=False)
    timeout_seconds: float = field(default=30.0, compare=False)
    last_error: Optional[str] = field(default=None, compare=False)
    started_at: Optional[float] = field(default=None, compare=False)
    completed_at: Optional[float] = field(default=None, compare=False)
    metadata: Dict[str, Any] = field(default_factory=dict, compare=False)

    @property
    def is_delayed(self) -> bool:
        return self.run_at > time.time()

    @property
    def has_retries_left(self) -> bool:
        return self.current_attempt <= self.max_retries

    def record_attempt(self) -> None:
        self.current_attempt += 1
        self.started_at = time.time()
        self.status = TaskStatus.RUNNING

    def mark_completed(self) -> None:
        self.status = TaskStatus.COMPLETED
        self.completed_at = time.time()

    def mark_failed(self, error: str) -> None:
        self.last_error = error
        if self.has_retries_left:
            self.status = TaskStatus.RETRYING
        else:
            self.status = TaskStatus.DEAD_LETTER
            self.completed_at = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "name": self.name,
            "priority": self.priority.name,
            "status": self.status.value,
            "payload": self.payload,
            "run_at": self.run_at,
            "created_at": self.created_at,
            "current_attempt": self.current_attempt,
            "max_retries": self.max_retries,
            "timeout_seconds": self.timeout_seconds,
            "last_error": self.last_error,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Task:
        priority_val = data.get("priority", "MEDIUM")
        if isinstance(priority_val, str):
            priority = TaskPriority[priority_val]
        else:
            priority = TaskPriority(priority_val)

        status_val = data.get("status", "PENDING")
        status = TaskStatus(status_val)

        return cls(
            task_id=data.get("task_id", str(uuid.uuid4())),
            name=data.get("name", "task"),
            priority=priority,
            status=status,
            payload=data.get("payload", {}),
            run_at=data.get("run_at", time.time()),
            created_at=data.get("created_at", time.time()),
            current_attempt=data.get("current_attempt", 0),
            max_retries=data.get("max_retries", 3),
            timeout_seconds=data.get("timeout_seconds", 30.0),
            last_error=data.get("last_error"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            metadata=data.get("metadata", {}),
        )
