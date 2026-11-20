"""
PulseQueue: High-Performance Concurrent Distributed Task Queue & Job Scheduler.
Designed for high reliability, fault tolerance, and observable background processing.
"""

__version__ = "1.0.0"
__author__ = "Samiksha Katariya"

from pulsequeue.core.task import Task, TaskStatus, TaskPriority
from pulsequeue.core.retry import RetryPolicy, ExponentialBackoffRetry, FixedDelayRetry
from pulsequeue.core.queue import PriorityTaskQueue
from pulsequeue.core.dlq import DeadLetterQueue
from pulsequeue.worker.pool import WorkerPool
from pulsequeue.scheduler.scheduler import TaskScheduler
from pulsequeue.client import PulseQueueClient

__all__ = [
    "Task",
    "TaskStatus",
    "TaskPriority",
    "RetryPolicy",
    "ExponentialBackoffRetry",
    "FixedDelayRetry",
    "PriorityTaskQueue",
    "DeadLetterQueue",
    "WorkerPool",
    "TaskScheduler",
    "PulseQueueClient",
]
