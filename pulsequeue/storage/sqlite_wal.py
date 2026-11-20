"""
SQLite storage backend with Write-Ahead Logging (WAL) mode for ACID transactions,
high read-concurrency, and crash resilience.
"""

from __future__ import annotations
import json
import sqlite3
import threading
from typing import Dict, List, Optional
from pulsequeue.core.task import Task, TaskStatus, TaskPriority
from pulsequeue.storage.base import StorageBackend


class SQLiteWALStorage(StorageBackend):
    def __init__(self, db_path: str = "pulsequeue.db"):
        self.db_path = db_path
        self._local = threading.local()
        self._all_conns = []
        self._conn_lock = threading.Lock()
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            # Enable WAL mode for high concurrency
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 5000;")
            self._local.conn = conn
            with self._conn_lock:
                self._all_conns.append(conn)
        return self._local.conn

    def _init_db(self) -> None:
        conn = self._get_connection()
        with conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    run_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    current_attempt INTEGER NOT NULL,
                    max_retries INTEGER NOT NULL,
                    timeout_seconds REAL NOT NULL,
                    last_error TEXT,
                    started_at REAL,
                    completed_at REAL,
                    metadata TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_priority_run ON tasks(priority, run_at);")

    def save(self, task: Task) -> None:
        conn = self._get_connection()
        with conn:
            conn.execute(
                """
                INSERT INTO tasks (
                    task_id, name, priority, status, payload, run_at, created_at,
                    current_attempt, max_retries, timeout_seconds, last_error,
                    started_at, completed_at, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status = excluded.status,
                    current_attempt = excluded.current_attempt,
                    run_at = excluded.run_at,
                    last_error = excluded.last_error,
                    started_at = excluded.started_at,
                    completed_at = excluded.completed_at,
                    metadata = excluded.metadata
                """,
                (
                    task.task_id,
                    task.name,
                    int(task.priority),
                    task.status.value,
                    json.dumps(task.payload),
                    task.run_at,
                    task.created_at,
                    task.current_attempt,
                    task.max_retries,
                    task.timeout_seconds,
                    task.last_error,
                    task.started_at,
                    task.completed_at,
                    json.dumps(task.metadata),
                ),
            )

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        return Task(
            task_id=row["task_id"],
            name=row["name"],
            priority=TaskPriority(row["priority"]),
            status=TaskStatus(row["status"]),
            payload=json.loads(row["payload"]),
            run_at=row["run_at"],
            created_at=row["created_at"],
            current_attempt=row["current_attempt"],
            max_retries=row["max_retries"],
            timeout_seconds=row["timeout_seconds"],
            last_error=row["last_error"],
            started_at=row["started_at"],
            completed_at=row["completed_at"],
            metadata=json.loads(row["metadata"]),
        )

    def get(self, task_id: str) -> Optional[Task]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
        row = cur.fetchone()
        return self._row_to_task(row) if row else None

    def update_status(self, task_id: str, status: TaskStatus, error: Optional[str] = None) -> bool:
        conn = self._get_connection()
        with conn:
            cur = conn.execute(
                "UPDATE tasks SET status = ?, last_error = COALESCE(?, last_error) WHERE task_id = ?",
                (status.value, error, task_id),
            )
            return cur.rowcount > 0

    def delete(self, task_id: str) -> bool:
        conn = self._get_connection()
        with conn:
            cur = conn.execute("DELETE FROM tasks WHERE task_id = ?", (task_id,))
            return cur.rowcount > 0

    def list_by_status(self, status: TaskStatus, limit: int = 100) -> List[Task]:
        conn = self._get_connection()
        cur = conn.execute("SELECT * FROM tasks WHERE status = ? LIMIT ?", (status.value, limit))
        return [self._row_to_task(r) for r in cur.fetchall()]

    def get_metrics(self) -> Dict[str, int]:
        conn = self._get_connection()
        cur = conn.execute("SELECT status, count(*) as count FROM tasks GROUP BY status")
        counts = {r["status"]: r["count"] for r in cur.fetchall()}
        metrics = {s.value: 0 for s in TaskStatus}
        metrics.update(counts)
        return metrics

    def close(self) -> None:
        with self._conn_lock:
            for c in self._all_conns:
                try:
                    c.close()
                except Exception:
                    pass
            self._all_conns.clear()
        if hasattr(self._local, "conn"):
            self._local.conn = None
