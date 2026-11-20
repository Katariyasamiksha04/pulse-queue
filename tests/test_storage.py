import os
import tempfile
import unittest
from pulsequeue.core.task import Task, TaskPriority, TaskStatus
from pulsequeue.storage.sqlite_wal import SQLiteWALStorage


class TestSQLiteWALStorage(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.temp_dir.name, "test_pulse.db")
        self.storage = SQLiteWALStorage(db_path=self.db_path)

    def tearDown(self):
        self.storage.close()
        self.temp_dir.cleanup()

    def test_save_and_retrieve_task(self):
        task = Task(
            name="send_welcome_email",
            priority=TaskPriority.HIGH,
            payload={"user_id": 1042, "email": "dev@example.com"},
        )
        self.storage.save(task)

        retrieved = self.storage.get(task.task_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "send_welcome_email")
        self.assertEqual(retrieved.priority, TaskPriority.HIGH)
        self.assertEqual(retrieved.payload["user_id"], 1042)

    def test_update_status_and_metrics(self):
        t1 = Task(name="task_a", status=TaskStatus.PENDING)
        t2 = Task(name="task_b", status=TaskStatus.COMPLETED)
        self.storage.save(t1)
        self.storage.save(t2)

        self.storage.update_status(t1.task_id, TaskStatus.RUNNING)
        updated = self.storage.get(t1.task_id)
        self.assertEqual(updated.status, TaskStatus.RUNNING)

        metrics = self.storage.get_metrics()
        self.assertEqual(metrics["RUNNING"], 1)
        self.assertEqual(metrics["COMPLETED"], 1)


if __name__ == "__main__":
    unittest.main()
