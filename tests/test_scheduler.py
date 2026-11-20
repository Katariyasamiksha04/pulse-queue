import time
import unittest
from pulsequeue.core.queue import PriorityTaskQueue
from pulsequeue.scheduler.scheduler import TaskScheduler


class TestTaskScheduler(unittest.TestCase):
    def setUp(self):
        self.queue = PriorityTaskQueue()
        self.scheduler = TaskScheduler(queue=self.queue)
        self.scheduler.start()

    def tearDown(self):
        self.scheduler.stop()

    def test_delayed_task_scheduling(self):
        task = self.scheduler.schedule_delayed(
            name="delayed_alert",
            payload={"msg": "ping"},
            delay_seconds=0.2,
        )
        self.assertIsNotNone(task)
        # Should not be ready immediately
        ready_task = self.queue.get(block=False)
        self.assertIsNone(ready_task)

        # After waiting past delay, it becomes ready
        time.sleep(0.3)
        ready_task = self.queue.get(block=False)
        self.assertIsNotNone(ready_task)
        self.assertEqual(ready_task.name, "delayed_alert")


if __name__ == "__main__":
    unittest.main()
