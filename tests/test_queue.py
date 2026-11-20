import unittest
import time
from pulsequeue.core.queue import PriorityTaskQueue
from pulsequeue.core.task import Task, TaskPriority, TaskStatus


class TestPriorityTaskQueue(unittest.TestCase):
    def setUp(self):
        self.queue = PriorityTaskQueue()

    def test_priority_ordering(self):
        """Ensure CRITICAL tasks are popped before MEDIUM and LOW tasks."""
        t_low = Task(name="low", priority=TaskPriority.LOW)
        t_med = Task(name="med", priority=TaskPriority.MEDIUM)
        t_crit = Task(name="crit", priority=TaskPriority.CRITICAL)

        self.queue.put(t_low)
        self.queue.put(t_med)
        self.queue.put(t_crit)

        popped_first = self.queue.get(block=False)
        self.assertEqual(popped_first.name, "crit")

        popped_second = self.queue.get(block=False)
        self.assertEqual(popped_second.name, "med")

        popped_third = self.queue.get(block=False)
        self.assertEqual(popped_third.name, "low")

    def test_delayed_task_does_not_fire_early(self):
        """Verify delayed tasks remain in queue until run_at timestamp."""
        future_time = time.time() + 1.0
        t_delayed = Task(name="delayed", run_at=future_time, priority=TaskPriority.CRITICAL)
        t_immediate = Task(name="immediate", run_at=time.time() - 1.0, priority=TaskPriority.LOW)

        self.queue.put(t_delayed)
        self.queue.put(t_immediate)

        # Immediate task should execute first even though its priority is lower
        task = self.queue.get(block=False)
        self.assertEqual(task.name, "immediate")

        # Now non-blocking get should return None because delayed task is not ready
        self.assertIsNone(self.queue.get(block=False))

    def test_cancellation(self):
        """Verify cancelling a task removes it and updates status."""
        task = Task(name="cancel_me")
        self.queue.put(task)
        self.assertEqual(self.queue.size(), 1)

        success = self.queue.cancel(task.task_id)
        self.assertTrue(success)
        self.assertEqual(task.status, TaskStatus.CANCELLED)
        self.assertEqual(self.queue.size(), 0)


if __name__ == "__main__":
    unittest.main()
