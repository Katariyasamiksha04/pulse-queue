import time
import unittest
from pulsequeue.core.dlq import DeadLetterQueue
from pulsequeue.core.queue import PriorityTaskQueue
from pulsequeue.core.retry import FixedDelayRetry
from pulsequeue.core.task import Task, TaskStatus
from pulsequeue.worker.executor import TaskExecutor
from pulsequeue.worker.pool import WorkerPool


class TestWorkerPool(unittest.TestCase):
    def setUp(self):
        self.queue = PriorityTaskQueue()
        self.executor = TaskExecutor()
        self.dlq = DeadLetterQueue()
        self.pool = WorkerPool(
            queue=self.queue,
            executor=self.executor,
            dlq=self.dlq,
            retry_policy=FixedDelayRetry(delay_seconds=0.05),
            num_workers=2,
        )

    def tearDown(self):
        self.pool.stop()

    def test_successful_task_execution(self):
        results = []

        def handle_add(a: int, b: int):
            results.append(a + b)
            return a + b

        self.executor.register_handler("add_numbers", handle_add)
        self.pool.start()

        task = Task(name="add_numbers", payload={"a": 15, "b": 27})
        self.queue.put(task)

        # Wait for processing
        time.sleep(0.3)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], 42)
        stats = self.pool.get_stats()
        self.assertEqual(stats["succeeded"], 1)

    def test_retry_and_dead_letter_queue_routing(self):
        attempts = []

        def failing_task():
            attempts.append(time.time())
            raise RuntimeError("Transient database connection failure")

        self.executor.register_handler("failing_job", failing_task)
        self.pool.start()

        task = Task(name="failing_job", max_retries=2)
        self.queue.put(task)

        # Wait for retries to exhaust
        for _ in range(20):
            if len(attempts) >= 3:
                break
            time.sleep(0.05)

        # Should have executed initial + 2 retries = 3 attempts total
        self.assertEqual(len(attempts), 3)
        self.assertEqual(self.dlq.size(), 1)

        dlq_task = self.dlq.get(task.task_id)
        self.assertIsNotNone(dlq_task)
        self.assertEqual(dlq_task.status, TaskStatus.DEAD_LETTER)


if __name__ == "__main__":
    unittest.main()
