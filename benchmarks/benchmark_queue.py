"""
Stress test and performance benchmarking script for PulseQueue.
Measures enqueue/dequeue operations per second and concurrent worker throughput.
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pulsequeue.core.queue import PriorityTaskQueue
from pulsequeue.core.task import Task, TaskPriority
from pulsequeue.worker.executor import TaskExecutor
from pulsequeue.worker.pool import WorkerPool


def run_benchmark(num_tasks: int = 10000, num_workers: int = 4):
    print("=" * 60)
    print(f"🚀 PulseQueue Performance Benchmark")
    print(f"   Tasks: {num_tasks:,} | Workers: {num_workers}")
    print("=" * 60)

    queue = PriorityTaskQueue()
    executor = TaskExecutor()

    # Enqueue benchmark
    start_enqueue = time.perf_counter()
    for i in range(num_tasks):
        p = TaskPriority(i % 4)
        task = Task(name="benchmark_task", priority=p, payload={"idx": i})
        queue.put(task)
    enqueue_duration = time.perf_counter() - start_enqueue
    enqueue_ops = num_tasks / enqueue_duration

    print(f"✅ Enqueue: {enqueue_duration:.3f}s | {enqueue_ops:,.0f} ops/sec")

    # Processing benchmark
    processed_count = 0

    def work_handler(idx: int):
        nonlocal processed_count
        processed_count += 1
        return idx * 2

    executor.register_handler("benchmark_task", work_handler)

    pool = WorkerPool(queue=queue, executor=executor, num_workers=num_workers)
    start_process = time.perf_counter()
    pool.start()

    # Wait until queue is drained
    while queue.size() > 0 or processed_count < num_tasks:
        time.sleep(0.01)

    process_duration = time.perf_counter() - start_process
    pool.stop()

    throughput = num_tasks / process_duration
    print(f"✅ Process: {process_duration:.3f}s | {throughput:,.0f} tasks/sec")
    print("=" * 60)
    print("Benchmark complete!\n")


if __name__ == "__main__":
    run_benchmark(num_tasks=5000, num_workers=4)
