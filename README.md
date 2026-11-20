# PulseQueue: Distributed Task Queue & Job Scheduler

PulseQueue is a lightweight, concurrent background task processing engine and job scheduler designed in Python. It provides reliable asynchronous task execution, priority-driven scheduling, dead-letter recovery, and persistent state management with zero external dependencies.

Developed by **Samiksha Katariya** as an exploration into distributed systems resilience and concurrency patterns.

---

## 🏛️ System Architecture

PulseQueue is built around a decoupled producer-consumer architecture designed to eliminate common concurrency bottlenecks:

```
                  +----------------------------------------------+
                  |               Client / API                   |
                  +----------------------+-----------------------+
                                         |
                                         v
            +----------------------------------------------------------+
            |                   Dual-Heap Queue                        |
            |  +------------------------+  +------------------------+  |
            |  |  Ready Priority Heap   |  |   Delayed Task Heap    |  |
            |  |  Min-Heap by Priority  |  |   Min-Heap by run_at   |  |
            |  +-----------+------------+  +-----------+------------+  |
            +--------------|---------------------------|---------------+
                           |                           | Promotion when
                           v                           | run_at <= now
            +------------------------------------------+
            |               Worker Pool                |
            |   [Worker-1]   [Worker-2]   [Worker-N]   |
            +--------------+---------------------------+
                           |
            +--------------+--------------+
            |                             |
            v                             v
     (Task Succeeded)              (Task Failed)
            |                             |
            v                       +-----+-----+
    [Mark Completed]                | Retries?  |
                                    +-----+-----+
                                     Yes /     \ No
                                        v       v
                             [Exponential      [Dead Letter Queue]
                               Jitter Wait]              |
                                    |             [Manual Replay /
                                    v              Diagnostic Inspection]
                              (Re-enqueue)
```

### Key Engineering Decisions

1. **Dual-Heap Structure for Priority & Delay**:
   - Standard priority queues suffer from head-of-line blocking when a high-priority job is scheduled in the future.
   - PulseQueue separates tasks into a `_ready_heap` (ordered by priority) and a `_delayed_heap` (ordered by execution timestamp). A condition-synchronized thread automatically promotes ready tasks without blocking immediate jobs.

2. **Exponential Backoff with Full Jitter**:
   - To mitigate thundering herd problems on downstream services, failed tasks compute retry intervals using decorrelated full jitter:
   $$\text{Delay} = \text{Uniform}(0, \min(\text{MaxDelay}, \text{BaseDelay} \times 2^{\text{attempt}-1}))$$

3. **Dead-Letter Queue (DLQ) & Poison Pill Isolation**:
   - Tasks exceeding maximum retry thresholds are safely routed to an isolated DLQ with detailed stack traces.
   - DLQ tasks can be inspected via telemetry APIs and selectively replayed once upstream dependencies recover.

4. **Crash Resilience with SQLite WAL**:
   - Write-Ahead Logging (`PRAGMA journal_mode = WAL`) guarantees ACID persistence and durability while supporting concurrent readers and writers without database locks.

---

## 🚀 Key Features

- **Priority Scheduling**: 4 distinct execution priorities (`CRITICAL`, `HIGH`, `MEDIUM`, `LOW`).
- **Delayed & Recurring Execution**: Schedule tasks for future execution or repeated cron-style intervals.
- **Worker Thread Pool**: Dynamic worker pool with graceful lifecycle handling, timeouts, and heartbeats.
- **Observability REST API**: Embedded HTTP server for health checks, queue metrics, and DLQ management.
- **Zero Third-Party Dependencies**: Runs on standard Python 3.10+ without requiring Redis, Celery, or external brokers.

---

## ⚡ Performance Benchmarks

Tested on standard consumer hardware (AMD Ryzen / Intel Core):

| Operation | Throughput | Mean Latency |
| :--- | :--- | :--- |
| **Priority Enqueue** | ~249,000 ops/sec | 0.004 ms |
| **Concurrent Worker Processing** | ~191,000 tasks/sec | 0.005 ms |

Run the benchmark suite locally:
```bash
python benchmarks/benchmark_queue.py
```

---

## 💻 Quickstart

### 1. Basic Task Dispatch

```python
from pulsequeue import PulseQueueClient, TaskPriority

client = PulseQueueClient(db_path="jobs.db", num_workers=4)

@client.task("send_notification")
def send_notification(user_id: str, message: str):
    print(f"Sending message to {user_id}: {message}")
    return {"status": "delivered"}

client.start()

# Immediate high-priority task
client.enqueue("send_notification", {"user_id": "usr_100", "message": "Welcome!"}, priority=TaskPriority.HIGH)

# Delayed task (executes after 5 seconds)
client.enqueue("send_notification", {"user_id": "usr_200", "message": "Follow up"}, delay_seconds=5.0)

client.stop()
```

### 2. Running Examples

- **Quickstart Demo**:
  ```bash
  python examples/basic_usage.py
  ```
- **Dead-Letter Queue & Replay Simulation**:
  ```bash
  python examples/dlq_recovery.py
  ```

---

## 🧪 Running Unit & Integration Tests

```bash
python -m unittest discover tests -v
```

All 12 unit and integration tests validate priority queue invariants, dual-heap scheduling, retry policies, SQLite WAL persistence, and worker pool failover.

---

## 🐳 Docker Deployment

Run with Docker Compose:
```bash
docker-compose up --build
```
The REST API will be available at `http://localhost:8080`.
- Health check: `GET http://localhost:8080/health`
- Queue telemetry: `GET http://localhost:8080/metrics`
- DLQ listing: `GET http://localhost:8080/dlq`
- Replay all dead tasks: `POST http://localhost:8080/dlq/replay-all`
