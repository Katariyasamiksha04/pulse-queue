"""
PulseQueue Quickstart Example:
Demonstrates registering handlers, enqueueing prioritized tasks,
delayed jobs, and checking task execution status.
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pulsequeue import PulseQueueClient, TaskPriority


def main():
    print("✨ Starting PulseQueue Quickstart Demo...\n")
    client = PulseQueueClient(db_path="quickstart.db", num_workers=2)

    # 1. Register task handlers
    @client.task("send_invoice")
    def handle_invoice(customer: str, amount: float):
        print(f"📧 [Worker] Generating & sending invoice of ${amount:.2f} to {customer}...")
        time.sleep(0.1)
        return {"invoice_id": f"INV-{int(time.time())}", "status": "sent"}

    @client.task("generate_report")
    def handle_report(quarter: str):
        print(f"📊 [Worker] Compiling financial report for {quarter}...")
        time.sleep(0.15)
        return {"quarter": quarter, "pages": 14}

    # 2. Start workers & scheduler
    client.start()

    # 3. Enqueue immediate tasks with different priorities
    print("📥 Submitting tasks with CRITICAL and MEDIUM priorities...")
    task_id1 = client.enqueue("generate_report", {"quarter": "Q4-2026"}, priority=TaskPriority.MEDIUM)
    task_id2 = client.enqueue("send_invoice", {"customer": "Acme Corp", "amount": 1450.00}, priority=TaskPriority.CRITICAL)

    # 4. Schedule a delayed task (runs in 1.5 seconds)
    print("⏰ Submitting delayed task (executes in 1.5s)...")
    task_id3 = client.enqueue("send_invoice", {"customer": "Beta Ltd", "amount": 320.50}, delay_seconds=1.5)

    # Allow workers to complete tasks
    time.sleep(2.0)

    # 5. Inspect status
    print("\n🔍 Querying persisted task statuses:")
    for tid in [task_id1, task_id2, task_id3]:
        info = client.get_status(tid)
        print(f"   Task {tid[:8]}... | Status: {info['status']} | Retries: {info['current_attempt']}")

    client.stop()
    for fname in ["quickstart.db", "quickstart.db-wal", "quickstart.db-shm"]:
        try:
            if os.path.exists(fname):
                os.remove(fname)
        except OSError:
            pass
    print("\n✅ Demo finished cleanly!")


if __name__ == "__main__":
    main()
