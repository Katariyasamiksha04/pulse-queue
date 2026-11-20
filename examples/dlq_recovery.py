"""
PulseQueue Dead Letter Queue (DLQ) & Poison Pill Recovery Example:
Demonstrates automatic retry backoff, routing exhausted failures to DLQ,
inspecting error reasons, and replaying recovered tasks back to the active queue.
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pulsequeue import PulseQueueClient, TaskPriority


def main():
    print("🛡️ PulseQueue Dead-Letter Queue (DLQ) & Resilience Demo\n")
    client = PulseQueueClient(db_path="dlq_demo.db", num_workers=2)

    service_healthy = False

    @client.task("sync_inventory")
    def sync_inventory(item_id: str, quantity: int):
        if not service_healthy:
            print(f"⚠️ [Worker] Downstream Warehouse API unreachable! Failing {item_id}...")
            raise ConnectionError("503 Service Unavailable: Warehouse API down")
        print(f"✅ [Worker] Inventory synced successfully: {item_id} (Qty: {quantity})")
        return {"status": "synced"}

    client.start()

    # Submit task with max 2 retries
    print("1️⃣ Submitting inventory task while downstream is DOWN...")
    task_id = client.enqueue("sync_inventory", {"item_id": "SKU-992", "quantity": 10}, max_retries=2)

    # Wait for retries to exhaust
    print("⏳ Waiting for retries to exhaust and route to Dead Letter Queue...")
    time.sleep(1.5)

    # Inspect DLQ
    dlq_size = client.dlq.size()
    print(f"\n📦 DLQ Size: {dlq_size}")
    failed_items = client.dlq.list_tasks()
    for item in failed_items:
        print(f"   Task: {item['task_id'][:8]}... | Status: {item['status']}")
        print(f"   DLQ Reason: {item['dlq_reason'].splitlines()[0]}")

    # Downstream service comes back online!
    print("\n🟢 Downstream service recovered! Initiating DLQ replay...")
    service_healthy = True

    # Replay all failed tasks from DLQ
    replayed_count = client.dlq.replay_all(client.queue)
    print(f"🔄 Replayed {replayed_count} tasks from DLQ back into active processing queue.")

    time.sleep(0.5)

    status = client.get_status(task_id)
    print(f"\n🏁 Final Task Status: {status['status']}")

    client.stop()
    for fname in ["dlq_demo.db", "dlq_demo.db-wal", "dlq_demo.db-shm"]:
        try:
            if os.path.exists(fname):
                os.remove(fname)
        except OSError:
            pass
    print("\n✅ DLQ recovery simulation complete!")


if __name__ == "__main__":
    main()
