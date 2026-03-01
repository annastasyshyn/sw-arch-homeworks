import hazelcast
import threading
import time

QUEUE_NAME = "demo_queue"

if __name__ == "__main__":
    client = hazelcast.HazelcastClient(cluster_name="dev")
    q = client.get_queue(QUEUE_NAME).blocking()
    q.clear()

    print("Queue max-size: 10. Putting 1..10...")
    for i in range(1, 11):
        q.put(i)
        print(f"  put({i}) ok, size = {q.size()}")

    print("Putting 11 (will BLOCK while queue is full and no consumer)...")
    result = {"done": False}

    def put_eleven():
        try:
            q.put(11)
            result["done"] = True
        except Exception:
            pass  # expected when main thread calls client.shutdown() while put() is blocking

    t = threading.Thread(target=put_eleven, daemon=True)
    t.start()
    time.sleep(3)
    if result["done"]:
        print("  put(11) returned — queue is likely UNBOUNDED (start Hazelcast with config: Docker mount hazelcast.yaml, or brew: hz start -c=\"$(pwd)/hazelcast_demo/config/hazelcast.yaml\" from repo root).")
    else:
        print("  After 3s put(11) still blocking — expected when queue is full and no one reads.")
    client.shutdown()
