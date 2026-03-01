"""Increment counter using optimistic locking (replace_if_same)."""
import hazelcast
import threading
import time

MAP_NAME = "counter_map"

if __name__ == "__main__":
    master_client = hazelcast.HazelcastClient(cluster_name="dev")
    master_map = master_client.get_map(MAP_NAME).blocking()
    master_map.put_if_absent("key", 0)

    def increment_with_optimistic_lock(client):
        m = client.get_map(MAP_NAME).blocking()
        key = "key"
        for _ in range(10_000):
            while True:
                old_value = m.get(key)
                old_value = old_value if old_value is not None else 0
                new_value = old_value + 1
                if m.replace_if_same(key, old_value, new_value):
                    break

    clients = []
    threads = []
    start_time = time.perf_counter()
    for _ in range(3):
        client = hazelcast.HazelcastClient(cluster_name="dev")
        clients.append(client)
        thread = threading.Thread(target=increment_with_optimistic_lock, args=(client,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    elapsed = time.perf_counter() - start_time
    final_value = master_map.get("key")
    print(f"Final value for 'key': {final_value}")
    print(f"Time taken (optimistic lock): {elapsed:.3f}s")

    for client in clients:
        client.shutdown()
    master_client.shutdown()
