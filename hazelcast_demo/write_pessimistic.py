import hazelcast
import threading
import time

MAP_NAME = "counter_map"

if __name__ == "__main__":
    master_client = hazelcast.HazelcastClient(cluster_name="dev")
    master_map = master_client.get_map(MAP_NAME).blocking()
    master_map.put_if_absent("key", 0)

    def increment_with_lock(client):
        m = client.get_map(MAP_NAME).blocking()
        key = "key"
        for _ in range(10_000):
            m.lock(key)
            try:
                value = m.get(key)
                value = (value or 0) + 1
                m.put(key, value)
            finally:
                m.unlock(key)

    clients = []
    threads = []
    start_time = time.perf_counter()
    for _ in range(3):
        client = hazelcast.HazelcastClient(cluster_name="dev")
        clients.append(client)
        thread = threading.Thread(target=increment_with_lock, args=(client,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    elapsed = time.perf_counter() - start_time
    final_value = master_map.get("key")
    print(f"Final value for 'key': {final_value}")
    print(f"Time taken (pessimistic lock): {elapsed:.3f}s")

    for client in clients:
        client.shutdown()
    master_client.shutdown()
