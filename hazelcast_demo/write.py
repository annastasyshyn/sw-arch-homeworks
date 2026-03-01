"""Fill the distributed map with 1000 entries."""
import hazelcast

if __name__ == "__main__":
    client = hazelcast.HazelcastClient(cluster_name="dev")
    my_map = client.get_map("my-distributed-map").blocking()

    for i in range(1000):
        my_map.put(i, f"Value-{i}")

    print("1000 entries written to the map successfully.")
    client.shutdown()
