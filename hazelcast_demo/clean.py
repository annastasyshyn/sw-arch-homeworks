"""Reset/clear the counter map to initial state."""
import hazelcast

if __name__ == "__main__":
    client = hazelcast.HazelcastClient(cluster_name="dev")
    m = client.get_map("counter_map").blocking()
    m.put("key", 0)
    print("counter_map['key'] = 0")
    client.shutdown()
