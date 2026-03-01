"""Producer for unbounded queue: writes 1..100 and sentinel -1. put() never blocks."""
import hazelcast

QUEUE_NAME = "demo_queue_unbounded"

if __name__ == "__main__":
    client = hazelcast.HazelcastClient(cluster_name="dev")
    q = client.get_queue(QUEUE_NAME).blocking()

    for i in range(1, 101):
        q.put(i)
        print(f"Produced: {i}")
    q.put(-1)
    print("Producer finished (sentinel -1)")
    client.shutdown()
