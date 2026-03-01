import hazelcast
import sys

QUEUE_NAME = "demo_queue_unbounded"

if __name__ == "__main__":
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else "0"
    client = hazelcast.HazelcastClient(cluster_name="dev")
    q = client.get_queue(QUEUE_NAME).blocking()

    while True:
        item = q.take()
        print(f"Consumer {consumer_id} consumed: {item}")
        if item == -1:
            q.put(-1)
            break
    print(f"Consumer {consumer_id} finished")
    client.shutdown()
