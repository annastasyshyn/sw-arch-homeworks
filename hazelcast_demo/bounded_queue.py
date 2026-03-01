"""Bounded queue demo: one writer and two readers in parallel (demo_queue, max 10)."""
from hazelcast import HazelcastClient
import threading

QUEUE_NAME = "demo_queue"
CLUSTER_NAME = "dev"


def writer(client: HazelcastClient, queue_name: str) -> None:
    my_queue = client.get_queue(queue_name).blocking()
    for i in range(1, 101):
        my_queue.put(i)
        print(f"Writing {i}")
    my_queue.put(-1)  # poison pill
    print("Writer finished (sentinel -1)")


def reader(client: HazelcastClient, queue_name: str, reader_id: str) -> None:
    my_queue = client.get_queue(queue_name).blocking()
    while True:
        value = my_queue.take()
        if value == -1:
            my_queue.put(-1)  # poison pill for other reader
            break
        print(f"Reader {reader_id} read value: {value}")
    print(f"Reader {reader_id} finished")


def run_clients() -> None:
    client1 = HazelcastClient(cluster_name=CLUSTER_NAME)
    client2 = HazelcastClient(cluster_name=CLUSTER_NAME)
    client3 = HazelcastClient(cluster_name=CLUSTER_NAME)

    writer_thread = threading.Thread(target=writer, args=(client1, QUEUE_NAME))
    reader_thread1 = threading.Thread(target=reader, args=(client2, QUEUE_NAME, "1"))
    reader_thread2 = threading.Thread(target=reader, args=(client3, QUEUE_NAME, "2"))

    writer_thread.start()
    reader_thread1.start()
    reader_thread2.start()

    writer_thread.join()
    reader_thread1.join()
    reader_thread2.join()

    client1.shutdown()
    client2.shutdown()
    client3.shutdown()
    print("Bounded queue demo done.")


if __name__ == "__main__":
    run_clients()
