import argparse
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

import requests


def worker(base_url: str, user_id: str, requests_per_client: int) -> None:
    payload = {"user_id": user_id, "amount": 1}
    with requests.Session() as session:
        for _ in range(requests_per_client):
            resp = session.post(f"{base_url}/transaction", json=payload, timeout=30)
            resp.raise_for_status()


def run_scenario(
    base_url: str,
    users: List[str],
    requests_per_client: int,
    max_workers: int,
) -> float:
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(worker, base_url, user_id, requests_per_client)
            for user_id in users
        ]
        for future in as_completed(futures):
            future.result()
    end = time.perf_counter()
    return end - start


def main() -> None:
    parser = argparse.ArgumentParser(description="Load test the facade service")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--scenario", choices=["independent", "conflict"], required=True
    )
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--requests", type=int, default=10000)
    parser.add_argument("--max-connections", type=int, default=200)
    args = parser.parse_args()

    if args.scenario == "independent":
        users = [f"User{idx + 1}" for idx in range(args.clients)]
        expected_total = args.requests
    else:
        users = ["User1" for _ in range(args.clients)]
        expected_total = args.requests * args.clients

    duration = run_scenario(
        base_url=args.base_url,
        users=users,
        requests_per_client=args.requests,
        max_workers=args.max_connections,
    )

    total_requests = args.requests * args.clients
    throughput = total_requests / duration if duration > 0 else 0.0

    print(f"Scenario: {args.scenario}")
    print(f"Total requests: {total_requests}")
    print(f"Total time (s): {duration:.2f}")
    print(f"Throughput (req/s): {throughput:.2f}")

    if args.scenario == "independent":
        resp = requests.get(f"{args.base_url}/accounts", timeout=30)
        resp.raise_for_status()
        balances = resp.json().get("balances", {})
        ok = all(balances.get(user, 0) == expected_total for user in users)
        print(f"Balances OK: {ok}")
    else:
        resp = requests.get(f"{args.base_url}/user/User1", timeout=30)
        resp.raise_for_status()
        balance = resp.json().get("balance", 0)
        print(f"User1 balance: {balance} (expected {expected_total})")


if __name__ == "__main__":
    main()
