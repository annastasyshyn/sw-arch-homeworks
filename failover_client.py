from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime

import httpx


RESET = "\033[0m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Facade failover demo client")
    parser.add_argument(
        "--base-url", default="http://localhost:8000", help="Facade base URL"
    )
    parser.add_argument(
        "--mode",
        choices=["transaction", "get-user", "get-accounts"],
        default="transaction",
        help="Which endpoint to hit each tick",
    )
    parser.add_argument("--user-id", default="u1", help="User id to use")
    parser.add_argument("--amount", type=int, default=1, help="Transaction amount")
    parser.add_argument(
        "--workers", type=int, default=1, help="Number of parallel workers"
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.3,
        help="Delay between requests per worker (seconds)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="Stop after N seconds (0 = run until Ctrl+C)",
    )
    parser.add_argument(
        "--timeout", type=float, default=10.0, help="Per-request timeout (seconds)"
    )
    parser.add_argument(
        "--print-services-every",
        type=float,
        default=0.0,
        help="Also poll /services every N seconds and print ready/not_ready counts",
    )
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI colors")
    return parser.parse_args()


@dataclass
class Stats:
    ok: int = 0
    err: int = 0
    started_at: float = field(default_factory=time.perf_counter)
    last_error: str = ""

    @property
    def total(self) -> int:
        return self.ok + self.err

    @property
    def success_rate(self) -> float:
        return (self.ok / self.total * 100.0) if self.total else 0.0


def color(text: str, code: str, enabled: bool) -> str:
    return f"{code}{text}{RESET}" if enabled else text


def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


async def do_request(
    client: httpx.AsyncClient, args: argparse.Namespace
) -> tuple[int, str]:
    try:
        if args.mode == "transaction":
            r = await client.post(
                f"{args.base_url}/transaction",
                json={"user_id": args.user_id, "amount": args.amount},
                timeout=args.timeout,
            )
            r.raise_for_status()
            data = r.json()
            return r.status_code, f"tx={data.get('transaction_id', '?')[:8]}"
        elif args.mode == "get-user":
            r = await client.get(
                f"{args.base_url}/user/{args.user_id}", timeout=args.timeout
            )
            r.raise_for_status()
            data = r.json()
            return r.status_code, (
                f"balance={data.get('balance')} "
                f"txs={len(data.get('transactions', []))}"
            )
        else:
            r = await client.get(f"{args.base_url}/accounts", timeout=args.timeout)
            r.raise_for_status()
            data = r.json()
            balances = data.get("balances")
            n = len(balances) if isinstance(balances, dict) else 0
            return r.status_code, f"accounts={n}"
    except httpx.HTTPStatusError as exc:
        return (
            exc.response.status_code,
            f"HTTP {exc.response.status_code}: {exc.response.text[:120]}",
        )
    except (httpx.HTTPError, OSError) as exc:
        return -1, f"{type(exc).__name__}: {exc}"


async def worker(
    worker_id: int,
    client: httpx.AsyncClient,
    args: argparse.Namespace,
    stats: Stats,
    stop_event: asyncio.Event,
    use_color: bool,
) -> None:
    while not stop_event.is_set():
        started = time.perf_counter()
        status, summary = await do_request(client, args)
        elapsed_ms = (time.perf_counter() - started) * 1000.0

        if status == 200:
            stats.ok += 1
            tag = color(f"OK  {status}", GREEN, use_color)
        else:
            stats.err += 1
            stats.last_error = summary
            tag = color(f"ERR {status if status > 0 else 'NET'}", RED, use_color)

        line = f"{now_str()} w{worker_id} {tag} {elapsed_ms:6.1f}ms  {summary}"
        print(line, flush=True)

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=args.interval)
        except asyncio.TimeoutError:
            pass


async def services_poller(
    client: httpx.AsyncClient,
    args: argparse.Namespace,
    stop_event: asyncio.Event,
    use_color: bool,
) -> None:
    while not stop_event.is_set():
        try:
            r = await client.get(f"{args.base_url}/services", timeout=args.timeout)
            r.raise_for_status()
            data = r.json().get("services", {})
            parts = []
            for name, info in data.items():
                ready = info.get("ready_count", 0)
                not_ready = info.get("not_ready_count", 0)
                short = name.replace("-service", "")
                if not_ready:
                    parts.append(
                        color(f"{short}={ready}+{not_ready}!", YELLOW, use_color)
                    )
                else:
                    parts.append(f"{short}={ready}")
            print(
                f"{now_str()} {color('SVC', CYAN, use_color)} {' '.join(parts)}",
                flush=True,
            )
        except (httpx.HTTPError, OSError) as exc:
            print(
                f"{now_str()} {color('SVC ERR', RED, use_color)} {exc}",
                flush=True,
            )

        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=args.print_services_every
            )
        except asyncio.TimeoutError:
            pass


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    def request_stop(*_: object) -> None:
        if not stop_event.is_set():
            print("\nStopping (Ctrl+C)...", file=sys.stderr, flush=True)
            stop_event.set()

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, request_stop)
            except (NotImplementedError, RuntimeError):
                signal.signal(sig, request_stop)
    except RuntimeError:
        signal.signal(signal.SIGINT, request_stop)


async def main() -> None:
    args = parse_args()
    use_color = not args.no_color and sys.stdout.isatty()

    stats = Stats()
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    print(
        f"{now_str()} starting failover client mode={args.mode} "
        f"workers={args.workers} interval={args.interval}s base_url={args.base_url}",
        flush=True,
    )

    async with httpx.AsyncClient() as client:
        tasks = [
            asyncio.create_task(worker(i, client, args, stats, stop_event, use_color))
            for i in range(args.workers)
        ]
        if args.print_services_every > 0:
            tasks.append(
                asyncio.create_task(
                    services_poller(client, args, stop_event, use_color)
                )
            )

        if args.duration > 0:
            tasks.append(asyncio.create_task(_stop_after(stop_event, args.duration)))

        try:
            await stop_event.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    elapsed = time.perf_counter() - stats.started_at
    rps = stats.total / elapsed if elapsed > 0 else 0.0
    summary = {
        "elapsed_sec": round(elapsed, 2),
        "total": stats.total,
        "ok": stats.ok,
        "err": stats.err,
        "success_rate_pct": round(stats.success_rate, 2),
        "rps": round(rps, 2),
        "last_error": stats.last_error,
    }
    print(f"\n{now_str()} SUMMARY {json.dumps(summary)}", flush=True)


async def _stop_after(stop_event: asyncio.Event, duration: float) -> None:
    try:
        await asyncio.sleep(duration)
    except asyncio.CancelledError:
        return
    stop_event.set()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
