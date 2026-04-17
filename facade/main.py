import asyncio
import json
import os
import random
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List

import hazelcast
import httpx
from fastapi import FastAPI
from pydantic import BaseModel

_DEFAULT_LOGGING_URLS = (
    "http://logging-service-1:8000,"
    "http://logging-service-2:8000,"
    "http://logging-service-3:8000"
)


def _logging_urls_from_env() -> List[str]:
    raw = os.getenv("LOGGING_URLS", _DEFAULT_LOGGING_URLS)
    parsed = [u.strip().rstrip("/") for u in raw.split(",") if u.strip()]
    if parsed:
        return parsed
    return [u.strip().rstrip("/") for u in _DEFAULT_LOGGING_URLS.split(",") if u.strip()]


CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "").rstrip("/")
LOGGING_SERVICE_NAME = os.getenv("LOGGING_SERVICE_NAME", "logging-service")
COUNTER_SERVICE_NAME = os.getenv("COUNTER_SERVICE_NAME", "counter-service")
COUNTER_HTTP_TIMEOUT = float(os.getenv("COUNTER_HTTP_TIMEOUT", "3.0"))
HAZELCAST_MEMBERS = [
    m.strip()
    for m in os.getenv(
        "HAZELCAST_MEMBERS", "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
    ).split(",")
    if m.strip()
]
HZ_CLUSTER_NAME = os.getenv("HZ_CLUSTER_NAME", "dev")
HZ_QUEUE_NAME = os.getenv("HZ_QUEUE_NAME", "counter-updates")

_hz_lock = threading.Lock()
_hz_client: hazelcast.HazelcastClient | None = None


def _shutdown_hz_client() -> None:
    global _hz_client
    with _hz_lock:
        if _hz_client is not None:
            try:
                _hz_client.shutdown()
            finally:
                _hz_client = None


def _enqueue_counter_message(payload: Dict[str, Any]) -> None:
    serialized = json.dumps(payload)
    global _hz_client
    with _hz_lock:
        if _hz_client is None:
            _hz_client = hazelcast.HazelcastClient(
                cluster_members=HAZELCAST_MEMBERS,
                cluster_name=HZ_CLUSTER_NAME,
            )
        queue = _hz_client.get_queue(HZ_QUEUE_NAME).blocking()
        queue.put(serialized)
    print(f"[facade] enqueued counter update transaction_id={payload.get('transaction_id')!r}")


class TransactionIn(BaseModel):
    user_id: str
    amount: int


class TransactionOut(BaseModel):
    transaction_id: str
    balance: int | None = None


class UserSummary(BaseModel):
    balance: int | None = None
    transactions: List[Dict[str, Any]]


class Metrics:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self.logging_total_seconds = 0.0
        self.logging_calls = 0
        self.counter_total_seconds = 0.0
        self.counter_calls = 0

    async def add_logging(self, seconds: float) -> None:
        async with self._lock:
            self.logging_total_seconds += seconds
            self.logging_calls += 1

    async def add_counter(self, seconds: float) -> None:
        async with self._lock:
            self.counter_total_seconds += seconds
            self.counter_calls += 1

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "logging_calls": self.logging_calls,
                "logging_total_seconds": self.logging_total_seconds,
                "logging_avg_ms": (
                    (self.logging_total_seconds / self.logging_calls * 1000)
                    if self.logging_calls
                    else 0.0
                ),
                "counter_calls": self.counter_calls,
                "counter_total_seconds": self.counter_total_seconds,
                "counter_avg_ms": (
                    (self.counter_total_seconds / self.counter_calls * 1000)
                    if self.counter_calls
                    else 0.0
                ),
            }

    async def reset(self) -> None:
        async with self._lock:
            self.logging_total_seconds = 0.0
            self.logging_calls = 0
            self.counter_total_seconds = 0.0
            self.counter_calls = 0


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.metrics = Metrics()
    app.state.client = httpx.AsyncClient()
    try:
        yield
    finally:
        await app.state.client.aclose()
        await asyncio.to_thread(_shutdown_hz_client)


app = FastAPI(lifespan=lifespan)


def _shuffled_urls(urls: List[str]) -> List[str]:
    out = list(urls)
    random.shuffle(out)
    return out


async def request_json(
    method: str, url: str, payload: Dict[str, Any] | None = None, timeout: float = 10.0
) -> httpx.Response:
    client: httpx.AsyncClient = app.state.client
    return await client.request(method, url, json=payload, timeout=timeout)


async def _instances_from_config(service_name: str) -> List[str]:
    if not CONFIG_SERVER_URL:
        return []
    client: httpx.AsyncClient = app.state.client
    try:
        resp = await client.get(
            f"{CONFIG_SERVER_URL}/instances/{service_name}",
            timeout=5.0,
        )
        if not resp.is_success:
            return []
        data = resp.json().get("instances") or []
        return [str(u).rstrip("/") for u in data if str(u).strip()]
    except Exception as e:
        print(f"[facade] config-server lookup {service_name!r} failed: {e}")
        return []


async def logging_instance_urls() -> List[str]:
    urls = await _instances_from_config(LOGGING_SERVICE_NAME)
    if urls:
        return urls
    return _logging_urls_from_env()


async def counter_instance_urls() -> List[str]:
    urls = await _instances_from_config(COUNTER_SERVICE_NAME)
    if urls:
        return urls
    u = os.getenv("COUNTER_URL", "").strip().rstrip("/")
    return [u] if u else []


async def logging_request(
    method: str,
    path: str,
    payload: Dict[str, Any] | None = None,
) -> httpx.Response:
    urls = await logging_instance_urls()
    if not urls:
        raise ValueError("No logging-service instances (config-server empty and no LOGGING_URLS)")
    ordered = _shuffled_urls(urls)
    last_error: Exception | None = None
    for base in ordered:
        url = f"{base.rstrip('/')}{path}"
        try:
            resp = await request_json(method, url, payload)
            if resp.is_success:
                return resp
            last_error = httpx.HTTPStatusError(
                f"HTTP {resp.status_code}", request=resp.request, response=resp
            )
        except Exception as e:
            last_error = e
            continue
    if last_error:
        raise last_error
    raise RuntimeError("No logging service available")


async def counter_get_json(path: str) -> Dict[str, Any] | None:
    urls = await counter_instance_urls()
    if not urls:
        return None
    client: httpx.AsyncClient = app.state.client
    metrics: Metrics = app.state.metrics
    ordered = _shuffled_urls(urls)
    last_error: Exception | None = None
    for base in ordered:
        url = f"{base.rstrip('/')}{path}"
        start = time.perf_counter()
        try:
            resp = await client.get(url, timeout=COUNTER_HTTP_TIMEOUT)
            await metrics.add_counter(time.perf_counter() - start)
            if resp.is_success:
                return resp.json()
            last_error = httpx.HTTPStatusError(
                f"HTTP {resp.status_code}", request=resp.request, response=resp
            )
        except Exception as e:
            await metrics.add_counter(time.perf_counter() - start)
            last_error = e
            continue
    if last_error:
        print(f"[facade] all counter instances failed for {path}: {last_error}")
    return None


@app.post("/transaction", response_model=TransactionOut)
async def create_transaction(payload: TransactionIn) -> TransactionOut:
    transaction_id = str(uuid.uuid4())
    timestamp = time.time()
    message = {
        "transaction_id": transaction_id,
        "user_id": payload.user_id,
        "amount": payload.amount,
        "timestamp": timestamp,
    }

    metrics: Metrics = app.state.metrics

    start = time.perf_counter()
    logging_resp = await logging_request("POST", "/log", message)
    await metrics.add_logging(time.perf_counter() - start)
    logging_resp.raise_for_status()

    q_payload = {
        "user_id": payload.user_id,
        "amount": payload.amount,
        "transaction_id": transaction_id,
    }
    await asyncio.to_thread(_enqueue_counter_message, q_payload)

    return TransactionOut(transaction_id=transaction_id, balance=None)


@app.get("/user/{user_id}", response_model=UserSummary)
async def get_user_summary(user_id: str) -> UserSummary:
    metrics: Metrics = app.state.metrics

    balance_data = await counter_get_json(f"/balance/{user_id}")
    if balance_data is None:
        balance: int | None = None
    else:
        balance = int(balance_data.get("balance", 0))

    start = time.perf_counter()
    logs_resp = await logging_request("GET", "/logs")
    await metrics.add_logging(time.perf_counter() - start)

    logs_resp.raise_for_status()

    logs = logs_resp.json().get("logs", {})
    transactions = [msg for msg in logs.values() if msg.get("user_id") == user_id]

    return UserSummary(balance=balance, transactions=transactions)


@app.get("/accounts")
async def get_accounts() -> Dict[str, Any]:
    data = await counter_get_json("/balances")
    if data is None:
        return {"balances": None}
    return {"balances": data.get("balances", {})}


@app.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    metrics: Metrics = app.state.metrics
    return await metrics.snapshot()


@app.post("/metrics/reset")
async def reset_metrics() -> Dict[str, str]:
    metrics: Metrics = app.state.metrics
    await metrics.reset()
    return {"status": "ok"}
