import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

LOGGING_URL = os.getenv("LOGGING_URL", "http://logging-service:8000")
COUNTER_URL = os.getenv("COUNTER_URL", "http://counter-service:8000")


class TransactionIn(BaseModel):
    user_id: str
    amount: int


class TransactionOut(BaseModel):
    transaction_id: str
    balance: int


class UserSummary(BaseModel):
    balance: int
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


app = FastAPI(lifespan=lifespan)


async def request_json(
    method: str, url: str, payload: Dict[str, Any] | None = None
) -> httpx.Response:
    client: httpx.AsyncClient = app.state.client
    return await client.request(method, url, json=payload, timeout=10.0)


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
    logging_resp = await request_json("POST", f"{LOGGING_URL}/log", message)
    await metrics.add_logging(time.perf_counter() - start)
    logging_resp.raise_for_status()

    start = time.perf_counter()
    counter_resp = await request_json(
        "POST",
        f"{COUNTER_URL}/update",
        payload.model_dump(),
    )
    await metrics.add_counter(time.perf_counter() - start)

    counter_resp.raise_for_status()
    new_balance = counter_resp.json().get("balance", 0)

    return TransactionOut(transaction_id=transaction_id, balance=new_balance)


@app.get("/user/{user_id}", response_model=UserSummary)
async def get_user_summary(user_id: str) -> UserSummary:
    metrics: Metrics = app.state.metrics

    start = time.perf_counter()
    balance_resp = await request_json("GET", f"{COUNTER_URL}/balance/{user_id}")
    await metrics.add_counter(time.perf_counter() - start)

    start = time.perf_counter()
    logs_resp = await request_json("GET", f"{LOGGING_URL}/logs")
    await metrics.add_logging(time.perf_counter() - start)

    balance_resp.raise_for_status()
    logs_resp.raise_for_status()

    balance = balance_resp.json().get("balance", 0)
    logs = logs_resp.json().get("logs", {})

    transactions = [msg for msg in logs.values() if msg.get("user_id") == user_id]

    return UserSummary(balance=balance, transactions=transactions)


@app.get("/accounts")
async def get_accounts() -> Dict[str, Dict[str, int]]:
    metrics: Metrics = app.state.metrics

    start = time.perf_counter()
    resp = await request_json("GET", f"{COUNTER_URL}/balances")
    await metrics.add_counter(time.perf_counter() - start)
    resp.raise_for_status()
    return {"balances": resp.json().get("balances", {})}


@app.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    metrics: Metrics = app.state.metrics
    return await metrics.snapshot()


@app.post("/metrics/reset")
async def reset_metrics() -> Dict[str, str]:
    metrics: Metrics = app.state.metrics
    await metrics.reset()
    return {"status": "ok"}
