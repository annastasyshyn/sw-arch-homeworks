import asyncio
import json
import os
import threading
from contextlib import asynccontextmanager
from typing import Any, Dict

import asyncpg
import httpx
import hazelcast
from fastapi import FastAPI
from pydantic import BaseModel

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@postgres:5432/postgres",
)
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "").rstrip("/")
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "").rstrip("/")
SERVICE_NAME = os.getenv("SERVICE_NAME", "counter-service")
HAZELCAST_MEMBERS = [
    m.strip() for m in os.getenv(
        "HAZELCAST_MEMBERS", "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
    ).split(",")
    if m.strip()
]
HZ_CLUSTER_NAME = os.getenv("HZ_CLUSTER_NAME", "dev")
QUEUE_NAME = os.getenv("HZ_QUEUE_NAME", "counter-updates")


class UpdateRequest(BaseModel):
    user_id: str
    amount: int


async def _apply_queued_item(pool: asyncpg.Pool, raw_item: Any) -> None:
    if isinstance(raw_item, bytes):
        raw_item = raw_item.decode("utf-8")
    if isinstance(raw_item, str):
        data = json.loads(raw_item)
    elif isinstance(raw_item, dict):
        data = raw_item
    else:
        raise ValueError(f"unsupported queue payload type: {type(raw_item)}")
    user_id = str(data["user_id"])
    amount = int(data["amount"])
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO accounts (user_id, balance)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE
            SET balance = accounts.balance + EXCLUDED.balance
            """,
            user_id,
            amount,
        )
    print(
        f"[counter-service] applied from queue user_id={user_id!r} amount={amount} "
        f"transaction_id={data.get('transaction_id')!r}"
    )


def _consumer_loop(
    pool: asyncpg.Pool,
    loop: asyncio.AbstractEventLoop,
    shutdown: threading.Event,
) -> None:
    print(f"[counter-service] queue consumer starting queue={QUEUE_NAME!r}")
    hz = hazelcast.HazelcastClient(
        cluster_members=HAZELCAST_MEMBERS,
        cluster_name=HZ_CLUSTER_NAME,
    )
    try:
        q = hz.get_queue(QUEUE_NAME).blocking()
        while not shutdown.is_set():
            item = q.poll(1.0)
            if item is None:
                continue
            try:
                fut = asyncio.run_coroutine_threadsafe(_apply_queued_item(pool, item), loop)
                fut.result(timeout=120)
            except Exception as e:
                print(f"[counter-service] queue item failed: {e}")
    finally:
        hz.shutdown()
        print("[counter-service] queue consumer stopped")


async def _register_with_config() -> None:
    if not CONFIG_SERVER_URL or not SERVICE_BASE_URL:
        print("[counter-service] skipping config registration (missing env)")
        return
    url = f"{CONFIG_SERVER_URL}/register"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                url,
                json={"service": SERVICE_NAME, "url": SERVICE_BASE_URL},
                timeout=10.0,
            )
            resp.raise_for_status()
        print(f"[counter-service] registered at config-server as {SERVICE_BASE_URL!r}")
    except Exception as e:
        print(f"[counter-service] config registration failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=10,
        command_timeout=60,
    )
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                user_id TEXT PRIMARY KEY,
                balance BIGINT NOT NULL DEFAULT 0
            )
            """
        )
    app.state.pool = pool
    shutdown = threading.Event()
    loop = asyncio.get_running_loop()
    consumer = threading.Thread(
        target=_consumer_loop,
        args=(pool, loop, shutdown),
        name="hz-queue-consumer",
        daemon=True,
    )
    consumer.start()
    app.state.consumer_shutdown = shutdown
    app.state.consumer_thread = consumer
    await _register_with_config()
    try:
        yield
    finally:
        shutdown.set()
        consumer.join(timeout=15.0)
        await pool.close()


app = FastAPI(lifespan=lifespan)


@app.post("/update")
async def update_balance(payload: UpdateRequest) -> Dict[str, int]:
    """Optional direct HTTP update (not used by facade in the MQ lab)."""
    pool = app.state.pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO accounts (user_id, balance)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE
            SET balance = accounts.balance + EXCLUDED.balance
            RETURNING balance
            """,
            payload.user_id,
            payload.amount,
        )
        new_balance = row["balance"]
    return {"balance": new_balance}


@app.get("/balance/{user_id}")
async def get_balance(user_id: str) -> Dict[str, int]:
    pool = app.state.pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance FROM accounts WHERE user_id = $1",
            user_id,
        )
        balance = int(row["balance"]) if row else 0
    return {"balance": balance}


@app.get("/balances")
async def get_balances() -> Dict[str, Dict[str, int]]:
    pool = app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, balance FROM accounts")
        balances = {r["user_id"]: int(r["balance"]) for r in rows}
    return {"balances": balances}


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}
