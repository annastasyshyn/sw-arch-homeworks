import asyncio
import json
import os
import socket
import threading
from contextlib import asynccontextmanager
from typing import Any, Dict

import asyncpg
import hazelcast
from fastapi import FastAPI
from pydantic import BaseModel

from k8s_client import KubernetesApiClient


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@postgres:5432/postgres",
)
SERVICE_NAME = os.getenv("SERVICE_NAME", "counter-service")
INSTANCE_ID = os.getenv("INSTANCE_ID", os.getenv("POD_NAME", socket.gethostname()))
CONFIGMAP_NAME = os.getenv("APP_CONFIGMAP_NAME", "microservices-config")


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
        f"[{INSTANCE_ID}] applied from queue user_id={user_id!r} amount={amount} "
        f"transaction_id={data.get('transaction_id')!r}",
        flush=True,
    )


def _consumer_loop(
    pool: asyncpg.Pool,
    loop: asyncio.AbstractEventLoop,
    shutdown: threading.Event,
    cluster_name: str,
    members: list[str],
    queue_name: str,
) -> None:
    print(
        f"[{INSTANCE_ID}] queue consumer starting cluster={cluster_name!r} "
        f"members={members} queue={queue_name!r}",
        flush=True,
    )
    reconnect_delay_seconds = 1.0
    max_reconnect_delay_seconds = 10.0
    while not shutdown.is_set():
        hz = None
        try:
            hz = hazelcast.HazelcastClient(
                cluster_members=members,
                cluster_name=cluster_name,
            )
            q = hz.get_queue(queue_name).blocking()
            reconnect_delay_seconds = 1.0
            print(
                f"[{INSTANCE_ID}] queue consumer connected to Hazelcast",
                flush=True,
            )
            while not shutdown.is_set():
                try:
                    item = q.poll(1.0)
                except Exception as exc:
                    if not shutdown.is_set():
                        print(
                            f"[{INSTANCE_ID}] queue poll failed, reconnecting: {exc}",
                            flush=True,
                        )
                    break
                if item is None:
                    continue
                try:
                    fut = asyncio.run_coroutine_threadsafe(
                        _apply_queued_item(pool, item), loop
                    )
                    fut.result(timeout=120)
                except Exception as exc:
                    print(
                        f"[{INSTANCE_ID}] queue item failed: {exc}",
                        flush=True,
                    )
        except Exception as exc:
            if not shutdown.is_set():
                print(
                    f"[{INSTANCE_ID}] queue consumer connection failed: {exc}",
                    flush=True,
                )
        finally:
            if hz is not None:
                try:
                    hz.shutdown()
                except Exception:
                    pass
        if shutdown.is_set():
            break
        print(
            f"[{INSTANCE_ID}] queue consumer retry in {reconnect_delay_seconds:.1f}s",
            flush=True,
        )
        shutdown.wait(reconnect_delay_seconds)
        reconnect_delay_seconds = min(
            reconnect_delay_seconds * 2.0,
            max_reconnect_delay_seconds,
        )
    print(f"[{INSTANCE_ID}] queue consumer stopped", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[{INSTANCE_ID}] Starting counter-service ...", flush=True)

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

    k8s = KubernetesApiClient()
    app.state.k8s = k8s
    cluster_name = await k8s.get_config_value_with_retry(
        CONFIGMAP_NAME, "hazelcast.cluster_name"
    )
    members_raw = await k8s.get_config_value_with_retry(
        CONFIGMAP_NAME, "hazelcast.members"
    )
    queue_name = await k8s.get_config_value_with_retry(
        CONFIGMAP_NAME, "hazelcast.queue_name"
    )
    members = [m.strip() for m in members_raw.split(",") if m.strip()]
    print(
        f"[{INSTANCE_ID}] Loaded MQ config from ConfigMap '{CONFIGMAP_NAME}': "
        f"cluster_name={cluster_name!r} members={members} queue={queue_name!r}",
        flush=True,
    )

    shutdown = threading.Event()
    loop = asyncio.get_running_loop()
    consumer = threading.Thread(
        target=_consumer_loop,
        args=(pool, loop, shutdown, cluster_name, members, queue_name),
        name="hz-queue-consumer",
        daemon=True,
    )
    consumer.start()
    app.state.consumer_shutdown = shutdown
    app.state.consumer_thread = consumer
    try:
        yield
    finally:
        print(f"[{INSTANCE_ID}] Shutting down ...", flush=True)
        shutdown.set()
        consumer.join(timeout=15.0)
        await k8s.close()
        await pool.close()
        print(f"[{INSTANCE_ID}] Shutdown complete", flush=True)


app = FastAPI(lifespan=lifespan)


@app.post("/update")
async def update_balance(payload: UpdateRequest) -> Dict[str, int]:
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
    return {"status": "ok", "instance_id": INSTANCE_ID}
