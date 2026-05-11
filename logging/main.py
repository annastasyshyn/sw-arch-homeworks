import json
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict

import httpx
from fastapi import FastAPI
from pydantic import BaseModel

from hazelcast.asyncio import HazelcastClient

INSTANCE_ID = os.getenv("LOGGING_INSTANCE_ID", str(uuid.uuid4())[:8])
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "").rstrip("/")
SERVICE_BASE_URL = os.getenv("SERVICE_BASE_URL", "").rstrip("/")
SERVICE_NAME = os.getenv("SERVICE_NAME", "logging-service")
HAZELCAST_MEMBERS = os.getenv(
    "HAZELCAST_MEMBERS", "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701"
).split(",")
MAP_NAME = os.getenv("HAZELCAST_MAP_NAME", "logs")


class LogMessage(BaseModel):
    transaction_id: str
    user_id: str
    amount: int
    timestamp: float | None = None


def _serialize(value: Dict[str, Any]) -> str:
    return json.dumps(value)


def _deserialize(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


async def _register_with_config() -> None:
    if not CONFIG_SERVER_URL or not SERVICE_BASE_URL:
        print(
            f"[logging-service instance {INSTANCE_ID}] "
            "skipping config registration (missing CONFIG_SERVER_URL or SERVICE_BASE_URL)"
        )
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
        print(
            f"[logging-service instance {INSTANCE_ID}] "
            f"registered at config-server as {SERVICE_BASE_URL!r}"
        )
    except Exception as e:
        print(f"[logging-service instance {INSTANCE_ID}] config registration failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        hz_client = await HazelcastClient.create_and_start(
            cluster_members=HAZELCAST_MEMBERS,
            cluster_name=os.getenv("HZ_CLUSTER_NAME", "dev"),
        )
        logs_map = await hz_client.get_map(MAP_NAME)
        print(f"[logging-service instance {INSTANCE_ID}] Connected to Hazelcast cluster")
    except Exception as e:
        print(f"[logging-service instance {INSTANCE_ID}] Hazelcast connection failed: {e}")
        raise
    app.state.hz_client = hz_client
    app.state.logs_map = logs_map
    await _register_with_config()
    try:
        yield
    finally:
        if hz_client:
            await hz_client.shutdown()


app = FastAPI(lifespan=lifespan)


@app.post("/log")
async def log_message(payload: LogMessage) -> Dict[str, str]:
    data = payload.model_dump()
    logs_map = app.state.logs_map
    key = payload.transaction_id
    await logs_map.set(key, _serialize(data))
    print(
        f"[logging-service instance {INSTANCE_ID}] Received POST /log transaction_id={payload.transaction_id} user_id={payload.user_id} amount={payload.amount}"
    )
    return {"status": "ok"}


@app.get("/logs")
async def get_logs() -> Dict[str, Dict[str, Dict[str, Any]]]:
    logs_map = app.state.logs_map
    entries = await logs_map.entry_set()
    snapshot = {}
    for key, value in entries:
        try:
            snapshot[str(key)] = _deserialize(value)
        except Exception:
            snapshot[str(key)] = {"raw": str(value)}
    print(
        f"[logging-service instance {INSTANCE_ID}] Received GET /logs returning {len(snapshot)} entries"
    )
    return {"logs": snapshot}


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "instance_id": INSTANCE_ID}
