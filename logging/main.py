import json
import os
import socket
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict

from fastapi import FastAPI
from pydantic import BaseModel

from hazelcast.asyncio import HazelcastClient

from k8s_client import KubernetesApiClient


SERVICE_NAME = os.getenv("SERVICE_NAME", "logging-service")
INSTANCE_ID = os.getenv(
    "LOGGING_INSTANCE_ID",
    os.getenv("POD_NAME", socket.gethostname() or str(uuid.uuid4())[:8]),
)
CONFIGMAP_NAME = os.getenv("APP_CONFIGMAP_NAME", "microservices-config")


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    print(f"[{INSTANCE_ID}] Starting logging-service ...", flush=True)

    k8s = KubernetesApiClient()
    app.state.k8s = k8s

    cluster_name = await k8s.get_config_value_with_retry(
        CONFIGMAP_NAME, "hazelcast.cluster_name"
    )
    members_raw = await k8s.get_config_value_with_retry(
        CONFIGMAP_NAME, "hazelcast.members"
    )
    map_name = await k8s.get_config_value_with_retry(
        CONFIGMAP_NAME, "hazelcast.map_name"
    )
    members = [m.strip() for m in members_raw.split(",") if m.strip()]
    print(
        f"[{INSTANCE_ID}] Loaded Hazelcast config from ConfigMap '{CONFIGMAP_NAME}': "
        f"cluster_name={cluster_name!r} members={members} map={map_name!r}",
        flush=True,
    )

    try:
        hz_client = await HazelcastClient.create_and_start(
            cluster_members=members,
            cluster_name=cluster_name,
        )
        logs_map = await hz_client.get_map(map_name)
        print(
            f"[{INSTANCE_ID}] Connected to Hazelcast cluster '{cluster_name}' "
            f"(map={map_name})",
            flush=True,
        )
    except Exception as exc:
        print(f"[{INSTANCE_ID}] Hazelcast connection failed: {exc}", flush=True)
        raise

    app.state.hz_client = hz_client
    app.state.logs_map = logs_map

    try:
        yield
    finally:
        print(f"[{INSTANCE_ID}] Shutting down ...", flush=True)
        if hz_client is not None:
            await hz_client.shutdown()
        await k8s.close()
        print(f"[{INSTANCE_ID}] Shutdown complete", flush=True)


app = FastAPI(lifespan=lifespan)


@app.post("/log")
async def log_message(payload: LogMessage) -> Dict[str, str]:
    data = payload.model_dump()
    logs_map = app.state.logs_map
    key = payload.transaction_id
    await logs_map.set(key, _serialize(data))
    print(
        f"[{INSTANCE_ID}] Received POST /log transaction_id={payload.transaction_id} "
        f"user_id={payload.user_id} amount={payload.amount}",
        flush=True,
    )
    return {"status": "ok", "instance_id": INSTANCE_ID}


@app.get("/logs")
async def get_logs() -> Dict[str, Dict[str, Dict[str, Any]]]:
    logs_map = app.state.logs_map
    entries = await logs_map.entry_set()
    snapshot: Dict[str, Dict[str, Any]] = {}
    for key, value in entries:
        try:
            snapshot[str(key)] = _deserialize(value)
        except Exception:
            snapshot[str(key)] = {"raw": str(value)}
    print(
        f"[{INSTANCE_ID}] Received GET /logs returning {len(snapshot)} entries",
        flush=True,
    )
    return {"logs": snapshot}


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "instance_id": INSTANCE_ID}
