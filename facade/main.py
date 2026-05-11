import asyncio
import json
import os
import random
import socket
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List

import hazelcast
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from k8s_client import KubernetesApiClient, ServiceInstance


SERVICE_NAME = os.getenv("SERVICE_NAME", "facade-service")
INSTANCE_ID = os.getenv("INSTANCE_ID", os.getenv("POD_NAME", socket.gethostname()))
CONFIGMAP_NAME = os.getenv("APP_CONFIGMAP_NAME", "microservices-config")

LOGGING_SERVICE_NAME = os.getenv("LOGGING_SERVICE_NAME", "logging-service")
COUNTER_SERVICE_NAME = os.getenv("COUNTER_SERVICE_NAME", "counter-service")
LOGGING_SERVICE_PORT_NAME = os.getenv("LOGGING_SERVICE_PORT_NAME", "http")
COUNTER_SERVICE_PORT_NAME = os.getenv("COUNTER_SERVICE_PORT_NAME", "http")
FACADE_SERVICE_PORT_NAME = os.getenv("FACADE_SERVICE_PORT_NAME", "http")

COUNTER_HTTP_TIMEOUT = float(os.getenv("COUNTER_HTTP_TIMEOUT", "3.0"))
LOGGING_HTTP_TIMEOUT = float(os.getenv("LOGGING_HTTP_TIMEOUT", "10.0"))


_hz_lock = threading.Lock()
_hz_client: hazelcast.HazelcastClient | None = None
_hz_members: list[str] = []
_hz_cluster_name: str = "dev"
_hz_queue_name: str = "counter-updates"


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
                cluster_members=_hz_members,
                cluster_name=_hz_cluster_name,
            )
        queue = _hz_client.get_queue(_hz_queue_name).blocking()
        queue.put(serialized)
    print(
        f"[{INSTANCE_ID}] enqueued counter update "
        f"transaction_id={payload.get('transaction_id')!r} queue={_hz_queue_name!r}",
        flush=True,
    )


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
    global _hz_members, _hz_cluster_name, _hz_queue_name

    print(f"[{INSTANCE_ID}] Starting facade-service ...", flush=True)

    app.state.metrics = Metrics()
    app.state.client = httpx.AsyncClient()
    app.state.k8s = KubernetesApiClient()

    cluster_name = await app.state.k8s.get_config_value_with_retry(
        CONFIGMAP_NAME, "hazelcast.cluster_name"
    )
    members_raw = await app.state.k8s.get_config_value_with_retry(
        CONFIGMAP_NAME, "hazelcast.members"
    )
    queue_name = await app.state.k8s.get_config_value_with_retry(
        CONFIGMAP_NAME, "hazelcast.queue_name"
    )
    _hz_cluster_name = cluster_name
    _hz_members = [m.strip() for m in members_raw.split(",") if m.strip()]
    _hz_queue_name = queue_name
    print(
        f"[{INSTANCE_ID}] Loaded MQ config from ConfigMap '{CONFIGMAP_NAME}': "
        f"cluster_name={cluster_name!r} members={_hz_members} queue={queue_name!r}",
        flush=True,
    )

    try:
        yield
    finally:
        await app.state.client.aclose()
        await app.state.k8s.close()
        await asyncio.to_thread(_shutdown_hz_client)
        print(f"[{INSTANCE_ID}] Shutdown complete", flush=True)


app = FastAPI(lifespan=lifespan)


def _shuffled(values: List[str]) -> List[str]:
    out = list(values)
    random.shuffle(out)
    return out


async def discover_logging_urls() -> List[str]:
    k8s: KubernetesApiClient = app.state.k8s
    return await k8s.discover_service_addresses(
        LOGGING_SERVICE_NAME,
        port_name=LOGGING_SERVICE_PORT_NAME,
        scheme="http",
    )


async def discover_counter_urls() -> List[str]:
    k8s: KubernetesApiClient = app.state.k8s
    return await k8s.discover_service_addresses(
        COUNTER_SERVICE_NAME,
        port_name=COUNTER_SERVICE_PORT_NAME,
        scheme="http",
    )


async def logging_request(
    method: str,
    path: str,
    payload: Dict[str, Any] | None = None,
) -> httpx.Response:
    urls = await discover_logging_urls()
    if not urls:
        raise HTTPException(
            status_code=503,
            detail="No logging-service instances discovered in Kubernetes",
        )
    client: httpx.AsyncClient = app.state.client
    k8s: KubernetesApiClient = app.state.k8s
    last_error: Exception | None = None
    for base in _shuffled(urls):
        url = f"{base.rstrip('/')}{path}"
        try:
            resp = await client.request(
                method, url, json=payload, timeout=LOGGING_HTTP_TIMEOUT
            )
            if resp.is_success:
                print(
                    f"[{INSTANCE_ID}] logging {method} {path} -> {base} OK",
                    flush=True,
                )
                return resp
            last_error = httpx.HTTPStatusError(
                f"HTTP {resp.status_code}",
                request=resp.request,
                response=resp,
            )
        except Exception as exc:
            last_error = exc
            print(
                f"[{INSTANCE_ID}] logging {method} {path} -> {base} FAILED: {exc}",
                flush=True,
            )
            k8s.invalidate_service_cache(
                LOGGING_SERVICE_NAME, LOGGING_SERVICE_PORT_NAME
            )
    raise HTTPException(
        status_code=503,
        detail=f"All logging-service instances unavailable: {last_error}",
    )


async def counter_get_json(path: str) -> Dict[str, Any] | None:
    urls = await discover_counter_urls()
    if not urls:
        print(
            f"[{INSTANCE_ID}] no counter-service instances discovered for {path}",
            flush=True,
        )
        return None
    client: httpx.AsyncClient = app.state.client
    metrics: Metrics = app.state.metrics
    k8s: KubernetesApiClient = app.state.k8s
    last_error: Exception | None = None
    for base in _shuffled(urls):
        url = f"{base.rstrip('/')}{path}"
        start = time.perf_counter()
        try:
            resp = await client.get(url, timeout=COUNTER_HTTP_TIMEOUT)
            await metrics.add_counter(time.perf_counter() - start)
            if resp.is_success:
                return resp.json()
            last_error = httpx.HTTPStatusError(
                f"HTTP {resp.status_code}",
                request=resp.request,
                response=resp,
            )
        except Exception as exc:
            await metrics.add_counter(time.perf_counter() - start)
            last_error = exc
            print(
                f"[{INSTANCE_ID}] counter GET {url} failed: {exc}",
                flush=True,
            )
            k8s.invalidate_service_cache(
                COUNTER_SERVICE_NAME, COUNTER_SERVICE_PORT_NAME
            )
    print(
        f"[{INSTANCE_ID}] all counter-service instances failed for {path}: {last_error}",
        flush=True,
    )
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
    balance: int | None
    if balance_data is None:
        balance = None
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


@app.get("/services")
async def list_services() -> Dict[str, Any]:
    """Show which pods are currently registered behind each Kubernetes
    Service, mimicking the "show registered services" Consul UI view.
    """
    k8s: KubernetesApiClient = app.state.k8s
    targets = [
        (SERVICE_NAME, FACADE_SERVICE_PORT_NAME),
        (LOGGING_SERVICE_NAME, LOGGING_SERVICE_PORT_NAME),
        (COUNTER_SERVICE_NAME, COUNTER_SERVICE_PORT_NAME),
    ]
    snapshot: Dict[str, Any] = {}
    for service, port_name in targets:
        instances = await k8s.get_service_instances(
            service, port_name=port_name, use_cache=False
        )
        snapshot[service] = _serialize_instances(instances)
    return {"namespace": k8s.namespace, "services": snapshot}


def _serialize_instances(instances: List[ServiceInstance]) -> Dict[str, Any]:
    ready = [
        {"pod_name": i.pod_name, "address": i.host_port}
        for i in instances
        if i.ready
    ]
    not_ready = [
        {"pod_name": i.pod_name, "address": i.host_port}
        for i in instances
        if not i.ready
    ]
    return {
        "ready_instances": ready,
        "not_ready_instances": not_ready,
        "ready_count": len(ready),
        "not_ready_count": len(not_ready),
    }


@app.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    metrics: Metrics = app.state.metrics
    return await metrics.snapshot()


@app.post("/metrics/reset")
async def reset_metrics() -> Dict[str, str]:
    metrics: Metrics = app.state.metrics
    await metrics.reset()
    return {"status": "ok"}


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok", "instance_id": INSTANCE_ID}
