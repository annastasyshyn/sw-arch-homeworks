import threading
from typing import Dict, List

from fastapi import FastAPI
from pydantic import BaseModel, Field

_lock = threading.Lock()
# service name -> ordered unique base URLs (no trailing slash)
REGISTRY: Dict[str, List[str]] = {}


class RegisterIn(BaseModel):
    service: str = Field(..., min_length=1)
    url: str = Field(..., min_length=1)


app = FastAPI(title="config-server", version="1.0.0")


@app.post("/register")
def register(body: RegisterIn) -> Dict[str, object]:
    name = body.service.strip()
    url = body.url.rstrip("/")
    with _lock:
        urls = REGISTRY.setdefault(name, [])
        if url not in urls:
            urls.append(url)
        snapshot = list(urls)
    print(f"[config-server] registered service={name!r} url={url!r} -> {snapshot}")
    return {"status": "ok", "service": name, "instances": snapshot}


@app.get("/instances/{service_name}")
def instances(service_name: str) -> Dict[str, List[str]]:
    with _lock:
        out = list(REGISTRY.get(service_name.strip(), []))
    print(f"[config-server] lookup service={service_name!r} -> {out}")
    return {"instances": out}


@app.get("/health")
def health() -> Dict[str, str]:
    return {"status": "ok"}
