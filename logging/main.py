import asyncio
from typing import Any, Dict

from fastapi import FastAPI
from pydantic import BaseModel


class LogMessage(BaseModel):
    transaction_id: str
    user_id: str
    amount: int


app = FastAPI()

_logs: Dict[str, Dict[str, Any]] = {}
_lock = asyncio.Lock()


@app.post("/log")
async def log_message(payload: LogMessage) -> Dict[str, str]:
    async with _lock:
        _logs[payload.transaction_id] = payload.model_dump()
    print(payload.model_dump())
    return {"status": "ok"}


@app.get("/logs")
async def get_logs() -> Dict[str, Dict[str, Dict[str, Any]]]:
    async with _lock:
        snapshot = dict(_logs)
    return {"logs": snapshot}
