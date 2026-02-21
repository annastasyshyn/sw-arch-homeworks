import asyncio
from typing import Dict

from fastapi import FastAPI
from pydantic import BaseModel


class UpdateRequest(BaseModel):
    user_id: str
    amount: int


app = FastAPI()

_balances: Dict[str, int] = {}
_lock = asyncio.Lock()


@app.post("/update")
async def update_balance(payload: UpdateRequest) -> Dict[str, int]:
    async with _lock:
        current = _balances.get(payload.user_id, 0)
        new_balance = current + payload.amount
        _balances[payload.user_id] = new_balance
    return {"balance": new_balance}


@app.get("/balance/{user_id}")
async def get_balance(user_id: str) -> Dict[str, int]:
    async with _lock:
        balance = _balances.get(user_id, 0)
    return {"balance": balance}


@app.get("/balances")
async def get_balances() -> Dict[str, Dict[str, int]]:
    async with _lock:
        snapshot = dict(_balances)
    return {"balances": snapshot}
