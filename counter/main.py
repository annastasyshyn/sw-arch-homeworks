import os
from contextlib import asynccontextmanager
from typing import Dict

import asyncpg
from fastapi import FastAPI
from pydantic import BaseModel

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@postgres:5432/postgres",
)


class UpdateRequest(BaseModel):
    user_id: str
    amount: int


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
    try:
        yield
    finally:
        await pool.close()


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
    return {"status": "ok"}
