import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

SHARD_AM_URL = "postgresql://localhost:5435/postgres"
SHARD_NZ_URL = "postgresql://localhost:5436/postgres"

shard_pools = {
    "shard_am": None,
    "shard_nz": None
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    shard_pools["shard_am"] = await asyncpg.create_pool(SHARD_AM_URL, min_size=1, max_size=10)
    shard_pools["shard_nz"] = await asyncpg.create_pool(SHARD_NZ_URL, min_size=1, max_size=10)
    yield
    await shard_pools["shard_am"].close()
    await shard_pools["shard_nz"].close()

app = FastAPI(title="Sharded Database Router API", lifespan=lifespan)

def get_shard_name(key: str) -> str:
    cleaned = key.strip().lower()
    if not cleaned or not cleaned[0].isalpha():
        raise HTTPException(
            status_code=400,
            detail="Username must start with an alphabet character [a-z]"
        )
    return "shard_am" if cleaned[0] <= 'm' else "shard_nz"

class UserCreate(BaseModel):
    username: str
    email: str

@app.post("/users")
async def create_user(payload: UserCreate) -> Dict[str, Any]:
    shard_name = get_shard_name(payload.username)
    pool = shard_pools[shard_name]

    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id, username, email",
                payload.username,
                payload.email
            )
            port = await conn.fetchval("SELECT inet_server_port();")
            return {
                "message": "User saved to shard",
                "user": dict(row),
                "shard": shard_name,
                "served_by_port": port
            }
        except asyncpg.UniqueViolationError:
            raise HTTPException(status_code=409, detail=f"User '{payload.username}' already exists")

@app.get("/users/{username}")
async def get_user(username: str) -> Dict[str, Any]:
    shard_name = get_shard_name(username)
    pool = shard_pools[shard_name]

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, username, email FROM users WHERE username = $1",
            username
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"User '{username}' not found on {shard_name}")

        port = await conn.fetchval("SELECT inet_server_port();")
        return {
            "user": dict(row),
            "shard": shard_name,
            "served_by_port": port
        }

@app.get("/users")
async def list_all_users_scatter_gather() -> Dict[str, Any]:
    async def fetch_from_shard(name: str):
        pool = shard_pools[name]
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, username, email FROM users ORDER BY username ASC")
            port = await conn.fetchval("SELECT inet_server_port();")
            return name, port, [dict(r) for r in rows]

    results = await asyncio.gather(
        fetch_from_shard("shard_am"),
        fetch_from_shard("shard_nz")
    )

    merged = []
    shards_meta = {}
    for shard_name, port, rows in results:
        merged.extend(rows)
        shards_meta[shard_name] = {"port": port, "count": len(rows)}

    return {
        "total_users": len(merged),
        "scatter_gather_summary": shards_meta,
        "users": merged
    }
