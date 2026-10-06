from contextlib import asynccontextmanager
from typing import AsyncGenerator
import asyncpg
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel

PRIMARY_DB_URL = "postgresql://localhost:5433/postgres"
REPLICA_DB_URL = "postgresql://localhost:5434/postgres"

db_pools = {
    "writer": None,
    "reader": None
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    db_pools["writer"] = await asyncpg.create_pool(PRIMARY_DB_URL, min_size=1, max_size=10)
    db_pools["reader"] = await asyncpg.create_pool(REPLICA_DB_URL, min_size=1, max_size=10)
    yield
    await db_pools["writer"].close()
    await db_pools["reader"].close()

app = FastAPI(title="Read/Write Split Replication API", lifespan=lifespan)

async def get_writer_db() -> AsyncGenerator[asyncpg.Connection, None]:
    async with db_pools["writer"].acquire() as conn:
        yield conn

async def get_reader_db() -> AsyncGenerator[asyncpg.Connection, None]:
    async with db_pools["reader"].acquire() as conn:
        yield conn

class UserCreate(BaseModel):
    name: str

@app.post("/users")
async def create_user(
    payload: UserCreate,
    db: asyncpg.Connection = Depends(get_writer_db)
):
    row = await db.fetchrow(
        "INSERT INTO users (name) VALUES ($1) RETURNING id, name",
        payload.name
    )
    port = await db.fetchval("SELECT inet_server_port();")
    return {
        "message": "User created successfully",
        "user": dict(row),
        "served_by_port": port,
        "database_role": "PRIMARY (WRITER)"
    }

@app.get("/users")
async def list_users(
    db: asyncpg.Connection = Depends(get_reader_db)
):
    rows = await db.fetch("SELECT id, name FROM users ORDER BY id ASC;")
    port = await db.fetchval("SELECT inet_server_port();")
    return {
        "users": [dict(r) for r in rows],
        "total": len(rows),
        "served_by_port": port,
        "database_role": "REPLICA (READER)"
    }

@app.post("/test-replica-write")
async def test_replica_write(
    payload: UserCreate,
    db: asyncpg.Connection = Depends(get_reader_db)
):
    try:
        await db.execute("INSERT INTO users (name) VALUES ($1);", payload.name)
        return {"status": "Unexpected success"}
    except asyncpg.ReadOnlySQLTransactionError as e:
        raise HTTPException(
            status_code=400,
            detail=f"Replication safety triggered: {str(e)}"
        )
