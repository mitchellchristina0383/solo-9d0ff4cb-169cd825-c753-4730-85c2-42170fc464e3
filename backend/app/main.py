"""模拟写入后端。

每个后端实例：
  * 把收到的写入落到自己的 SQLite（/data/writes.db），持久化保存；
  * 支持慢回执：POST /writes?delay=秒，先落库再延迟回执，模拟"已送达、未回执"；
  * 支持健康故障注入：POST /control/fault on/off，故障期间 /healthz 503、写入 500；
  * 支持持久化内容字节配额：POST /control/quota {"bytes": N|null}，配额落 meta 表，
    重启后仍生效；/healthz 与 /quota 报告已用/剩余字节；
  * 容量判断与落库在同一临界区完成（共享连接 + 写锁），按 UTF-8 字节数原子占额，
    并发请求不会重复占用同一份余量；配额满返回 507，已落库记录不受影响；
  * /writes/<id> 供代理在切流后仍向"原送达节点"查询回执；
  * /records 列出本节点全部写入记录。
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import aiosqlite
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

NODE_NAME = os.environ.get("NODE_NAME", "backend")
DB_PATH = os.environ.get("BACKEND_DB", "/data/writes.db")

# 故障开关是进程内瞬态，不需要落库。
_fault_on = False
# 正在慢回执中的请求 id -> asyncio.Event，用于 /writes/{id} 等待回执完成。
_pending: dict[str, asyncio.Event] = {}

# 共享连接 + 写锁：容量"检查-占额-落库"必须在同一临界区，保证并发请求
# 不会重复占用同一份剩余配额。锁只在短小的读写段内持有，慢回执的 sleep 在锁外。
_conn: aiosqlite.Connection | None = None
_write_lock: asyncio.Lock


async def init_db() -> None:
    global _conn, _write_lock
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    _conn = aiosqlite.connect(DB_PATH)
    await _conn
    _conn.row_factory = aiosqlite.Row
    _write_lock = asyncio.Lock()
    await _conn.execute("PRAGMA journal_mode=WAL")
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS writes (
            id           TEXT PRIMARY KEY,
            payload      TEXT NOT NULL,
            client_ip    TEXT,
            received_at  REAL NOT NULL,
            acked_at     REAL,
            delay        REAL NOT NULL DEFAULT 0
        )
        """
    )
    await _conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key    TEXT PRIMARY KEY,
            value  TEXT NOT NULL
        )
        """
    )
    await _conn.commit()


async def _get_quota() -> int | None:
    """持久化内容的字节配额；None 表示不限制。"""
    cur = await _conn.execute("SELECT value FROM meta WHERE key = 'quota_bytes'")
    row = await cur.fetchone()
    return int(row["value"]) if row else None


async def _set_quota(quota: int | None) -> None:
    async with _write_lock:
        if quota is None:
            await _conn.execute("DELETE FROM meta WHERE key = 'quota_bytes'")
        else:
            await _conn.execute(
                "INSERT INTO meta (key, value) VALUES ('quota_bytes', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (str(quota),),
            )
        await _conn.commit()


async def _used_bytes() -> int:
    """已持久化内容的 UTF-8 字节数（payload 按 UTF-8 编码计）。"""
    cur = await _conn.execute(
        "SELECT COALESCE(SUM(LENGTH(CAST(payload AS BLOB))), 0) AS used FROM writes"
    )
    row = await cur.fetchone()
    return int(row["used"])


async def _quota_state() -> dict[str, Any]:
    quota = await _get_quota()
    used = await _used_bytes()
    return {
        "node": NODE_NAME,
        "quota_bytes": quota,
        "used_bytes": used,
        "remaining_bytes": None if quota is None else quota - used,
        "unlimited": quota is None,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    try:
        yield
    finally:
        if _conn is not None:
            await _conn.close()


app = FastAPI(title=f"mock-write-backend-{NODE_NAME}", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> JSONResponse:
    # 健康与配额是两回事：配额满时节点仍健康（200），是否可切流由代理判断。
    quota = await _quota_state()
    if _fault_on:
        return JSONResponse(
            status_code=503,
            content={"node": NODE_NAME, "healthy": False, "quota": quota},
        )
    return JSONResponse(
        status_code=200,
        content={"node": NODE_NAME, "healthy": True, "quota": quota},
    )


@app.get("/quota")
async def get_quota() -> dict[str, Any]:
    return await _quota_state()


@app.post("/control/quota")
async def control_quota(request: Request) -> JSONResponse:
    """设置/清除持久化内容的字节配额。body: {"bytes": N} 或 {"bytes": null}。

    调低配额只影响后续写入，已落库的旧记录一律保留（used 可能暂时超过配额，
    此时剩余为负、新写入一律拒绝，直到配额调高或记录被清理）。
    """
    body = await request.json()
    raw = body.get("bytes", None)
    if raw is not None:
        try:
            raw = int(raw)
        except (TypeError, ValueError):
            return JSONResponse(status_code=400,
                                content={"error": "bytes must be an integer or null"})
        if raw < 0:
            return JSONResponse(status_code=400,
                                content={"error": "bytes must be >= 0 or null"})
    await _set_quota(raw)
    state = await _quota_state()
    state["ok"] = True
    return JSONResponse(content=state)


@app.post("/control/fault")
async def control_fault(request: Request) -> dict[str, Any]:
    """注入/解除健康故障。body: {"on": true|false}"""
    global _fault_on
    body = await request.json()
    _fault_on = bool(body.get("on", True))
    return {"node": NODE_NAME, "fault": _fault_on}


@app.post("/writes")
async def create_write(request: Request) -> JSONResponse:
    if _fault_on:
        # 故障期间拒绝写入（代理探测会先失败，正常不会走到这里）。
        return JSONResponse(
            status_code=500,
            content={"node": NODE_NAME, "error": "node in fault state, write rejected"},
        )

    delay = float(request.query_params.get("delay", "0"))
    try:
        body = await request.json()
    except Exception:
        body = {}
    write_id = str(body.get("id") or uuid.uuid4())
    payload = str(body.get("payload", ""))
    payload_bytes = len(payload.encode("utf-8"))
    now = time.time()

    # 原子占额 + 落库：检查剩余、写入行在同一临界区完成，并发请求不会
    # 重复占用同一份余量。同 id 重写（INSERT OR REPLACE）按差值计费。
    async with _write_lock:
        quota = await _get_quota()
        if quota is not None:
            cur = await _conn.execute(
                "SELECT LENGTH(CAST(payload AS BLOB)) AS b FROM writes WHERE id = ?",
                (write_id,),
            )
            row = await cur.fetchone()
            existing = int(row["b"]) if row else 0
            used = await _used_bytes()
            if used - existing + payload_bytes > quota:
                return JSONResponse(
                    status_code=507,
                    content={
                        "node": NODE_NAME,
                        "id": write_id,
                        "accepted": False,
                        "error": "quota_exceeded",
                        "detail": "insufficient storage quota, write not accepted",
                        "quota_bytes": quota,
                        "used_bytes": used,
                        "remaining_bytes": quota - used,
                        "needed_bytes": payload_bytes,
                    },
                )
        # 先落库，再慢回执：即使回执期间连接被切，写入也已经在本节点持久化。
        await _conn.execute(
            "INSERT OR REPLACE INTO writes (id, payload, client_ip, received_at, delay) "
            "VALUES (?, ?, ?, ?, ?)",
            (write_id, payload, request.client.host if request.client else None, now, delay),
        )
        await _conn.commit()

    if delay > 0:
        done = asyncio.Event()
        _pending[write_id] = done
        try:
            await asyncio.sleep(delay)
        finally:
            # 已送达的写入回执不受配额影响：调低配额不取消在途写入。
            async with _write_lock:
                await _conn.execute(
                    "UPDATE writes SET acked_at = ? WHERE id = ?", (time.time(), write_id)
                )
                await _conn.commit()
            _pending.pop(write_id, None)
            done.set()
    else:
        async with _write_lock:
            await _conn.execute(
                "UPDATE writes SET acked_at = ? WHERE id = ?", (time.time(), write_id)
            )
            await _conn.commit()

    return JSONResponse(
        status_code=201,
        content={
            "id": write_id,
            "node": NODE_NAME,
            "received_at": now,
            "delay": delay,
            "status": "acked",
            "payload_bytes": payload_bytes,
        },
    )


@app.get("/writes/{write_id}")
async def get_write(write_id: str, wait: bool = False) -> JSONResponse:
    """查询单条写入的回执状态。

    wait=true 时，如果该写入仍在慢回执中，最多等待 60 秒，供代理向原送达节点取回执。
    """
    if wait and write_id in _pending:
        try:
            await asyncio.wait_for(_pending[write_id].wait(), timeout=60)
        except asyncio.TimeoutError:
            pass

    cur = await _conn.execute("SELECT * FROM writes WHERE id = ?", (write_id,))
    row = await cur.fetchone()
    if row is None:
        return JSONResponse(
            status_code=404, content={"node": NODE_NAME, "id": write_id, "found": False}
        )
    data = dict(row)
    data["node"] = NODE_NAME
    data["found"] = True
    data["status"] = "acked" if data["acked_at"] is not None else "pending"
    return JSONResponse(status_code=200, content=data)


@app.get("/records")
async def records() -> dict[str, Any]:
    cur = await _conn.execute(
        "SELECT id, payload, client_ip, received_at, acked_at, delay "
        "FROM writes ORDER BY received_at"
    )
    rows = await cur.fetchall()
    return {"node": NODE_NAME, "count": len(rows), "records": [dict(r) for r in rows]}
