"""Distributed Financial Intelligence Simulator — FastAPI entrypoint."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from .algorithms.event_sourcing import EventSourcingProtocol
from .algorithms.eventual import EventualProtocol
from .algorithms.raft import RaftProtocol
from .algorithms.strong import StrongProtocol
from .algorithms.two_pc import TwoPCProtocol
from .api.routes import router
from .api.ws import hub
from .db import init_db
from .engine.faults import FaultInjector
from .engine.repo import Repository
from .engine.scenarios import ScenarioRunner
from .engine.transactions import TransactionEngine
from .simulation.cluster import Cluster


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    repo = Repository()
    cluster = Cluster(repo)
    for proto in (RaftProtocol(), TwoPCProtocol(), EventualProtocol(),
                  StrongProtocol(), EventSourcingProtocol()):
        proto.repo = repo
        cluster.protocols[proto.name] = proto
    await repo.seed_accounts(cluster.accounts)
    cluster.seed_replicas()
    cluster.bus.persist = repo.log_event
    cluster.bus.subscribe(hub.broadcast_event)
    await cluster.start()
    app.state.cluster = cluster
    app.state.repo = repo
    app.state.engine = TransactionEngine(cluster, repo)
    app.state.faults = FaultInjector(cluster)
    app.state.runner = ScenarioRunner(cluster, repo, app.state.engine, app.state.faults)
    yield
    await cluster.stop()


app = FastAPI(title="Distributed Financial Intelligence Simulator", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(router)


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await hub.connect(ws)
    try:
        while True:
            await ws.receive_text()  # keepalive; client sends nothing meaningful
    except WebSocketDisconnect:
        hub.disconnect(ws)
    except Exception:
        hub.disconnect(ws)


# --- Static frontend (single-container deployment) ---
import os
from fastapi.responses import FileResponse

_STATIC_DIR = os.environ.get(
    "DFIS_STATIC_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "static")),
)
if os.path.isdir(_STATIC_DIR):
    from fastapi.staticfiles import StaticFiles

    _index = os.path.join(_STATIC_DIR, "index.html")

    @app.get("/{full_path:path}", include_in_schema=False)
    async def spa(full_path: str):
        if full_path.startswith("api/") or full_path == "docs" or full_path.startswith("openapi"):
            from fastapi import HTTPException
            raise HTTPException(status_code=404)
        candidate = os.path.normpath(os.path.join(_STATIC_DIR, full_path))
        if full_path and os.path.isfile(candidate) and candidate.startswith(_STATIC_DIR):
            return FileResponse(candidate)
        return FileResponse(_index)
