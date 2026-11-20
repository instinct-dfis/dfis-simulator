"""REST API. Thin layer over the live cluster; every button in the UI maps here."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from ..engine.scenarios import ScenarioRunner

router = APIRouter(prefix="/api")


def ctx(request: Request):
    s = request.app.state
    return s.cluster, s.repo, s.engine, s.faults, s.runner


# ---------- cluster -----------------------------------------------------------
@router.get("/cluster")
async def get_cluster(request: Request):
    cluster, *_ = ctx(request)
    return cluster.snapshot()


class ModeBody(BaseModel):
    mode: str

@router.post("/cluster/mode")
async def set_mode(body: ModeBody, request: Request):
    cluster, *_ = ctx(request)
    if body.mode not in cluster.protocols:
        raise HTTPException(400, f"unknown mode {body.mode}")
    cluster.mode = body.mode
    cluster.bus.emit("cluster", f"Consistency mode switched to {body.mode}")
    return {"mode": cluster.mode}


@router.get("/modes")
async def get_modes(request: Request):
    cluster, *_ = ctx(request)
    return {"current": cluster.mode,
            "modes": [{"id": name, "description": p.description}
                      for name, p in cluster.protocols.items()]}


@router.get("/accounts")
async def get_accounts(request: Request):
    cluster, *_ = ctx(request)
    ref = cluster.online_nodes()[0] if cluster.online_nodes() else cluster.nodes[0]
    return {"accounts": [{"id": aid, "country": meta["country"],
                          "balance": ref.ledger.balance_of(aid)}
                         for aid, meta in cluster.accounts.items()],
            "reference_node": ref.code,
            "total_supply": cluster.total_supply}


@router.get("/consistency")
async def get_consistency(request: Request):
    cluster, *_ = ctx(request)
    return {"consistency": cluster.consistency_report(),
            "integrity": cluster.integrity_report()}


# ---------- faults -------------------------------------------------------------
@router.post("/faults/node/{node_id}/stop")
async def stop_node(node_id: int, request: Request):
    _, _, _, faults, _ = ctx(request)
    if not 0 <= node_id < 50:
        raise HTTPException(404, "node not found")
    return await faults.stop_node(node_id)


@router.post("/faults/node/{node_id}/crash")
async def crash_node(node_id: int, request: Request):
    _, _, _, faults, _ = ctx(request)
    return await faults.stop_node(node_id, crashed=True)


@router.post("/faults/node/{node_id}/start")
async def start_node(node_id: int, request: Request):
    _, _, _, faults, _ = ctx(request)
    return await faults.start_node(node_id)


class LinkBody(BaseModel):
    a: str
    b: str

@router.post("/faults/link/cut")
async def cut_link(body: LinkBody, request: Request):
    _, _, _, faults, _ = ctx(request)
    return faults.cut_link(body.a, body.b)

@router.post("/faults/link/restore")
async def restore_link(body: LinkBody, request: Request):
    _, _, _, faults, _ = ctx(request)
    return faults.restore_link(body.a, body.b)


class NetBody(BaseModel):
    value: float = Field(ge=0)
    a: str | None = None
    b: str | None = None

@router.post("/faults/latency")
async def set_latency(body: NetBody, request: Request):
    _, _, _, faults, _ = ctx(request)
    return faults.set_latency(body.value, body.a, body.b)

@router.post("/faults/loss")
async def set_loss(body: NetBody, request: Request):
    _, _, _, faults, _ = ctx(request)
    if not 0 <= body.value <= 1:
        raise HTTPException(400, "rate must be 0..1")
    return faults.set_loss(body.value, body.a, body.b)

@router.post("/faults/duplication")
async def set_dup(body: NetBody, request: Request):
    _, _, _, faults, _ = ctx(request)
    if not 0 <= body.value <= 1:
        raise HTTPException(400, "rate must be 0..1")
    return faults.set_dup(body.value, body.a, body.b)


class SkewBody(BaseModel):
    node_id: int
    offset_ms: float

@router.post("/faults/clock-skew")
async def set_skew(body: SkewBody, request: Request):
    _, _, _, faults, _ = ctx(request)
    return faults.set_clock_skew(body.node_id, body.offset_ms)


class CrashArmBody(BaseModel):
    node_id: int
    when: str

@router.post("/faults/arm-crash")
async def arm_crash(body: CrashArmBody, request: Request):
    _, _, _, faults, _ = ctx(request)
    return faults.arm_crash(body.node_id, body.when)


@router.post("/faults/reset")
async def reset_faults(request: Request):
    _, _, _, faults, _ = ctx(request)
    return await faults.reset_all()


# ---------- transactions ---------------------------------------------------------
class TxBody(BaseModel):
    from_account: str
    to_account: str
    amount: float = Field(gt=0)
    tx_id: str | None = None
    mode: str | None = None

@router.post("/transactions")
async def submit_tx(body: TxBody, request: Request):
    _, _, engine, _, _ = ctx(request)
    return await engine.submit(body.from_account, body.to_account, body.amount,
                               tx_id=body.tx_id, mode=body.mode)


class GenBody(BaseModel):
    count: int = Field(default=20, ge=1, le=200)
    max_amount: float = 500.0
    concurrency: int = 10

@router.post("/transactions/generate")
async def generate_txs(body: GenBody, request: Request):
    _, _, engine, _, _ = ctx(request)
    result = await engine.generate(body.count, body.max_amount, body.concurrency)
    result.pop("results", None)
    return result


@router.get("/transactions")
async def list_txs(request: Request, limit: int = 50):
    _, repo, *_ = ctx(request)
    rows = await repo.recent_txs(min(limit, 200))
    counts = await repo.tx_counts()
    return {"transactions": [{"tx_id": r.tx_id, "from": r.from_account, "to": r.to_account,
                              "amount": r.amount, "mode": r.mode, "status": r.status,
                              "error": r.error, "sim_latency_ms": r.sim_latency_ms,
                              "created_at": r.created_at.isoformat() if r.created_at else None}
                             for r in rows],
            "counts": counts}


# ---------- scenarios -------------------------------------------------------------
class RunBody(BaseModel):
    key: str | None = None            # predefined scenario key
    steps: list[dict] | None = None   # custom steps
    name: str | None = None
    mode: str | None = None

@router.get("/scenarios")
async def list_scenarios(request: Request):
    _, _, _, _, runner = ctx(request)
    return {"scenarios": [{"key": s["key"], "name": s["name"],
                           "description": s["description"], "steps": s["steps"]}
                          for s in runner.predefined()]}


@router.post("/scenarios/run")
async def run_scenario(body: RunBody, request: Request):
    _, _, _, _, runner = ctx(request)
    if body.key:
        sc = next((s for s in runner.predefined() if s["key"] == body.key), None)
        if sc is None:
            raise HTTPException(404, "unknown scenario")
        return await runner.run(sc["name"], sc["steps"], custom=False, mode=body.mode)
    if body.steps:
        return await runner.run(body.name or "custom scenario", body.steps,
                                custom=True, mode=body.mode)
    raise HTTPException(400, "provide key or steps")


@router.get("/runs")
async def list_runs(request: Request, limit: int = 20):
    _, repo, *_ = ctx(request)
    rows = await repo.runs(min(limit, 100))
    return {"runs": [{"id": r.id, "name": r.name, "mode": r.mode, "custom": r.custom,
                      "status": r.status,
                      "started_at": r.started_at.isoformat() if r.started_at else None,
                      "passed": (r.result or {}).get("passed"),
                      "metrics": (r.result or {}).get("metrics")} for r in rows]}


@router.get("/runs/{run_id}")
async def get_run(run_id: int, request: Request):
    _, repo, *_ = ctx(request)
    row = await repo.get_run(run_id)
    if row is None:
        raise HTTPException(404, "run not found")
    return row.result or {"status": row.status}


# ---------- reports ------------------------------------------------------------------
@router.get("/reports/summary")
async def report_summary(request: Request):
    _, repo, *_ = ctx(request)
    cluster = request.app.state.cluster
    summary = await repo.metrics_summary()
    return {"by_mode": summary,
            "descriptions": {name: p.description for name, p in cluster.protocols.items()},
            "note": "كل الأرقام مقاسة من تشغيلات فعلية مسجلة في قاعدة البيانات، وليست قيمًا ثابتة."}


@router.get("/reports/export")
async def report_export(request: Request, format: str = "json"):
    _, repo, *_ = ctx(request)
    rows = await repo.runs(100)
    data = [{"run_id": r.id, "name": r.name, "mode": r.mode,
             "started_at": r.started_at.isoformat() if r.started_at else None,
             "metrics": (r.result or {}).get("metrics"),
             "passed": (r.result or {}).get("passed"),
             "errors": (r.result or {}).get("errors")} for r in rows]
    if format == "csv":
        import io
        buf = io.StringIO()
        buf.write("run_id,name,mode,availability,throughput,latency_avg,latency_p95,consistency,integrity,passed\n")
        for d in data:
            m = d.get("metrics") or {}
            buf.write(f"{d['run_id']},\"{d['name']}\",{d['mode']},{m.get('availability','')},"
                      f"{m.get('throughput','')},{m.get('latency_avg','')},{m.get('latency_p95','')},"
                      f"{m.get('consistency','')},{m.get('integrity','')},{d.get('passed')}\n")
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(buf.getvalue(), media_type="text/csv",
                                 headers={"Content-Disposition": "attachment; filename=dfis-report.csv"})
    return {"runs": data}


# ---------- events ---------------------------------------------------------------------
@router.get("/events")
async def list_events(request: Request, limit: int = 100, category: str | None = None):
    _, repo, *_ = ctx(request)
    rows = await repo.recent_events(min(limit, 500))
    out = [{"id": r.id, "ts": r.ts.isoformat() if r.ts else None, "level": r.level,
            "category": r.category, "node_id": r.node_id, "message": r.message, "data": r.data}
           for r in rows if category is None or r.category == category]
    return {"events": out}


# ---------- reset ------------------------------------------------------------------------
@router.post("/reset")
async def reset_all(request: Request):
    cluster, repo, *_ = ctx(request)
    s = request.app.state
    await s.faults.reset_all()
    for n in cluster.nodes:
        if not n.is_online():
            await cluster.start_node(n.id)
    await repo.reset_all()
    cluster.seed_replicas()
    cluster.bus.emit("cluster", "Simulation reset: all replicas reseeded, database cleared")
    return {"reset": True}
