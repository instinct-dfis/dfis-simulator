"""Transaction engine: validation, idempotency at the DB level, routing to the
active consistency protocol, latency measurement and honest status reporting."""
from __future__ import annotations

import asyncio
import time
import uuid

from ..config import settings


class TransactionEngine:
    def __init__(self, cluster, repo):
        self.cluster = cluster
        self.repo = repo

    async def submit(self, from_account: str, to_account: str, amount: float,
                     tx_id: str | None = None, mode: str | None = None,
                     gateway_node: int | None = None) -> dict:
        mode = mode or self.cluster.mode
        tx_id = tx_id or str(uuid.uuid4())
        # validation before touching any protocol
        if from_account not in self.cluster.accounts:
            return {"tx_id": tx_id, "status": "failed", "error": f"unknown account {from_account}", "duplicate": False}
        if to_account not in self.cluster.accounts:
            return {"tx_id": tx_id, "status": "failed", "error": f"unknown account {to_account}", "duplicate": False}
        if from_account == to_account:
            return {"tx_id": tx_id, "status": "failed", "error": "source and destination are the same", "duplicate": False}
        if amount <= 0:
            return {"tx_id": tx_id, "status": "failed", "error": "amount must be positive", "duplicate": False}

        row, created = await self.repo.insert_pending_tx(tx_id, from_account, to_account, amount, mode)
        if not created:
            # the idempotency key already exists: return the original outcome, never re-execute
            return {"tx_id": tx_id, "status": row.status, "duplicate": True,
                    "error": row.error, "detail": "idempotency key already processed; original result returned"}
        tx = {"tx_id": tx_id, "from_account": from_account, "to_account": to_account,
              "amount": amount, "mode": mode, "gateway_node": gateway_node}
        self.cluster.bus.emit("tx", f"tx {tx_id[:8]} submitted: {amount} {from_account} -> {to_account} [{mode}]",
                              data={"tx_id": tx_id, "status": "pending", "mode": mode})
        started = time.monotonic()
        protocol = self.cluster.protocols[mode]
        try:
            result = await protocol.execute(self.cluster, tx)
        except Exception as exc:  # a protocol bug must not silently lose money: mark failed loudly
            result = {"outcome": "failed", "error": f"protocol error: {exc}", "detail": ""}
            self.cluster.bus.emit("tx", f"tx {tx_id[:8]} protocol error: {exc}", level="error",
                                  data={"tx_id": tx_id, "status": "failed"})
        sim_latency = (time.monotonic() - started) * 1000.0 / settings.time_scale
        status = result["outcome"]
        await self.repo.update_tx(tx_id, status, result.get("error"), sim_latency)
        level = "info" if status == "committed" else ("warn" if status == "rejected" else "error")
        self.cluster.bus.emit("tx", f"tx {tx_id[:8]} {status.upper()} ({sim_latency:.0f} sim ms)"
                                    + (f": {result.get('error')}" if result.get("error") else ""),
                              level=level, data={"tx_id": tx_id, "status": status, "mode": mode})
        return {"tx_id": tx_id, "status": status, "duplicate": False,
                "error": result.get("error"), "detail": result.get("detail", ""),
                "sim_latency_ms": round(sim_latency, 1), "mode": mode}

    async def generate(self, count: int = 20, max_amount: float = 500.0,
                       concurrency: int = 10, mode: str | None = None) -> dict:
        """Load generator: random transfers between random accounts, real executions."""
        accounts = list(self.cluster.accounts)
        sem = asyncio.Semaphore(concurrency)
        results: list[dict] = []

        async def one() -> None:
            async with sem:
                a, b = self.cluster.rng.sample(accounts, 2)
                amount = round(self.cluster.rng.uniform(1, max_amount), 2)
                results.append(await self.submit(a, b, amount, mode=mode))

        started = time.monotonic()
        await asyncio.gather(*(one() for _ in range(count)))
        elapsed = time.monotonic() - started
        committed = sum(1 for r in results if r["status"] == "committed")
        return {"submitted": count, "committed": committed,
                "rejected": sum(1 for r in results if r["status"] == "rejected"),
                "failed": sum(1 for r in results if r["status"] == "failed"),
                "duplicates": sum(1 for r in results if r.get("duplicate")),
                "elapsed_s": round(elapsed, 2),
                "throughput_tx_per_s": round(count / elapsed, 2) if elapsed else 0.0,
                "results": results}
