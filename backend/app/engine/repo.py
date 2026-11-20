"""Persistence layer: every durable fact lives in the database (PostgreSQL in Docker,
SQLite locally). Simulated crashes lose RAM only; these rows are the 'disk' of each node."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import delete, desc, func, select, text
from sqlalchemy.exc import IntegrityError

from ..db import SessionLocal
from ..models import (Account, EventRow, LedgerEntryRow, MetricSampleRow,
                      ScenarioRunRow, Transaction, WalEntryRow)


class Repository:
    # ---- transactions ----------------------------------------------------
    async def insert_pending_tx(self, tx_id: str, from_a: str, to_a: str,
                                amount: float, mode: str):
        """Returns (row, created). created=False means the idempotency key already existed."""
        async with SessionLocal() as s:
            row = Transaction(tx_id=tx_id, from_account=from_a, to_account=to_a,
                              amount=amount, mode=mode, status="pending")
            s.add(row)
            try:
                await s.commit()
                return row, True
            except IntegrityError:
                await s.rollback()
        async with SessionLocal() as s:
            existing = (await s.execute(select(Transaction).where(Transaction.tx_id == tx_id))).scalar_one()
            return existing, False

    async def update_tx(self, tx_id: str, status: str, error: str | None = None,
                        sim_latency_ms: float | None = None) -> None:
        async with SessionLocal() as s:
            row = (await s.execute(select(Transaction).where(Transaction.tx_id == tx_id))).scalar_one_or_none()
            if row:
                row.status = status
                row.error = error
                if sim_latency_ms is not None:
                    row.sim_latency_ms = sim_latency_ms
                row.finished_at = datetime.now(timezone.utc)
                await s.commit()

    async def get_tx(self, tx_id: str):
        async with SessionLocal() as s:
            return (await s.execute(select(Transaction).where(Transaction.tx_id == tx_id))).scalar_one_or_none()

    async def recent_txs(self, limit: int = 100):
        async with SessionLocal() as s:
            rows = (await s.execute(select(Transaction).order_by(desc(Transaction.created_at)).limit(limit))).scalars().all()
            return rows

    async def tx_counts(self):
        async with SessionLocal() as s:
            rows = (await s.execute(select(Transaction.status, func.count()).group_by(Transaction.status))).all()
            return {status: count for status, count in rows}

    # ---- ledger replicas ---------------------------------------------------
    async def persist_ledger_rows(self, node_id: int, tx_id: str,
                                  legs: list[tuple[str, float]], mode: str,
                                  balances_after: dict[str, float]) -> None:
        async with SessionLocal() as s:
            max_seq = (await s.execute(select(func.max(LedgerEntryRow.seq)).where(LedgerEntryRow.node_id == node_id))).scalar() or 0
            for account_id, delta in legs:
                max_seq += 1
                s.add(LedgerEntryRow(node_id=node_id, seq=max_seq, tx_id=tx_id,
                                     account_id=account_id, delta=delta,
                                     balance_after=balances_after[account_id], mode=mode))
            await s.commit()

    async def ledger_rows_for_node(self, node_id: int):
        async with SessionLocal() as s:
            return (await s.execute(select(LedgerEntryRow).where(LedgerEntryRow.node_id == node_id)
                                    .order_by(LedgerEntryRow.seq))).scalars().all()

    # ---- WAL -----------------------------------------------------------------
    async def write_wal(self, node_id: int, seq: int, kind: str, payload: dict,
                        applied: bool = False) -> None:
        async with SessionLocal() as s:
            s.add(WalEntryRow(node_id=node_id, seq=seq, kind=kind, payload=payload, applied=applied))
            await s.commit()

    async def wal_rows(self, node_id: int):
        async with SessionLocal() as s:
            return (await s.execute(select(WalEntryRow).where(WalEntryRow.node_id == node_id)
                                    .order_by(WalEntryRow.seq))).scalars().all()

    async def replay_wal(self, cluster, node) -> None:
        """Rebuild in-RAM protocol state from the node's durable WAL after a crash/restart."""
        for row in await self.wal_rows(node.id):
            node.wal_seq = max(node.wal_seq, row.seq)
            p = row.payload
            if row.kind == "prepare" and not row.applied:
                node.twopc.prepared[p["tx_id"]] = {"legs": p["legs"], "coordinator": p["coordinator"]}
                for account_id, delta in p["legs"]:
                    if delta < 0:
                        node.twopc.reserved[account_id] = node.twopc.reserved.get(account_id, 0.0) + (-delta)
            elif row.kind == "coordinator_state":
                node.twopc.coordinating[p["tx_id"]] = p["state"]
            elif row.kind == "es_event":
                node.es.events.append(p["event"])
                node.es.seq = max(node.es.seq, p["event"]["seq"])

    # ---- events --------------------------------------------------------------
    async def log_event(self, event: dict) -> None:
        async with SessionLocal() as s:
            s.add(EventRow(level=event["level"], category=event["category"],
                           node_id=event.get("node_id"), message=event["message"],
                           data=event.get("data") or {}))
            await s.commit()

    async def recent_events(self, limit: int = 200):
        async with SessionLocal() as s:
            return (await s.execute(select(EventRow).order_by(desc(EventRow.id)).limit(limit))).scalars().all()

    # ---- scenario runs & metrics ---------------------------------------------
    async def create_run(self, name: str, mode: str, custom: bool, steps: list) -> int:
        async with SessionLocal() as s:
            row = ScenarioRunRow(name=name, mode=mode, custom=custom, steps=steps)
            s.add(row)
            await s.commit()
            return row.id

    async def finish_run(self, run_id: int, result: dict, status: str = "done") -> None:
        async with SessionLocal() as s:
            row = (await s.execute(select(ScenarioRunRow).where(ScenarioRunRow.id == run_id))).scalar_one()
            row.status = status
            row.result = result
            row.finished_at = datetime.now(timezone.utc)
            await s.commit()

    async def add_metric(self, run_id: int, mode: str, metric: str, value: float) -> None:
        async with SessionLocal() as s:
            s.add(MetricSampleRow(run_id=run_id, mode=mode, metric=metric, value=value))
            await s.commit()

    async def runs(self, limit: int = 50):
        async with SessionLocal() as s:
            return (await s.execute(select(ScenarioRunRow).order_by(desc(ScenarioRunRow.id)).limit(limit))).scalars().all()

    async def get_run(self, run_id: int):
        async with SessionLocal() as s:
            return (await s.execute(select(ScenarioRunRow).where(ScenarioRunRow.id == run_id))).scalar_one_or_none()

    async def metrics_summary(self):
        """Average of each metric per mode, over completed runs (real measured data only)."""
        async with SessionLocal() as s:
            rows = (await s.execute(
                select(MetricSampleRow.mode, MetricSampleRow.metric,
                       func.avg(MetricSampleRow.value), func.count())
                .group_by(MetricSampleRow.mode, MetricSampleRow.metric))).all()
            out: dict[str, dict] = {}
            for mode, metric, avg, count in rows:
                out.setdefault(mode, {})[metric] = {"avg": round(float(avg), 4), "samples": count}
            return out

    # ---- housekeeping -----------------------------------------------------------
    async def reset_all(self) -> None:
        async with SessionLocal() as s:
            for model in (LedgerEntryRow, WalEntryRow, Transaction, EventRow,
                          MetricSampleRow, ScenarioRunRow):
                await s.execute(delete(model))
            await s.commit()

    async def seed_accounts(self, accounts: dict[str, dict]) -> None:
        async with SessionLocal() as s:
            existing = (await s.execute(select(func.count(Account.id)))).scalar() or 0
            if existing == 0:
                for aid, meta in accounts.items():
                    s.add(Account(id=aid, country=meta["country"],
                                  initial_balance=meta["initial_balance"]))
                await s.commit()
