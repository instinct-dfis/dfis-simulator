"""Strong consistency via synchronous full replication.

Every write is validated and applied on EVERY online replica before the client is
acknowledged. Any unreachable replica aborts the write. Consistency is absolute
among online nodes; availability collapses as soon as one node is down: that is
the documented CP tradeoff, visible live in the dashboard.
"""
from __future__ import annotations

import asyncio

from .base import ProtocolBase
from ..engine.apply import apply_committed

ACK_TIMEOUT_S = 5.0


class StrongProtocol(ProtocolBase):
    name = "strong"
    description = ("Synchronous replication to every online replica before ack. "
                   "Reads never diverge; writes fail entirely when any replica is "
                   "unreachable (consistency over availability).")

    def __init__(self) -> None:
        super().__init__()
        self.ack_futures: dict[str, asyncio.Future] = {}
        self.acks: dict[str, set[int]] = {}

    async def execute(self, cluster, tx) -> dict:
        async with self.lock:
            online = cluster.online_nodes()
            offline = [n for n in cluster.nodes if not n.is_online()]
            if offline:
                return {"outcome": "failed",
                        "error": f"strong mode requires ALL replicas online; {len(offline)} down "
                                 f"(e.g. {offline[0].code})",
                        "detail": "Availability sacrificed for consistency: no write may skip a replica."}
            gateway = next((n for n in online if n.country == cluster.accounts[tx["from_account"]]["country"]), online[0])
            if gateway.ledger.has_applied(tx["tx_id"]):
                return {"outcome": "committed", "error": None, "detail": "duplicate suppressed (idempotent)"}
            legs = self.legs_of(tx)
            if gateway.ledger.balance_of(tx["from_account"]) < tx["amount"]:
                return {"outcome": "rejected", "error": "insufficient funds",
                        "detail": "validated on a fully-consistent replica"}
            fut = asyncio.get_running_loop().create_future()
            self.ack_futures[tx["tx_id"]] = fut
            self.acks[tx["tx_id"]] = {gateway.id}
            for n in online:
                if n.id != gateway.id:
                    cluster.send(gateway, n, {"proto": "strong", "type": "APPLY_SYNC",
                                              "tx_id": tx["tx_id"], "legs": legs,
                                              "from_node": gateway.id})
            await apply_committed(cluster, self.repo, gateway, tx["tx_id"], legs, "strong")
            self._check_done(tx["tx_id"], len(online))
        try:
            await asyncio.wait_for(fut, ACK_TIMEOUT_S)
            return {"outcome": "committed", "error": None,
                    "detail": f"synchronously applied on all {len(online)} replicas"}
        except asyncio.TimeoutError:
            # roll back whoever applied (honest all-or-nothing)
            legs = self.legs_of(tx)
            for n in cluster.online_nodes():
                if n.ledger.revert(tx["tx_id"]):
                    cluster.bus.emit("strong", f"{n.code} rolled back tx {tx['tx_id'][:8]} (incomplete replication)",
                                     level="warn", node_id=n.id)
            return {"outcome": "failed", "error": "replication incomplete within timeout; rolled back everywhere",
                    "detail": "No replica keeps the write: atomicity preserved."}

    def _check_done(self, tx_id: str, needed: int) -> None:
        if len(self.acks.get(tx_id, set())) >= needed:
            fut = self.ack_futures.get(tx_id)
            if fut and not fut.done():
                fut.set_result(True)

    async def on_message(self, cluster, node, msg: dict) -> None:
        if msg["type"] == "APPLY_SYNC":
            ok = False
            if node.ledger.has_applied(msg["tx_id"]):
                ok = True
            else:
                legs = [tuple(x) for x in msg["legs"]]
                if all(node.ledger.balance_of(a) >= -d for a, d in legs if d < 0):
                    ok = await apply_committed(cluster, self.repo, node, msg["tx_id"], legs, "strong")
            src = cluster.nodes[msg["from_node"]]
            cluster.send(node, src, {"proto": "strong", "type": "ACK",
                                     "tx_id": msg["tx_id"], "ok": ok, "from_node": node.id})
        elif msg["type"] == "ACK":
            if msg["ok"]:
                self.acks.setdefault(msg["tx_id"], set()).add(msg["from_node"])
                self._check_done(msg["tx_id"], len(cluster.online_nodes()))
