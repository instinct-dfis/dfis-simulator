"""Eventual consistency: local commit + asynchronous gossip + anti-entropy.

Maximizes availability (a node commits even when totally isolated) at the cost of
temporary divergence. Double spending CANNOT be prevented under partition, so the
protocol detects it at merge time and compensates (revert). That tradeoff is the
point of the comparison, and it is real: run two spends of the same money on two
sides of a cut link and watch the conflict resolution fire.
"""
from __future__ import annotations

from .base import ProtocolBase
from ..engine.apply import apply_committed

GOSSIP_DIGEST_EVERY_TICKS = 150  # ~1.5s at 10ms ticks


class EventualProtocol(ProtocolBase):
    name = "eventual"
    description = ("Commit locally first, reconcile later via gossip/anti-entropy. "
                   "Highest availability and lowest latency; replicas diverge under "
                   "partition and conflicting spends are detected and compensated "
                   "at merge time, not prevented.")

    def __init__(self) -> None:
        super().__init__()
        self._tick_count: dict[int, int] = {}

    async def execute(self, cluster, tx) -> dict:
        async with self.lock:
            online = cluster.online_nodes()
            if not online:
                return {"outcome": "failed", "error": "no online node", "detail": ""}
            gateway = None
            forced = tx.get("gateway_node")
            if forced is not None and cluster.nodes[forced].is_online():
                gateway = cluster.nodes[forced]  # simulates a client attached to a specific site
            if gateway is None:
                gateway = next((n for n in online if n.country == cluster.accounts[tx["from_account"]]["country"]), online[0])
            if gateway.ledger.has_applied(tx["tx_id"]):
                return {"outcome": "committed", "error": None, "detail": "duplicate suppressed (idempotent)"}
            legs = self.legs_of(tx)
            if gateway.ledger.balance_of(tx["from_account"]) < tx["amount"]:
                return {"outcome": "rejected", "error": "insufficient funds (local view)",
                        "detail": f"{gateway.code} sees balance {gateway.ledger.balance_of(tx['from_account'])}"}
            ts = gateway.now_ms(cluster.network.sim_now_ms())
            await apply_committed(cluster, self.repo, gateway, tx["tx_id"], legs, "eventual")
            cluster.bus.emit("eventual", f"tx {tx['tx_id'][:8]} committed locally on {gateway.code}, gossiping",
                             node_id=gateway.id)
            for n in online:
                if n.id != gateway.id:
                    cluster.send(gateway, n, {"proto": "eventual", "type": "GOSSIP",
                                              "tx_id": tx["tx_id"], "legs": legs,
                                              "ts": ts, "origin": gateway.id})
            return {"outcome": "committed", "error": None,
                    "detail": f"committed immediately on {gateway.code}; replication is asynchronous"}

    async def on_message(self, cluster, node, msg: dict) -> None:
        t = msg["type"]
        if t == "GOSSIP":
            if node.ledger.has_applied(msg["tx_id"]) or msg["tx_id"] in node.reverted:
                return
            legs = [tuple(x) for x in msg["legs"]]
            await apply_committed(cluster, self.repo, node, msg["tx_id"], legs, "eventual")
            await self._detect_conflicts(cluster, node, legs)
        elif t == "REVERT":
            node.reverted.add(msg["tx_id"])
            if node.ledger.revert(msg["tx_id"]):
                cluster.bus.emit("eventual", f"{node.code} reverted tx {msg['tx_id'][:8]} (conflict loser)",
                                 level="warn", node_id=node.id)
        elif t == "DIGEST":
            have = set(msg["tx_ids"])
            missing = [e for e in node.ledger.entries if e.tx_id not in have]
            by_tx: dict[str, list] = {}
            for e in missing[-200:]:
                by_tx.setdefault(e.tx_id, []).append((e.account_id, e.delta))
            if by_tx:
                src = cluster.nodes[msg["from"]]
                for tx_id, legs in by_tx.items():
                    cluster.send(node, src, {"proto": "eventual", "type": "GOSSIP",
                                             "tx_id": tx_id, "legs": legs,
                                             "ts": 0.0, "origin": node.id})

    async def _detect_conflicts(self, cluster, node, legs) -> None:
        """If applying pushed a debited account negative, one conflicting debit is
        compensated. The loser is chosen deterministically by tx_id so that EVERY
        replica converges on the same loser, no matter the message arrival order."""
        for account_id, delta in legs:
            if delta >= 0:
                continue
            if node.ledger.balance_of(account_id) < 0:
                # deterministic loser: max tx_id among conflicting debits
                debits = sorted({e.tx_id for e in node.ledger.entries
                                 if e.account_id == account_id and e.delta < 0})
                loser = debits[-1]
                node.reverted.add(loser)
                node.ledger.revert(loser)
                if self.repo:
                    await self.repo.update_tx(loser, "rejected",
                                              error="double-spend detected at merge; compensated")
                cluster.bus.emit("eventual",
                                 f"DOUBLE SPEND detected on {account_id}: tx {loser[:8]} reverted on {node.code}",
                                 level="error", node_id=node.id,
                                 data={"account": account_id, "reverted_tx": loser})
                for n in cluster.online_nodes():
                    if n.id != node.id:
                        cluster.send(node, n, {"proto": "eventual", "type": "REVERT", "tx_id": loser})

    def on_tick(self, cluster, node, now_ms: float) -> None:
        c = self._tick_count.get(node.id, 0) + 1
        self._tick_count[node.id] = c
        if c % GOSSIP_DIGEST_EVERY_TICKS == 0:
            peers = [n for n in cluster.online_nodes() if n.id != node.id]
            if peers:
                peer = cluster.rng.choice(peers)
                recent = [e.tx_id for e in node.ledger.entries[-50:]]
                cluster.send(node, peer, {"proto": "eventual", "type": "DIGEST",
                                          "tx_ids": recent, "from": node.id})
