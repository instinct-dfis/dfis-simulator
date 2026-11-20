"""Event sourcing: the append-only event log is the source of truth; balances are projections.

A single sequencer (lowest-id online node) assigns a global sequence to each event.
Events replicate asynchronously; lagging replicas catch up by requesting missing
events. Recovery replays the durable event log and rebuilds the projection, which
is exactly how real event-sourced systems restore state after a crash.
"""
from __future__ import annotations

import asyncio

from .base import ProtocolBase
from ..engine.apply import apply_committed


class EventSourcingProtocol(ProtocolBase):
    name = "event_sourcing"
    description = ("Append-only replicated event log; balances are derived projections. "
                   "Crash recovery = replay the log. Concurrency is optimistic: the "
                   "sequencer validates against its current projection.")

    async def _sequencer(self, cluster):
        online = cluster.online_nodes()
        if not online:
            return None
        seq_node = min(online, key=lambda n: n.id)
        # adopt the longest known log before sequencing (leader catch-up)
        best = max(online, key=lambda n: n.es.seq)
        if best.es.seq > seq_node.es.seq:
            for ev in best.es.events:
                if ev["seq"] > seq_node.es.seq:
                    await apply_committed(cluster, self.repo, seq_node, ev["tx_id"],
                                          [tuple(x) for x in ev["legs"]], "event_sourcing")
                    seq_node.es.seq = max(seq_node.es.seq, ev["seq"])
        return seq_node

    async def execute(self, cluster, tx) -> dict:
        async with self.lock:
            seq_node = await self._sequencer(cluster)
            if seq_node is None:
                return {"outcome": "failed", "error": "no online node", "detail": ""}
            if seq_node.ledger.has_applied(tx["tx_id"]) or any(
                    e["tx_id"] == tx["tx_id"] for e in seq_node.es.events):
                return {"outcome": "committed", "error": None, "detail": "duplicate suppressed (idempotent)"}
            legs = self.legs_of(tx)
            if seq_node.ledger.balance_of(tx["from_account"]) < tx["amount"]:
                return {"outcome": "rejected", "error": "insufficient funds (projection)",
                        "detail": f"sequencer {seq_node.code} projection balance "
                                  f"{seq_node.ledger.balance_of(tx['from_account'])}"}
            await apply_committed(cluster, self.repo, seq_node, tx["tx_id"], legs, "event_sourcing")
            event = seq_node.es.events[-1]
            cluster.bus.emit("event_sourcing",
                             f"event #{event['seq']} appended by sequencer {seq_node.code} for tx {tx['tx_id'][:8]}",
                             node_id=seq_node.id)
            for n in cluster.online_nodes():
                if n.id != seq_node.id:
                    cluster.send(seq_node, n, {"proto": "event_sourcing", "type": "ES_EVENT", "event": event})
            return {"outcome": "committed", "error": None,
                    "detail": f"event #{event['seq']} durably appended; replicas project asynchronously"}

    async def on_message(self, cluster, node, msg: dict) -> None:
        t = msg["type"]
        if t == "ES_EVENT":
            ev = msg["event"]
            if ev["seq"] <= node.es.seq:
                return  # duplicate delivery
            if ev["seq"] > node.es.seq + 1:
                src = cluster.nodes[msg["_from"]]
                cluster.send(node, src, {"proto": "event_sourcing", "type": "ES_CATCHUP_REQ",
                                         "have_seq": node.es.seq, "from_node": node.id})
                return
            await apply_committed(cluster, self.repo, node, ev["tx_id"],
                                  [tuple(x) for x in ev["legs"]], "event_sourcing")
        elif t == "ES_CATCHUP_REQ":
            requester = cluster.nodes[msg["from_node"]]
            missing = [e for e in node.es.events if e["seq"] > msg["have_seq"]]
            cluster.send(node, requester, {"proto": "event_sourcing", "type": "ES_CATCHUP",
                                           "events": missing[:500]})
        elif t == "ES_CATCHUP":
            for ev in sorted(msg["events"], key=lambda e: e["seq"]):
                if ev["seq"] == node.es.seq + 1:
                    await apply_committed(cluster, self.repo, node, ev["tx_id"],
                                          [tuple(x) for x in ev["legs"]], "event_sourcing")

    async def on_node_up(self, cluster, node) -> None:
        # After replaying its durable log, ask peers for anything newer.
        peers = [n for n in cluster.online_nodes() if n.id != node.id]
        if peers:
            best = max(peers, key=lambda n: n.es.seq)
            if best.es.seq > node.es.seq:
                cluster.send(node, best, {"proto": "event_sourcing", "type": "ES_CATCHUP_REQ",
                                          "have_seq": node.es.seq, "from_node": node.id})
                cluster.bus.emit("event_sourcing",
                                 f"{node.code} catching up from {best.code} (has seq {node.es.seq}, needs {best.es.seq})",
                                 node_id=node.id)
