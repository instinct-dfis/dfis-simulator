"""The single funnel through which every committed leg reaches a replica + its durable store."""
from __future__ import annotations

from ..simulation.cluster import Cluster
from ..simulation.node import VirtualNode


async def apply_committed(cluster: Cluster, repo, node: VirtualNode, tx_id: str,
                          legs: list[tuple[str, float]], mode: str) -> bool:
    """Idempotent apply to the node's replica, persisted to its durable rows.

    Returns False when the tx was already applied (duplicate delivery / replay),
    which is exactly what protects the system from duplicated network messages.
    """
    if not node.is_online():
        return False
    applied = node.ledger.apply(tx_id, legs, mode, node.now_ms(0.0))
    if applied:
        if repo is not None:
            balances_after = {a: node.ledger.balance_of(a) for a, _ in legs}
            await repo.persist_ledger_rows(node.id, tx_id, legs, mode, balances_after)
            node.wal_seq += 1
            await repo.write_wal(node.id, node.wal_seq, "apply",
                                 {"tx_id": tx_id, "mode": mode}, applied=True)
        if mode == "event_sourcing":
            node.es.seq += 1
            event = {"seq": node.es.seq, "tx_id": tx_id, "legs": legs, "node": node.id}
            node.es.events.append(event)
            if repo is not None:
                node.wal_seq += 1
                await repo.write_wal(node.id, node.wal_seq, "es_event", {"event": event}, applied=True)
    return applied
