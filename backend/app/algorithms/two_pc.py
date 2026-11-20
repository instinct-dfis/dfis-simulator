"""Two-Phase Commit across all online replicas.

Coordinator asks every online node to PREPARE (reserve funds + WAL), then decides
COMMIT or ABORT. Honest about 2PC's famous weakness: if the coordinator crashes
after PREPARE, participants stay blocked with locked funds until it recovers and
replays its WAL. Recovery is implemented for real via the coordinator_state WAL.
"""
from __future__ import annotations

import asyncio

from .base import ProtocolBase
from ..engine.apply import apply_committed

VOTE_TIMEOUT_S = 4.0
ACK_TIMEOUT_S = 4.0


class TwoPCProtocol(ProtocolBase):
    name = "two_pc"
    description = ("Atomic commit across replicas: all-or-nothing via PREPARE/COMMIT. "
                   "Strongly consistent among participants, but BLOCKS when the "
                   "coordinator crashes between the phases (funds stay reserved "
                   "until recovery).")

    def __init__(self) -> None:
        super().__init__()
        self.vote_futures: dict[str, asyncio.Future] = {}

    async def execute(self, cluster, tx) -> dict:
        async with self.lock:
            online = cluster.online_nodes()
            if len(online) < 2:
                return {"outcome": "failed", "error": "not enough online replicas", "detail": ""}
            coordinator = next((n for n in online if n.country == cluster.accounts[tx["from_account"]]["country"]), online[0])
            legs = self.legs_of(tx)
            if coordinator.ledger.has_applied(tx["tx_id"]):
                return {"outcome": "committed", "error": None, "detail": "duplicate suppressed (idempotent)"}
            state = {"phase": "preparing", "decision": None, "votes": {},
                     "participants": [n.id for n in online], "legs": legs, "acks": []}
            coordinator.twopc.coordinating[tx["tx_id"]] = state
            if self.repo:
                coordinator.wal_seq += 1
                await self.repo.write_wal(coordinator.id, coordinator.wal_seq,
                                          "coordinator_state", {"tx_id": tx["tx_id"], "state": state})
            cluster.bus.emit("2pc", f"Coordinator {coordinator.code} PREPARE for tx {tx['tx_id'][:8]} -> {len(online)} participants",
                             node_id=coordinator.id)
            fut = asyncio.get_running_loop().create_future()
            self.vote_futures[tx["tx_id"]] = fut
            for n in online:
                if n.id != coordinator.id:
                    cluster.send(coordinator, n, {"proto": "two_pc", "type": "PREPARE",
                                                  "tx_id": tx["tx_id"], "legs": legs,
                                                  "coordinator": coordinator.id})
            # coordinator votes locally
            vote, reason = self._prepare_local(cluster, coordinator, tx["tx_id"], legs, coordinator.id)
            if self.repo:
                coordinator.wal_seq += 1
                await self.repo.write_wal(coordinator.id, coordinator.wal_seq, "prepare",
                                          {"tx_id": tx["tx_id"], "legs": legs,
                                           "coordinator": coordinator.id})
            state["votes"][str(coordinator.id)] = vote
            self._check_votes(cluster, coordinator, tx["tx_id"])
        if await cluster.maybe_crash(coordinator, "after_prepare"):
            return {"outcome": "failed",
                    "error": "coordinator crashed after PREPARE (participants blocked until recovery)",
                    "detail": "Classic 2PC blocking: replicas hold reserved funds and wait for the coordinator's WAL recovery."}
        try:
            await asyncio.wait_for(fut, VOTE_TIMEOUT_S)
        except asyncio.TimeoutError:
            state["decision"] = "abort"
        decision = state["decision"] or "abort"
        return await self._finalize(cluster, coordinator, tx["tx_id"], decision,
                                    reason if decision == "abort" else None)

    def _prepare_local(self, cluster, node, tx_id: str, legs, coordinator_id: int) -> tuple[str, str]:
        if node.ledger.has_applied(tx_id):
            return "yes", "already applied"
        if tx_id in node.twopc.prepared:
            return "yes", "already prepared"  # idempotent re-PREPARE (recovery path)
        for account_id, delta in legs:
            if delta < 0:
                available = node.ledger.balance_of(account_id) - node.twopc.reserved.get(account_id, 0.0)
                if available < -delta:
                    return "no", f"insufficient funds on {node.code} (available {available})"
        for account_id, delta in legs:
            if delta < 0:
                node.twopc.reserved[account_id] = node.twopc.reserved.get(account_id, 0.0) + (-delta)
        node.twopc.prepared[tx_id] = {"legs": legs, "coordinator": coordinator_id}
        return "yes", ""

    def _check_votes(self, cluster, coordinator, tx_id: str) -> None:
        state = coordinator.twopc.coordinating.get(tx_id)
        if state is None or state["decision"] is not None:
            return
        votes = state["votes"]
        if "no" in votes.values():
            state["decision"] = "abort"
        elif all(str(p) in votes for p in state["participants"]):
            state["decision"] = "commit"
        if state["decision"] is not None:
            fut = self.vote_futures.get(tx_id)
            if fut and not fut.done():
                fut.set_result(True)

    async def _finalize(self, cluster, coordinator, tx_id: str, decision: str, reason: str | None) -> dict:
        state = coordinator.twopc.coordinating[tx_id]
        state["phase"] = "committing" if decision == "commit" else "aborting"
        state["decision"] = decision
        if self.repo:
            coordinator.wal_seq += 1
            await self.repo.write_wal(coordinator.id, coordinator.wal_seq, "coordinator_state",
                                      {"tx_id": tx_id, "state": state})
        if await cluster.maybe_crash(coordinator, "after_decision"):
            return {"outcome": "failed",
                    "error": f"coordinator crashed after deciding {decision.upper()}, before notifying participants",
                    "detail": "Recovery will re-send the recorded decision from the WAL."}
        msg_type = "COMMIT" if decision == "commit" else "ABORT"
        for n in cluster.online_nodes():
            if n.id != coordinator.id and n.id in state["participants"]:
                cluster.send(coordinator, n, {"proto": "two_pc", "type": msg_type, "tx_id": tx_id})
        if decision == "commit":
            legs = [tuple(x) for x in state["legs"]]
            for account_id, delta in legs:
                if delta < 0:
                    coordinator.twopc.reserved[account_id] = max(
                        0.0, coordinator.twopc.reserved.get(account_id, 0.0) + delta)
            coordinator.twopc.prepared.pop(tx_id, None)
            await apply_committed(cluster, self.repo, coordinator, tx_id, legs, "two_pc")
            cluster.bus.emit("2pc", f"tx {tx_id[:8]} COMMITTED on all participants", node_id=coordinator.id)
            state["phase"] = "done"
            return {"outcome": "committed", "error": None,
                    "detail": f"2PC commit across {len(state['participants'])} replicas"}
        else:
            coordinator.twopc.prepared.pop(tx_id, None)
            for account_id, delta in [tuple(x) for x in state["legs"]]:
                if delta < 0:
                    coordinator.twopc.reserved[account_id] = max(
                        0.0, coordinator.twopc.reserved.get(account_id, 0.0) + delta)
            cluster.bus.emit("2pc", f"tx {tx_id[:8]} ABORTED: {reason or 'vote no/timeout'}",
                             level="warn", node_id=coordinator.id)
            state["phase"] = "done"
            return {"outcome": "rejected", "error": reason or "a participant voted NO or timed out",
                    "detail": "2PC abort: nothing applied anywhere, reservations released"}

    async def on_message(self, cluster, node, msg: dict) -> None:
        t = msg["type"]
        if t == "PREPARE":
            tx_id = msg["tx_id"]
            legs = [tuple(x) for x in msg["legs"]]
            vote, reason = self._prepare_local(cluster, node, tx_id, legs, msg["coordinator"])
            if self.repo and vote == "yes" and reason != "already prepared":
                node.wal_seq += 1
                await self.repo.write_wal(node.id, node.wal_seq, "prepare",
                                          {"tx_id": tx_id, "legs": legs,
                                           "coordinator": msg["coordinator"]})
            coord = cluster.nodes[msg["coordinator"]]
            cluster.send(node, coord, {"proto": "two_pc", "type": "VOTE", "tx_id": tx_id,
                                       "vote": vote, "reason": reason, "voter": node.id})
        elif t == "VOTE":
            tx_id = msg["tx_id"]
            state = node.twopc.coordinating.get(tx_id)
            if state and state["decision"] is None:
                state["votes"][str(msg["voter"])] = msg["vote"]
                if self.repo:
                    node.wal_seq += 1
                    await self.repo.write_wal(node.id, node.wal_seq, "coordinator_state",
                                              {"tx_id": tx_id, "state": state})
                self._check_votes(cluster, node, tx_id)
        elif t in ("COMMIT", "ABORT"):
            tx_id = msg["tx_id"]
            prep = node.twopc.prepared.pop(tx_id, None)
            if prep:
                for account_id, delta in prep["legs"]:
                    if delta < 0:
                        node.twopc.reserved[account_id] = max(
                            0.0, node.twopc.reserved.get(account_id, 0.0) + delta)
            if t == "COMMIT":
                legs = prep["legs"] if prep else None
                if legs:
                    await apply_committed(cluster, self.repo, node, tx_id,
                                          [tuple(x) for x in legs], "two_pc")

    async def on_node_up(self, cluster, node) -> None:
        """Crash recovery: resume every in-doubt transaction from the WAL."""
        for tx_id, state in list(node.twopc.coordinating.items()):
            if state.get("phase") == "done":
                continue
            cluster.bus.emit("2pc", f"{node.code} recovering in-doubt tx {tx_id[:8]} from WAL",
                             level="warn", node_id=node.id)
            if state.get("decision"):
                await self._finalize(cluster, node, tx_id, state["decision"], None)
            else:
                # decision lost with RAM: re-prepare participants (idempotent) and re-decide
                for pid in state["participants"]:
                    peer = cluster.nodes[pid]
                    if peer.id != node.id and peer.is_online():
                        cluster.send(node, peer, {"proto": "two_pc", "type": "PREPARE",
                                                  "tx_id": tx_id, "legs": state["legs"],
                                                  "coordinator": node.id})
                # the coordinator must re-cast its own vote too (its prepared
                # state survived in the WAL, so this is idempotent)
                vote, _reason = self._prepare_local(cluster, node, tx_id,
                                                    [tuple(x) for x in state["legs"]], node.id)
                state["votes"][str(node.id)] = vote
                self._check_votes(cluster, node, tx_id)
                fut = asyncio.get_running_loop().create_future()
                self.vote_futures[tx_id] = fut
                try:
                    await asyncio.wait_for(fut, VOTE_TIMEOUT_S)
                except asyncio.TimeoutError:
                    state["decision"] = "abort"
                await self._finalize(cluster, node, tx_id, state["decision"] or "abort", "recovered re-vote")
