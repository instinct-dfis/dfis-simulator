"""Raft consensus over the whole 50-node cluster (one replicated log, majority = 26).

Real implementation: elections with randomized timeouts, heartbeats, AppendEntries
with prev-log consistency checks, commit at majority, no-op entry on leadership
change so prior-term entries become committable, step-down on higher term.
All messages travel through SimulatedNetwork -> latency/loss/duplication/partitions
genuinely affect elections and replication.
"""
from __future__ import annotations

import asyncio

from .base import ProtocolBase
from ..engine.apply import apply_committed

HEARTBEAT_MS = 120.0
ELECTION_MIN_MS = 300.0
ELECTION_MAX_MS = 700.0
EXECUTE_TIMEOUT_S = 6.0


class RaftProtocol(ProtocolBase):
    name = "raft"
    description = ("Leader-based replicated log. Writes serialize through the leader and "
                   "commit only after a majority of the 50 nodes persist them. Consistent "
                   "under partitions (minority side cannot commit), but unavailable while "
                   "no leader exists.")

    def __init__(self) -> None:
        super().__init__()
        self.pending: dict[str, asyncio.Future] = {}   # tx_id -> future resolved at commit
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}
        self.last_heartbeat_sent: float = 0.0
        self.leader_id: int | None = None

    # ---- client path ------------------------------------------------------
    async def execute(self, cluster, tx) -> dict:
        async with self.lock:
            leader = await self._await_leader(cluster)
            if leader is None:
                return {"outcome": "failed", "error": "no leader (election in progress or majority unavailable)",
                        "detail": "Raft sacrifices availability when a majority cannot be reached."}
            if leader.ledger.has_applied(tx["tx_id"]) or any(
                    e["command"]["tx_id"] == tx["tx_id"] for e in leader.raft.log):
                return {"outcome": "committed", "error": None, "detail": "duplicate suppressed (idempotent)"}
            legs = self.legs_of(tx)
            # double-spend guard: leader ledger + still-uncommitted log effects
            effective = leader.ledger.balance_of(tx["from_account"])
            for e in leader.raft.log:
                for a, d in e["command"]["legs"]:
                    if a == tx["from_account"]:
                        effective += d
            if effective < tx["amount"]:
                return {"outcome": "rejected", "error": "insufficient funds",
                        "detail": f"leader effective balance {effective}"}
            entry = {"term": leader.raft.term, "command": {"tx_id": tx["tx_id"], "legs": legs}}
            leader.raft.log.append(entry)
            idx = len(leader.raft.log) - 1
            self.match_index[leader.id] = idx
            fut = asyncio.get_running_loop().create_future()
            self.pending[tx["tx_id"]] = fut
            self._send_append_entries(cluster, leader)
            cluster.bus.emit("raft", f"Leader {leader.code} appended tx {tx['tx_id'][:8]} at index {idx}, replicating",
                             node_id=leader.id)
        if await cluster.maybe_crash(leader, "after_append_before_commit"):
            return {"outcome": "failed", "error": "leader crashed mid-transaction (after append, before commit)",
                    "detail": "A new leader with this entry in its log will commit it after election."}
        try:
            await asyncio.wait_for(fut, EXECUTE_TIMEOUT_S)
            return {"outcome": "committed", "error": None, "detail": f"committed at log index {idx} by majority"}
        except asyncio.TimeoutError:
            return {"outcome": "failed",
                    "error": "replication timeout (majority not reached; entry may commit later)",
                    "detail": "Idempotency key makes a client retry safe."}

    async def _await_leader(self, cluster):
        for _ in range(60):
            if self.leader_id is not None:
                n = cluster.nodes[self.leader_id]
                if n.is_online() and n.raft.role == "leader":
                    return n
            await asyncio.sleep(0.05)
        return None

    # ---- timers -------------------------------------------------------------
    def on_tick(self, cluster, node, now_ms: float) -> None:
        r = node.raft
        if r.election_timeout_ms == 0.0:
            r.election_timeout_ms = cluster.rng.uniform(ELECTION_MIN_MS, ELECTION_MAX_MS)
            r.last_heartbeat_ms = now_ms
        if r.role == "leader":
            if now_ms - self.last_heartbeat_sent >= HEARTBEAT_MS:
                self.last_heartbeat_sent = now_ms
                self._send_append_entries(cluster, node)
        elif now_ms - r.last_heartbeat_ms > r.election_timeout_ms:
            self._start_election(cluster, node, now_ms)

    def _start_election(self, cluster, node, now_ms: float) -> None:
        r = node.raft
        r.role = "candidate"
        r.term += 1
        r.voted_for = node.id
        r.votes = {node.id}
        r.last_heartbeat_ms = now_ms
        r.election_timeout_ms = cluster.rng.uniform(ELECTION_MIN_MS, ELECTION_MAX_MS)
        cluster.bus.emit("raft", f"{node.code} starts election for term {r.term}", node_id=node.id)
        last_term = r.log[-1]["term"] if r.log else 0
        for peer in cluster.online_nodes():
            if peer.id == node.id:
                continue
            cluster.send(node, peer, {"proto": "raft", "type": "RequestVote",
                                      "term": r.term, "candidate_id": node.id,
                                      "last_log_index": len(r.log) - 1, "last_log_term": last_term})
        self._maybe_win(cluster, node)

    def _maybe_win(self, cluster, node) -> None:
        r = node.raft
        majority = len(cluster.nodes) // 2 + 1
        if r.role == "candidate" and len(r.votes) >= majority:
            r.role = "leader"
            r.leader_id = node.id
            self.leader_id = node.id
            self.next_index = {n.id: len(r.log) for n in cluster.nodes}
            self.match_index = {n.id: -1 for n in cluster.nodes}
            self.match_index[node.id] = len(r.log) - 1
            # no-op entry of the current term so prior-term entries can be committed
            r.log.append({"term": r.term, "command": {"tx_id": f"noop-{node.id}-{r.term}", "legs": []}})
            self.match_index[node.id] = len(r.log) - 1
            cluster.bus.emit("raft", f"{node.code} elected LEADER for term {r.term} with {len(r.votes)} votes",
                             node_id=node.id)
            self._send_append_entries(cluster, node)

    def _send_append_entries(self, cluster, leader) -> None:
        r = leader.raft
        for peer in cluster.online_nodes():
            if peer.id == leader.id:
                continue
            ni = self.next_index.get(peer.id, len(r.log))
            prev_index = ni - 1
            prev_term = r.log[prev_index]["term"] if prev_index >= 0 and prev_index < len(r.log) else 0
            entries = r.log[ni:]
            cluster.send(leader, peer, {"proto": "raft", "type": "AppendEntries",
                                        "term": r.term, "leader_id": leader.id,
                                        "prev_index": prev_index, "prev_term": prev_term,
                                        "entries": entries, "commit_index": r.commit_index})

    # ---- messages -------------------------------------------------------------
    async def on_message(self, cluster, node, msg: dict) -> None:
        t = msg["type"]
        r = node.raft
        if t == "RequestVote":
            if msg["term"] > r.term:
                r.term, r.voted_for, r.role = msg["term"], None, "follower"
            grant = False
            if msg["term"] >= r.term and r.voted_for in (None, msg["candidate_id"]):
                my_last_term = r.log[-1]["term"] if r.log else 0
                up_to_date = (msg["last_log_term"] > my_last_term or
                              (msg["last_log_term"] == my_last_term and
                               msg["last_log_index"] >= len(r.log) - 1))
                if up_to_date:
                    grant = True
                    r.voted_for = msg["candidate_id"]
                    r.last_heartbeat_ms = cluster.network.sim_now_ms()
            src = cluster.nodes[msg["candidate_id"]]
            cluster.send(node, src, {"proto": "raft", "type": "RequestVoteReply",
                                     "term": r.term, "granted": grant, "voter": node.id})

        elif t == "RequestVoteReply":
            if r.role == "candidate" and msg["term"] == r.term and msg["granted"]:
                r.votes.add(msg["voter"])
                self._maybe_win(cluster, node)
            elif msg["term"] > r.term:
                r.term, r.role, r.voted_for = msg["term"], "follower", None

        elif t == "AppendEntries":
            if msg["term"] < r.term:
                leader = cluster.nodes[msg["leader_id"]]
                cluster.send(node, leader, {"proto": "raft", "type": "AppendEntriesReply",
                                            "term": r.term, "granted": False,
                                            "match_index": len(r.log) - 1, "follower": node.id})
                return
            r.term = msg["term"]
            r.role = "follower"
            r.leader_id = msg["leader_id"]
            self.leader_id = msg["leader_id"]
            r.last_heartbeat_ms = cluster.network.sim_now_ms()
            r.election_timeout_ms = cluster.rng.uniform(ELECTION_MIN_MS, ELECTION_MAX_MS)
            ok = True
            pi, pt = msg["prev_index"], msg["prev_term"]
            if pi >= 0:
                if len(r.log) <= pi or r.log[pi]["term"] != pt:
                    ok = False
            if ok:
                idx = pi + 1
                for e in msg["entries"]:
                    if idx < len(r.log) and r.log[idx]["term"] != e["term"]:
                        del r.log[idx:]
                    if idx == len(r.log):
                        r.log.append(e)
                    idx += 1
                if msg["commit_index"] > r.commit_index:
                    new_commit = min(msg["commit_index"], len(r.log) - 1)
                    for i in range(r.commit_index + 1, new_commit + 1):
                        cmd = r.log[i]["command"]
                        if cmd["legs"]:
                            await apply_committed(cluster, self.repo, node, cmd["tx_id"],
                                                  [tuple(x) for x in cmd["legs"]], "raft")
                    r.commit_index = new_commit
            leader = cluster.nodes[msg["leader_id"]]
            cluster.send(node, leader, {"proto": "raft", "type": "AppendEntriesReply",
                                        "term": r.term, "granted": ok,
                                        "match_index": len(r.log) - 1, "follower": node.id})

        elif t == "AppendEntriesReply":
            if r.role != "leader":
                return
            if msg["term"] > r.term:
                r.term, r.role, r.voted_for = msg["term"], "follower", None
                if self.leader_id == node.id:
                    self.leader_id = None
                cluster.bus.emit("raft", f"{node.code} steps down (saw term {msg['term']})",
                                 level="warn", node_id=node.id)
                return
            f = msg["follower"]
            if msg["granted"]:
                self.match_index[f] = msg["match_index"]
                self.next_index[f] = msg["match_index"] + 1
                await self._advance_commit(cluster, node)
            else:
                self.next_index[f] = max(0, self.next_index.get(f, len(r.log)) - 1)
                self._send_append_entries(cluster, node)

    async def _advance_commit(self, cluster, leader) -> None:
        r = leader.raft
        majority = len(cluster.nodes) // 2 + 1
        for idx in range(r.commit_index + 1, len(r.log)):
            if r.log[idx]["term"] != r.term:
                continue  # prior-term entries commit indirectly with a current-term one
            replicated = sum(1 for m in self.match_index.values() if m >= idx)
            if replicated >= majority:
                # commit everything up to idx, in order (including prior-term entries)
                for i in range(r.commit_index + 1, idx + 1):
                    cmd = r.log[i]["command"]
                    if cmd["legs"]:
                        await apply_committed(cluster, self.repo, leader, cmd["tx_id"],
                                              [tuple(x) for x in cmd["legs"]], "raft")
                        fut = self.pending.pop(cmd["tx_id"], None)
                        if fut and not fut.done():
                            fut.set_result(True)
                        cluster.bus.emit("raft", f"tx {cmd['tx_id'][:8]} committed by majority at index {i}",
                                         node_id=leader.id)
                r.commit_index = idx

    async def on_node_down(self, cluster, node) -> None:
        if self.leader_id == node.id:
            self.leader_id = None
            cluster.bus.emit("raft", f"Leader {node.code} lost; waiting for new election",
                             level="warn", node_id=node.id)
