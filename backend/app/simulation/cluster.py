"""The simulated cluster: 50 virtual servers in 5 countries + the fault-injection surface."""
from __future__ import annotations

import asyncio
import random
from typing import Any

from ..config import settings
from .events import EventBus
from .ledger import ReplicaLedger
from .network import SimulatedNetwork
from .node import COUNTRY_CODES, COUNTRY_GEO, VirtualNode


class Cluster:
    def __init__(self, repo=None, seed: int | None = None):
        self.countries = list(settings.countries)
        self.network = SimulatedNetwork(self.countries, settings.time_scale,
                                        seed if seed is not None else settings.network_seed)
        self.bus = EventBus()
        self.rng = random.Random(seed if seed is not None else settings.network_seed)
        self.repo = repo  # persistence layer (app.engine.repo.Repository); None in pure unit tests

        per_country = settings.node_count // len(self.countries)
        self.nodes: list[VirtualNode] = [
            VirtualNode(id=i, country=self.countries[i // per_country])
            for i in range(settings.node_count)
        ]
        # Accounts are sharded by country -> cross-country transfers are genuinely distributed.
        self.accounts: dict[str, dict] = {}
        for c in self.countries:
            for i in range(settings.accounts_per_country):
                aid = f"{COUNTRY_CODES[c]}-{i:03d}"
                self.accounts[aid] = {"country": c, "initial_balance": settings.initial_balance}
        self.total_supply = round(len(self.accounts) * settings.initial_balance, 6)

        self.mode = "raft"  # raft|two_pc|eventual|strong|event_sourcing
        self.protocols: dict[str, Any] = {}
        self._tick_task: asyncio.Task | None = None
        self._running = False
        # crash hooks: node_id -> arming point (set by fault injector / scenarios)
        self.crash_hooks: dict[int, str] = {}
        self.network.set_deliverer(self._deliver)

    # ---- topology -------------------------------------------------------
    def online_nodes(self, country: str | None = None) -> list[VirtualNode]:
        return [n for n in self.nodes if n.is_online() and (country is None or n.country == country)]

    def nodes_of(self, country: str) -> list[VirtualNode]:
        return [n for n in self.nodes if n.country == country]

    def accounts_of(self, country: str) -> list[str]:
        return [a for a, meta in self.accounts.items() if meta["country"] == country]

    def seed_replicas(self) -> None:
        for n in self.nodes:
            for aid, meta in self.accounts.items():
                n.ledger.seed(aid, meta["initial_balance"])
            n.es.events.clear()

    # ---- messaging -------------------------------------------------------
    async def _deliver(self, node_id: int, msg: dict) -> None:
        node = self.nodes[node_id]
        if not node.is_online():
            return
        proto = self.protocols.get(msg.get("proto", ""))
        if proto is not None:
            await proto.on_message(self, node, msg)

    def send(self, src: VirtualNode, dst: VirtualNode, msg: dict) -> bool:
        msg = dict(msg)
        msg["_from"] = src.id
        return self.network.send((src.id, src.country), (dst.id, dst.country), msg)

    def broadcast_country(self, src: VirtualNode, country: str, msg: dict, include_self=False) -> int:
        dsts = [n for n in self.nodes_of(country) if n.is_online() and (include_self or n.id != src.id)]
        sent = 0
        for d in dsts:
            if self.send(src, d, msg):
                sent += 1
        if include_self and src.is_online():
            # loopback: deliver to own handler directly (no network involved)
            pass
        return sent

    # ---- lifecycle -------------------------------------------------------
    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self.bus.start()
        self._tick_task = asyncio.get_running_loop().create_task(self._tick_loop())
        self.bus.emit("cluster", f"Cluster started: {len(self.nodes)} nodes in {len(self.countries)} countries, mode={self.mode}")

    async def stop(self) -> None:
        self._running = False
        if self._tick_task:
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass
        await self.bus.stop()

    async def _tick_loop(self) -> None:
        # Drives Raft election/heartbeat timers and gossip anti-entropy.
        while self._running:
            await asyncio.sleep(0.01)  # 10ms real
            now = SimulatedNetwork.sim_now_ms()
            for node in self.nodes:
                if not node.is_online():
                    continue
                proto = self.protocols.get(self.mode)
                if proto is not None and hasattr(proto, "on_tick"):
                    try:
                        proto.on_tick(self, node, now)
                    except Exception as exc:  # a protocol bug must not kill the tick loop
                        self.bus.emit("error", f"tick error on {node.code}: {exc}", level="error", node_id=node.id)

    # ---- faults ------------------------------------------------------------
    async def stop_node(self, node_id: int, crashed: bool = False) -> None:
        node = self.nodes[node_id]
        if not node.is_online():
            return
        node.status = "crashed" if crashed else "offline"
        self.network.node_down(node_id, True)
        node.reset_volatile_state()  # RAM is gone; disk (DB replica + WAL) survives
        self.bus.emit("fault", f"Node {node.code} {'CRASHED' if crashed else 'stopped'}",
                      level="warn", node_id=node_id)
        proto = self.protocols.get(self.mode)
        if proto is not None and hasattr(proto, "on_node_down"):
            await proto.on_node_down(self, node)

    async def start_node(self, node_id: int) -> None:
        node = self.nodes[node_id]
        was = node.status
        node.status = "online"
        self.network.node_down(node_id, False)
        # Recovery: rebuild the replica from its durable rows, then replay the WAL.
        if self.repo is not None:
            node.ledger = ReplicaLedger()
            for aid, meta in self.accounts.items():
                node.ledger.seed(aid, meta["initial_balance"])
            for row in await self.repo.ledger_rows_for_node(node_id):
                node.ledger.replay_row(row.seq, row.tx_id, row.account_id, row.delta, row.mode)
            await self.repo.replay_wal(self, node)
        self.bus.emit("fault", f"Node {node.code} restarted (was {was}); replica rebuilt from durable log",
                      level="info", node_id=node_id)
        proto = self.protocols.get(self.mode)
        if proto is not None and hasattr(proto, "on_node_up"):
            await proto.on_node_up(self, node)

    def arm_crash(self, node_id: int, when: str) -> None:
        self.crash_hooks[node_id] = when
        self.bus.emit("fault", f"Crash hook armed on {self.nodes[node_id].code} at '{when}'",
                      level="warn", node_id=node_id)

    async def maybe_crash(self, node: VirtualNode, when: str) -> bool:
        """Protocols call this at arming points. Returns True if the node crashed."""
        if self.crash_hooks.pop(node.id, None) == when:
            await self.stop_node(node.id, crashed=True)
            self.bus.emit("fault", f"Node {node.code} crashed mid-transaction at '{when}'",
                          level="error", node_id=node.id)
            return True
        return False

    # ---- integrity & consistency ------------------------------------------
    def integrity_report(self) -> dict:
        bad = []
        for n in self.online_nodes():
            if not n.ledger.integrity_ok(self.total_supply):
                bad.append({"node": n.code, "total": n.ledger.total()})
        return {"ok": not bad, "expected_total": self.total_supply, "violations": bad}

    def consistency_report(self) -> dict:
        """Compare every online node's balances against the majority view."""
        online = self.online_nodes()
        if len(online) < 2:
            return {"agreement": 1.0, "diverged_nodes": [], "checked": len(online)}
        accounts = list(self.accounts)
        # majority balance per account
        diverged = []
        for aid in accounts:
            views: dict[float, int] = {}
            for n in online:
                b = n.ledger.balance_of(aid)
                views[b] = views.get(b, 0) + 1
            majority = max(views.values())
            for n in online:
                if views[n.ledger.balance_of(aid)] != majority or len(views) > 1:
                    if n.ledger.balance_of(aid) != max(views, key=views.get):
                        diverged.append(n.code)
        agreeing = len(online) - len(set(diverged))
        return {"agreement": round(agreeing / len(online), 4),
                "diverged_nodes": sorted(set(diverged)), "checked": len(online)}

    def snapshot(self) -> dict:
        return {
            "mode": self.mode,
            "nodes": [{"id": n.id, "code": n.code, "country": n.country,
                       "status": n.status, "clock_offset_ms": n.clock_offset_ms,
                       "entries": len(n.ledger.entries),
                       "raft": {"role": n.raft.role, "term": n.raft.term,
                                "leader_id": n.raft.leader_id}} for n in self.nodes],
            "countries": [{"name": c, "lat": COUNTRY_GEO[c][0], "lon": COUNTRY_GEO[c][1],
                           "nodes": len(self.nodes_of(c)),
                           "online": len(self.online_nodes(c))} for c in self.countries],
            "links": [{"a": list(k)[0], "b": list(k)[1],
                       "up": v.up, "latency_ms": v.latency_ms,
                       "loss": v.loss_rate, "dup": v.dup_rate}
                      for k, v in self.network._links.items()],
            "network": {"default": vars(self.network.default), "stats": self.network.stats},
            "total_supply": self.total_supply,
        }
