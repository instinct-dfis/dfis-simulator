"""Simulated network between virtual nodes.

Delivers messages asynchronously with configurable latency, loss, duplication
and country-level partitions. This is the layer every fault is injected into,
so Raft/2PC/gossip genuinely experience the failures (no mocked outcomes).
"""
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable


@dataclass
class LinkConditions:
    latency_ms: float = 5.0
    loss_rate: float = 0.0    # 0..1 probability a message is dropped
    dup_rate: float = 0.0     # 0..1 probability a message is delivered twice
    up: bool = True           # False = link cut (partition)


class SimulatedNetwork:
    def __init__(self, countries: list[str], time_scale: float, seed: int = 7):
        self.countries = countries
        self.time_scale = time_scale
        self.rng = random.Random(seed)
        self.default = LinkConditions()
        self._links: dict[frozenset[str], LinkConditions] = {}
        self._node_down: set[int] = set()
        self._deliver: Callable[[int, dict], Awaitable[None]] | None = None
        self.stats = {"sent": 0, "dropped": 0, "duplicated": 0, "delivered": 0}
        self.inflight: int = 0
        self._idle = asyncio.Event()
        self._idle.set()

    def set_deliverer(self, fn: Callable[[int, dict], Awaitable[None]]) -> None:
        self._deliver = fn

    # ---- fault controls -------------------------------------------------
    def link(self, a: str, b: str) -> LinkConditions:
        return self._links.setdefault(frozenset((a, b)), LinkConditions())

    def cut_link(self, a: str, b: str) -> None:
        self.link(a, b).up = False

    def restore_link(self, a: str, b: str) -> None:
        self.link(a, b).up = True

    def set_latency(self, ms: float, a: str | None = None, b: str | None = None) -> None:
        (self.link(a, b) if a and b else self.default).latency_ms = ms

    def set_loss(self, rate: float, a: str | None = None, b: str | None = None) -> None:
        (self.link(a, b) if a and b else self.default).loss_rate = rate

    def set_dup(self, rate: float, a: str | None = None, b: str | None = None) -> None:
        (self.link(a, b) if a and b else self.default).dup_rate = rate

    def node_down(self, node_id: int, down: bool) -> None:
        (self._node_down.add if down else self._node_down.discard)(node_id)

    def conditions_for(self, ca: str, cb: str) -> LinkConditions:
        specific = self._links.get(frozenset((ca, cb)))
        base = self.default
        if specific is None:
            return base
        return LinkConditions(
            latency_ms=specific.latency_ms if specific.latency_ms != 5.0 else base.latency_ms,
            loss_rate=max(specific.loss_rate, base.loss_rate),
            dup_rate=max(specific.dup_rate, base.dup_rate),
            up=specific.up,
        )

    # ---- sending ---------------------------------------------------------
    def send(self, src: tuple[int, str], dst: tuple[int, str], msg: dict[str, Any]) -> bool:
        """Enqueue a message. Returns False if it could never arrive (down/cut)."""
        self.stats["sent"] += 1
        if src[0] in self._node_down or dst[0] in self._node_down:
            self.stats["dropped"] += 1
            return False
        cond = self.conditions_for(src[1], dst[1])
        if not cond.up:
            self.stats["dropped"] += 1
            return False
        if self.rng.random() < cond.loss_rate:
            self.stats["dropped"] += 1
            return False
        copies = 2 if self.rng.random() < cond.dup_rate else 1
        if copies == 2:
            self.stats["duplicated"] += 1
        for _ in range(copies):
            self._schedule(dst[0], msg, cond.latency_ms)
        return True

    def broadcast(self, src: tuple[int, str], dsts: list[tuple[int, str]], msg: dict[str, Any]) -> int:
        return sum(1 for d in dsts if self.send(src, d, msg))

    def _schedule(self, dst_id: int, msg: dict[str, Any], latency_ms: float) -> None:
        self.inflight += 1
        self._idle.clear()

        async def _deliver() -> None:
            try:
                await asyncio.sleep(latency_ms * self.time_scale / 1000.0)
                if dst_id not in self._node_down and self._deliver:
                    self.stats["delivered"] += 1
                    await self._deliver(dst_id, msg)
                else:
                    self.stats["dropped"] += 1
            finally:
                self.inflight -= 1
                if self.inflight == 0:
                    self._idle.set()

        asyncio.get_running_loop().create_task(_deliver())

    async def wait_idle(self, timeout: float = 10.0) -> None:
        """Wait until no messages are in flight (used by scenarios/tests)."""
        try:
            await asyncio.wait_for(self._idle.wait(), timeout)
        except asyncio.TimeoutError:
            pass

    @staticmethod
    def sim_now_ms() -> float:
        return time.monotonic() * 1000.0
