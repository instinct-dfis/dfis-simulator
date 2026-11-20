"""Common protocol interface. Every consistency algorithm is a REAL implementation
driven by the simulated network: none of them fake their outcomes."""
from __future__ import annotations

import asyncio


class ProtocolBase:
    name = "base"
    description = ""

    def __init__(self) -> None:
        self.repo = None
        self.lock = asyncio.Lock()

    async def execute(self, cluster, tx) -> dict:
        """Run one transfer. Returns {"outcome": "committed"|"rejected"|"failed", "error": str|None,
        "detail": str}. Must be idempotent on tx.tx_id."""
        raise NotImplementedError

    async def on_message(self, cluster, node, msg: dict) -> None:
        pass

    def on_tick(self, cluster, node, now_ms: float) -> None:
        pass

    async def on_node_up(self, cluster, node) -> None:
        pass

    async def on_node_down(self, cluster, node) -> None:
        pass

    @staticmethod
    def legs_of(tx) -> list[tuple[str, float]]:
        return [(tx["from_account"], -tx["amount"]), (tx["to_account"], tx["amount"])]
