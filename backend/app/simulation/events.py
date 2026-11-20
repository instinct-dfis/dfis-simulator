"""Cluster event bus: every significant action flows here -> DB log + WebSocket."""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Awaitable


class EventBus:
    def __init__(self) -> None:
        self.queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=10000)
        self.subscribers: list[Callable[[dict], Awaitable[None]]] = []
        self.persist: Callable[[dict], Awaitable[None]] | None = None
        self._task: asyncio.Task | None = None
        self._running = False

    def emit(self, category: str, message: str, level: str = "info",
             node_id: int | None = None, data: dict[str, Any] | None = None) -> None:
        event = {"category": category, "message": message, "level": level,
                 "node_id": node_id, "data": data or {}}
        try:
            self.queue.put_nowait(event)
        except asyncio.QueueFull:
            pass  # never block the simulation on logging

    def subscribe(self, fn: Callable[[dict], Awaitable[None]]) -> None:
        self.subscribers.append(fn)

    async def _dispatch_loop(self) -> None:
        while self._running:
            event = await self.queue.get()
            if self.persist:
                try:
                    await self.persist(event)
                except Exception:
                    pass  # logging must never kill the simulator
            for fn in list(self.subscribers):
                try:
                    await fn(event)
                except Exception:
                    pass

    def start(self) -> None:
        self._running = True
        self._task = asyncio.get_running_loop().create_task(self._dispatch_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
