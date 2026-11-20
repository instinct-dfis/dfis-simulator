"""WebSocket hub: the dashboard receives every cluster event live."""
from __future__ import annotations

import json

from fastapi import WebSocket


class WSHub:
    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self.clients.discard(ws)

    async def broadcast_event(self, event: dict) -> None:
        if not self.clients:
            return
        payload = json.dumps({"type": "event", "event": event}, ensure_ascii=False, default=str)
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.clients.discard(ws)


hub = WSHub()
