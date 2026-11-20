"""Fault injection surface. Every control is real: it changes the simulated
network / node states that the protocols actually use."""
from __future__ import annotations


class FaultInjector:
    def __init__(self, cluster):
        self.cluster = cluster

    async def stop_node(self, node_id: int, crashed: bool = False) -> dict:
        await self.cluster.stop_node(node_id, crashed=crashed)
        return {"node": self.cluster.nodes[node_id].code, "status": self.cluster.nodes[node_id].status}

    async def start_node(self, node_id: int) -> dict:
        await self.cluster.start_node(node_id)
        return {"node": self.cluster.nodes[node_id].code, "status": "online"}

    def cut_link(self, a: str, b: str) -> dict:
        self.cluster.network.cut_link(a, b)
        self.cluster.bus.emit("fault", f"Link CUT between {a} and {b}", level="warn")
        return {"link": [a, b], "up": False}

    def restore_link(self, a: str, b: str) -> dict:
        self.cluster.network.restore_link(a, b)
        self.cluster.bus.emit("fault", f"Link restored between {a} and {b}")
        return {"link": [a, b], "up": True}

    def set_latency(self, ms: float, a: str | None = None, b: str | None = None) -> dict:
        self.cluster.network.set_latency(ms, a, b)
        scope = f"{a}<->{b}" if a and b else "all links"
        self.cluster.bus.emit("fault", f"Network latency set to {ms}ms on {scope}", level="warn")
        return {"latency_ms": ms, "scope": scope}

    def set_loss(self, rate: float, a: str | None = None, b: str | None = None) -> dict:
        self.cluster.network.set_loss(rate, a, b)
        self.cluster.bus.emit("fault", f"Message loss set to {rate:.0%}", level="warn")
        return {"loss_rate": rate}

    def set_dup(self, rate: float, a: str | None = None, b: str | None = None) -> dict:
        self.cluster.network.set_dup(rate, a, b)
        self.cluster.bus.emit("fault", f"Message duplication set to {rate:.0%}", level="warn")
        return {"dup_rate": rate}

    def set_clock_skew(self, node_id: int, offset_ms: float) -> dict:
        node = self.cluster.nodes[node_id]
        node.clock_offset_ms = offset_ms
        self.cluster.bus.emit("fault", f"Clock of {node.code} skewed by {offset_ms}ms", level="warn",
                              node_id=node_id)
        return {"node": node.code, "offset_ms": offset_ms}

    def arm_crash(self, node_id: int, when: str) -> dict:
        self.cluster.arm_crash(node_id, when)
        return {"node": self.cluster.nodes[node_id].code, "armed_at": when}

    async def reset_all(self) -> dict:
        net = self.cluster.network
        for link in net._links.values():
            link.up, link.loss_rate, link.dup_rate = True, 0.0, 0.0
        net.default.latency_ms, net.default.loss_rate, net.default.dup_rate = 5.0, 0.0, 0.0
        self.cluster.crash_hooks.clear()
        for n in self.cluster.nodes:
            n.clock_offset_ms = 0.0
            if not n.is_online():
                await self.cluster.start_node(n.id)
        self.cluster.bus.emit("fault", "All faults cleared")
        return {"faults": "cleared"}
