"""A virtual server: one of 50 simulated nodes, each holding a ledger replica."""
from __future__ import annotations

import random
from dataclasses import dataclass, field

from .ledger import ReplicaLedger

COUNTRY_CODES = {"Egypt": "EG", "Germany": "DE", "USA": "US", "Japan": "JP", "Brazil": "BR"}
COUNTRY_GEO = {  # approximate lat/lon for the map view
    "Egypt": (26.8, 30.8), "Germany": (51.2, 10.4), "USA": (39.8, -98.6),
    "Japan": (36.2, 138.3), "Brazil": (-10.8, -52.9),
}


@dataclass
class RaftState:
    role: str = "follower"           # follower|candidate|leader
    term: int = 0
    voted_for: int | None = None
    log: list[dict] = field(default_factory=list)  # {"term": int, "command": dict}
    commit_index: int = -1
    leader_id: int | None = None
    last_heartbeat_ms: float = 0.0
    election_timeout_ms: float = 0.0
    votes: set[int] = field(default_factory=set)


@dataclass
class TwoPCState:
    # participant side: tx_id -> {"legs": [...], "reserved": {...}, "coordinator": node_id}
    prepared: dict[str, dict] = field(default_factory=dict)
    reserved: dict[str, float] = field(default_factory=dict)  # account -> reserved amount
    # coordinator side WAL mirror: tx_id -> {"phase": "preparing"|"committing"|"done",
    #   "decision": "commit"|"abort"|None, "participants": [...], "acks": [...], "votes": {...}}
    coordinating: dict[str, dict] = field(default_factory=dict)


@dataclass
class EventSourcingState:
    events: list[dict] = field(default_factory=list)  # {"seq","tx_id","legs","ts","node"}
    seq: int = 0


@dataclass
class VirtualNode:
    id: int
    country: str
    status: str = "online"           # online|offline|crashed
    clock_offset_ms: float = 0.0
    ledger: ReplicaLedger = field(default_factory=ReplicaLedger)
    raft: RaftState = field(default_factory=RaftState)
    twopc: TwoPCState = field(default_factory=TwoPCState)
    es: EventSourcingState = field(default_factory=EventSourcingState)
    wal_seq: int = 0
    reverted: set = field(default_factory=set)  # tombstoned tx ids (eventual conflict losers)

    @property
    def code(self) -> str:
        return f"{COUNTRY_CODES[self.country]}-{self.id:02d}"

    def is_online(self) -> bool:
        return self.status == "online"

    def now_ms(self, cluster_now_ms: float) -> float:
        return cluster_now_ms + self.clock_offset_ms

    def reset_volatile_state(self) -> None:
        """What a crash destroys: everything that lived only in RAM."""
        self.raft = RaftState()
        self.twopc = TwoPCState()
        # The ledger's durable part is reloaded from the DB on restart,
        # so the in-memory replica is rebuilt there, not here.
        self.ledger = ReplicaLedger()
