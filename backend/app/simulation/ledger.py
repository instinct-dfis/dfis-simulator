"""Double-entry ledger replica. Pure data structure, no I/O: fully unit-testable."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class LedgerEntry:
    seq: int
    tx_id: str
    account_id: str
    delta: float
    mode: str
    ts: float


@dataclass
class ReplicaLedger:
    """One node's local copy of the ledger.

    Guarantees:
    - Idempotent apply: the same tx_id can never be applied twice.
    - Balance integrity is checkable against the known total supply.
    """
    entries: list[LedgerEntry] = field(default_factory=list)
    balances: dict[str, float] = field(default_factory=dict)
    applied_tx: set[str] = field(default_factory=set)

    def seed(self, account_id: str, balance: float) -> None:
        self.balances[account_id] = balance

    def balance_of(self, account_id: str) -> float:
        return self.balances.get(account_id, 0.0)

    def has_applied(self, tx_id: str) -> bool:
        return tx_id in self.applied_tx

    def apply(self, tx_id: str, legs: list[tuple[str, float]], mode: str, ts: float) -> bool:
        """Apply the double-entry legs of a tx atomically. Returns False if already applied."""
        if tx_id in self.applied_tx:
            return False
        for account_id, delta in legs:
            self.balances[account_id] = round(self.balances.get(account_id, 0.0) + delta, 6)
            self.entries.append(
                LedgerEntry(seq=len(self.entries) + 1, tx_id=tx_id, account_id=account_id,
                            delta=delta, mode=mode, ts=ts)
            )
        self.applied_tx.add(tx_id)
        return True

    def replay_row(self, seq: int, tx_id: str, account_id: str, delta: float, mode: str) -> None:
        """Rebuild from this node's durable rows (crash recovery). Rows are already
        the committed truth, so no idempotency guard: each row is one leg."""
        self.balances[account_id] = round(self.balances.get(account_id, 0.0) + delta, 6)
        self.entries.append(LedgerEntry(seq=seq, tx_id=tx_id, account_id=account_id,
                                        delta=delta, mode=mode, ts=0.0))
        self.applied_tx.add(tx_id)

    def revert(self, tx_id: str) -> bool:
        """Compensating reverse (used by eventual-consistency conflict resolution)."""
        if tx_id not in self.applied_tx:
            return False
        for e in [e for e in self.entries if e.tx_id == tx_id]:
            self.balances[e.account_id] = round(self.balances[e.account_id] - e.delta, 6)
        self.entries = [e for e in self.entries if e.tx_id != tx_id]
        self.applied_tx.discard(tx_id)
        return True

    def total(self) -> float:
        return round(sum(self.balances.values()), 6)

    def integrity_ok(self, expected_total: float, tol: float = 0.01) -> bool:
        return abs(self.total() - expected_total) <= tol
