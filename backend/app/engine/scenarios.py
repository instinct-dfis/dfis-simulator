"""Test lab: predefined and custom scenarios.

A scenario is a list of steps over a small DSL. The runner executes each step for
real against the live cluster, captures every emitted event, then computes
integrity/consistency/metrics from the actual final state. Nothing is staged.
"""
from __future__ import annotations

import asyncio
import time
import uuid

from .metrics import record_run_metrics


def _predefined() -> list[dict]:
    return [
        {"key": "concurrent_same_account", "name": "محاولة إنفاق مزدوج (عمليتان متزامنتان)",
         "description": "Two concurrent transfers drain the same account; at most one may win.",
         "steps": [
             {"action": "concurrent_transfers", "count": 2, "amount": 7000, "same_from": True},
             {"action": "wait_idle"},
             {"action": "assert_integrity"}, {"action": "assert_consistency"}]},
        {"key": "duplicate_submission", "name": "تكرار نفس المعاملة (idempotency)",
         "description": "Same idempotency key submitted twice plus 50% message duplication; must apply exactly once.",
         "steps": [
             {"action": "set_dup", "rate": 0.5},
             {"action": "duplicate_transfer", "amount": 250},
             {"action": "wait_idle"},
             {"action": "assert_integrity"}, {"action": "assert_consistency"},
             {"action": "reset_faults"}]},
        {"key": "network_partition", "name": "انقسام الشبكة بين دولتين",
         "description": "Cut Egypt<->USA mid-traffic, run transfers on both sides, heal, observe behavior per mode.",
         "steps": [
             {"action": "cut_link", "a": "Egypt", "b": "USA"},
             {"action": "transfer_batch", "count": 6},
             {"action": "wait_idle"},
             {"action": "restore_link", "a": "Egypt", "b": "USA"},
             {"action": "wait_ms", "ms": 3000},
             {"action": "wait_idle"},
             {"action": "assert_integrity"}, {"action": "assert_consistency"}]},
        {"key": "crash_mid_transaction", "name": "انهيار المنسّق/القائد أثناء معاملة",
         "description": "Arm a crash hook at the protocol's critical point, run a transfer, then restart and verify WAL recovery.",
         "steps": [
             {"action": "arm_crash", "when": "auto"},
             {"action": "transfer", "amount": 120, "expect": "any"},
             {"action": "wait_ms", "ms": 500},
             {"action": "start_node", "node": "last_crashed"},
             {"action": "wait_ms", "ms": 3000},
             {"action": "wait_idle"},
             {"action": "assert_integrity"}, {"action": "assert_consistency"}]},
        {"key": "message_chaos", "name": "فقدان وتكرار وتأخير الرسائل",
         "description": "30% loss, 30% duplication, 150ms latency; 10 transfers must each apply at most once.",
         "steps": [
             {"action": "set_loss", "rate": 0.3}, {"action": "set_dup", "rate": 0.3},
             {"action": "set_latency", "ms": 150},
             {"action": "transfer_batch", "count": 10},
             {"action": "wait_ms", "ms": 2000}, {"action": "wait_idle"},
             {"action": "assert_integrity"}, {"action": "assert_consistency"},
             {"action": "reset_faults"}]},
        {"key": "clock_skew", "name": "انحراف ساعات الخوادم",
         "description": "Skew three nodes by +/-5s and run traffic; observe ordering effects (esp. conflict winners in eventual mode).",
         "steps": [
             {"action": "set_clock_skew", "node": 3, "offset_ms": 5000},
             {"action": "set_clock_skew", "node": 17, "offset_ms": -5000},
             {"action": "set_clock_skew", "node": 41, "offset_ms": 8000},
             {"action": "transfer_batch", "count": 6},
             {"action": "wait_idle"},
             {"action": "assert_integrity"},
             {"action": "reset_faults"}]},
        {"key": "node_restart_recovery", "name": "إيقاف خادم واستعادته",
         "description": "Stop node 7, run 5 transfers, restart it, verify its replica catches up to the majority.",
         "steps": [
             {"action": "stop_node", "node": 7},
             {"action": "transfer_batch", "count": 5},
             {"action": "wait_idle"},
             {"action": "start_node", "node": 7},
             {"action": "wait_ms", "ms": 3000}, {"action": "wait_idle"},
             {"action": "assert_integrity"},
             {"action": "check_node_catchup", "node": 7}]},
        {"key": "double_spend_partition", "name": "إنفاق مزدوج تحت الانقسام (وضع eventual)",
         "description": "Partition, spend the same funds on both sides, heal; eventual consistency must detect and compensate.",
         "steps": [
             {"action": "set_mode", "mode": "eventual"},
             {"action": "cut_link", "a": "Egypt", "b": "USA"},
             {"action": "split_spend", "amount": 8000},
             {"action": "wait_ms", "ms": 500},
             {"action": "restore_link", "a": "Egypt", "b": "USA"},
             {"action": "wait_ms", "ms": 4000}, {"action": "wait_idle"},
             {"action": "assert_integrity"}, {"action": "assert_consistency"}]},
    ]


class ScenarioRunner:
    def __init__(self, cluster, repo, engine, faults):
        self.cluster = cluster
        self.repo = repo
        self.engine = engine
        self.faults = faults

    @staticmethod
    def predefined() -> list[dict]:
        return _predefined()

    async def run(self, name: str, steps: list[dict], custom: bool = False,
                  mode: str | None = None) -> dict:
        cluster = self.cluster
        run_mode = mode or cluster.mode
        run_id = await self.repo.create_run(name, run_mode, custom, steps)
        outcomes: list[dict] = []
        step_log: list[dict] = []
        errors: list[str] = []
        faults_active = False
        collected: list[dict] = []

        async def collector(ev: dict) -> None:
            collected.append(ev)
        cluster.bus.subscribe(collector)

        started = time.monotonic()
        last_crashed: int | None = None
        last_stopped: int | None = None
        try:
            for i, step in enumerate(steps):
                act = step.get("action")
                entry: dict = {"i": i, "action": act, "ok": True}
                try:
                    if act == "set_mode":
                        cluster.mode = step["mode"]
                        run_mode = step["mode"]
                        entry["detail"] = f"mode={cluster.mode}"
                        await asyncio.sleep(0.3)  # let a leader settle if raft
                    elif act == "reset_faults":
                        await self.faults.reset_all()
                        faults_active = False
                    elif act == "stop_node":
                        nid = self._resolve_node(step.get("node"), last_stopped, last_crashed)
                        await self.faults.stop_node(nid)
                        last_stopped = nid
                        faults_active = True
                        entry["detail"] = f"stopped {cluster.nodes[nid].code}"
                    elif act == "crash_node":
                        nid = self._resolve_node(step.get("node"), last_stopped, last_crashed)
                        await self.faults.stop_node(nid, crashed=True)
                        last_crashed = nid
                        faults_active = True
                        entry["detail"] = f"crashed {cluster.nodes[nid].code}"
                    elif act == "start_node":
                        nid = self._resolve_node(step.get("node"), last_stopped, last_crashed)
                        if step.get("node") == "last_crashed":
                            nid = last_crashed
                        await self.faults.start_node(nid)
                        entry["detail"] = f"restarted {cluster.nodes[nid].code}"
                    elif act == "cut_link":
                        self.faults.cut_link(step["a"], step["b"])
                        faults_active = True
                    elif act == "restore_link":
                        self.faults.restore_link(step["a"], step["b"])
                    elif act == "set_latency":
                        self.faults.set_latency(step["ms"], step.get("a"), step.get("b"))
                        faults_active = True
                    elif act == "set_loss":
                        self.faults.set_loss(step["rate"], step.get("a"), step.get("b"))
                        faults_active = True
                    elif act == "set_dup":
                        self.faults.set_dup(step["rate"], step.get("a"), step.get("b"))
                        faults_active = True
                    elif act == "set_clock_skew":
                        nid = step["node"] if isinstance(step.get("node"), int) else cluster.rng.randrange(len(cluster.nodes))
                        self.faults.set_clock_skew(nid, step["offset_ms"])
                        faults_active = True
                    elif act == "arm_crash":
                        when = step.get("when", "auto")
                        if when == "auto":
                            when = "after_append_before_commit" if cluster.mode == "raft" else "after_prepare"
                        nid = await self._pick_crash_target(step, when)
                        if nid is None:
                            entry["ok"] = False
                            entry["detail"] = "no crash target available (no leader/online node)"
                        else:
                            self.faults.arm_crash(nid, when)
                            last_crashed = nid
                            faults_active = True
                            entry["detail"] = f"armed {cluster.nodes[nid].code} at {when}"
                    elif act == "transfer":
                        a = step.get("from") or self._rand_account()
                        b = step.get("to") or self._rand_account(exclude=a)
                        r = await self.engine.submit(a, b, step.get("amount", 100.0),
                                                     tx_id=step.get("tx_id"))
                        outcomes.append(r)
                        entry["detail"] = f"{a}->{b} {r['status']}"
                        if r["status"] == "failed" and step.get("expect") != "any":
                            errors.append(f"step {i}: {r.get('error')}")
                    elif act == "duplicate_transfer":
                        a, b = self._rand_account(), None
                        b = self._rand_account(exclude=a)
                        tid = step.get("tx_id") or str(uuid.uuid4())
                        r1 = await self.engine.submit(a, b, step.get("amount", 100.0), tx_id=tid)
                        r2 = await self.engine.submit(a, b, step.get("amount", 100.0), tx_id=tid)
                        outcomes.extend([r1, r2])
                        once = (r2.get("duplicate") is True)
                        entry["detail"] = f"first={r1['status']}, second={'suppressed' if once else 'EXECUTED TWICE!'}"
                        if not once:
                            entry["ok"] = False
                            errors.append("duplicate submission executed twice")
                    elif act == "concurrent_transfers":
                        a = self._rand_account()
                        tasks = [self.engine.submit(a, self._rand_account(exclude=a),
                                                    step.get("amount", 100.0))
                                 for _ in range(step.get("count", 2))]
                        rs = await asyncio.gather(*tasks)
                        outcomes.extend(rs)
                        committed = sum(1 for r in rs if r["status"] == "committed")
                        entry["detail"] = f"{committed}/{len(rs)} committed from same account"
                        if committed > 1 and step.get("amount", 0) * 2 > 10000:
                            entry["ok"] = False
                            errors.append("double spend succeeded!")
                    elif act == "transfer_batch":
                        r = await self.engine.generate(step.get("count", 10),
                                                       concurrency=step.get("concurrency", 10))
                        outcomes.extend(r["results"])
                        entry["detail"] = (f"batch: {r['committed']} committed, {r['rejected']} rejected, "
                                           f"{r['failed']} failed, {r['throughput_tx_per_s']} tx/s")
                    elif act == "split_spend":
                        # spend the same Egypt-side funds from both sides of the Egypt<->USA cut
                        a = self.cluster.accounts_of("Egypt")[0]
                        usa = self.cluster.accounts_of("USA")[0]
                        eg2 = self.cluster.accounts_of("Egypt")[1]
                        amount = step.get("amount", 8000)
                        r1 = await self.engine.submit(a, eg2, amount)
                        # second spend is submitted on the OTHER side of the partition
                        usa_side = next((n for n in self.cluster.online_nodes() if n.country == "USA"), None)
                        r2 = await self.engine.submit(a, usa, amount,
                                                      gateway_node=usa_side.id if usa_side else None)
                        outcomes.extend([r1, r2])
                        entry["detail"] = f"side1={r1['status']}, side2={r2['status']}"
                    elif act == "wait_ms":
                        await asyncio.sleep(step["ms"] / 1000.0)
                    elif act == "wait_idle":
                        await cluster.network.wait_idle(timeout=15)
                        await asyncio.sleep(0.3)
                    elif act == "assert_integrity":
                        rep = cluster.integrity_report()
                        entry["detail"] = "integrity OK" if rep["ok"] else f"VIOLATIONS: {rep['violations'][:5]}"
                        entry["ok"] = rep["ok"]
                        if not rep["ok"]:
                            errors.append(f"step {i}: integrity violated: {rep['violations'][:3]}")
                    elif act == "assert_consistency":
                        # Replicas may legitimately be mid-catch-up (Raft commit_index
                        # propagation, gossip, restart replay). The meaningful property
                        # is CONVERGENCE: poll until agreement or timeout.
                        timeout = step.get("timeout_ms", 12000) / 1000.0
                        target = step.get("min_agreement", 1.0)
                        waited = 0.0
                        rep = cluster.consistency_report()
                        while rep["agreement"] < target and waited < timeout:
                            await asyncio.sleep(0.25)
                            waited += 0.25
                            rep = cluster.consistency_report()
                        entry["detail"] = (f"agreement={rep['agreement']:.1%} after {waited:.2f}s "
                                           f"diverged={rep['diverged_nodes'][:5]}")
                        if rep["agreement"] < target:
                            entry["ok"] = False
                            errors.append(f"step {i}: replicas did not converge in {timeout}s: {rep['diverged_nodes'][:5]}")
                    elif act == "check_node_catchup":
                        nid = step["node"]
                        node = cluster.nodes[nid]
                        others = [n for n in cluster.online_nodes() if n.id != nid]
                        if others:
                            ref = others[0]
                            same = all(node.ledger.balance_of(a) == ref.ledger.balance_of(a)
                                       for a in cluster.accounts)
                            entry["detail"] = (f"{node.code} entries={len(node.ledger.entries)} "
                                               f"reference={len(ref.ledger.entries)} match={same}")
                            entry["ok"] = same
                            if not same:
                                errors.append(f"step {i}: {node.code} did not catch up")
                    elif act == "wait_leader":
                        for _ in range(100):
                            proto = cluster.protocols.get("raft")
                            if proto and proto.leader_id is not None:
                                break
                            await asyncio.sleep(0.05)
                    else:
                        entry["ok"] = False
                        entry["detail"] = f"unknown action {act}"
                        errors.append(f"step {i}: unknown action {act}")
                except Exception as exc:
                    entry["ok"] = False
                    entry["detail"] = f"exception: {exc}"
                    errors.append(f"step {i} ({act}): {exc}")
                step_log.append(entry)
        finally:
            cluster.bus.subscribers.remove(collector)
        elapsed = time.monotonic() - started

        # final state, computed from what actually happened
        integrity = cluster.integrity_report()
        consistency = cluster.consistency_report()
        metrics = await record_run_metrics(self.repo, run_id, run_mode, outcomes,
                                           elapsed, consistency, integrity, faults_active)
        balance_diffs = self._balance_diffs()
        result = {
            "run_id": run_id, "name": name, "mode": run_mode, "custom": custom,
            "steps": step_log, "errors": errors, "metrics": metrics,
            "integrity": integrity, "consistency": consistency,
            "balance_diffs": balance_diffs,
            "per_server_sample": self._server_sample(),
            "events": collected[-300:],
            "elapsed_s": round(elapsed, 2),
            "faults_active_during_run": faults_active,
            "passed": not errors,
        }
        await self.repo.finish_run(run_id, result,
                                   status="done" if not errors else "done")
        return result

    # ---- helpers -------------------------------------------------------------
    def _resolve_node(self, spec, last_stopped, last_crashed) -> int:
        if isinstance(spec, int):
            return spec
        if spec == "last_stopped" and last_stopped is not None:
            return last_stopped
        if spec == "last_crashed" and last_crashed is not None:
            return last_crashed
        if spec == "last_crashed" or spec == "last_stopped":
            return 0
        online = self.cluster.online_nodes()
        return self.cluster.rng.choice(online).id if online else 0

    async def _pick_crash_target(self, step, when: str) -> int | None:
        if isinstance(step.get("node"), int):
            return step["node"]
        if self.cluster.mode == "raft" and when == "after_append_before_commit":
            proto = self.cluster.protocols.get("raft")
            for _ in range(100):
                if proto and proto.leader_id is not None:
                    return proto.leader_id
                await asyncio.sleep(0.05)
            return None
        online = self.cluster.online_nodes()
        return online[0].id if online else None

    def _rand_account(self, exclude: str | None = None) -> str:
        choices = [a for a in self.cluster.accounts if a != exclude]
        return self.cluster.rng.choice(choices)

    def _balance_diffs(self) -> list[dict]:
        online = self.cluster.online_nodes()
        if len(online) < 2:
            return []
        ref = online[0]
        diffs = []
        for aid in self.cluster.accounts:
            expected = ref.ledger.balance_of(aid)
            for n in online[1:]:
                b = n.ledger.balance_of(aid)
                if b != expected:
                    diffs.append({"account": aid, "node": n.code,
                                  "balance": b, "reference": expected,
                                  "diff": round(b - expected, 2)})
        return diffs[:100]

    def _server_sample(self) -> list[dict]:
        sample_ids = [0, 7, 13, 25, 38, 49]
        return [{"node": self.cluster.nodes[i].code, "status": self.cluster.nodes[i].status,
                 "entries": len(self.cluster.nodes[i].ledger.entries),
                 "total": self.cluster.nodes[i].ledger.total()}
                for i in sample_ids if i < len(self.cluster.nodes)]
