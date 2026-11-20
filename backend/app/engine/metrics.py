"""Measured metrics only. Nothing here is invented: every number is computed from
what actually happened during a run (rule 2 and rule 10 of the spec)."""
from __future__ import annotations


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = min(len(s) - 1, max(0, int(round((p / 100.0) * (len(s) - 1)))))
    return s[k]


async def record_run_metrics(repo, run_id: int, mode: str, outcomes: list[dict],
                             elapsed_s: float, consistency: dict, integrity: dict,
                             faults_active: bool) -> dict:
    submitted = len(outcomes)
    committed = sum(1 for o in outcomes if o.get("status") == "committed")
    duplicates = sum(1 for o in outcomes if o.get("duplicate"))
    latencies = [o.get("sim_latency_ms", 0.0) for o in outcomes if o.get("sim_latency_ms")]
    metrics = {
        "availability": round(committed / submitted, 4) if submitted else 0.0,
        "throughput": round(committed / elapsed_s, 2) if elapsed_s > 0 else 0.0,
        "latency_avg": round(sum(latencies) / len(latencies), 1) if latencies else 0.0,
        "latency_p95": round(percentile(latencies, 95), 1) if latencies else 0.0,
        "consistency": consistency.get("agreement", 1.0),
        "integrity": 1.0 if integrity.get("ok") else 0.0,
        # fault tolerance = availability measured while faults were injected
        "fault_tolerance": round(committed / submitted, 4) if (submitted and faults_active) else None,
        "exactly_once": 1.0 if duplicates == 0 else 0.0,
    }
    for k, v in metrics.items():
        if v is not None:
            await repo.add_metric(run_id, mode, k, float(v))
    return {k: v for k, v in metrics.items() if v is not None} | {
        "submitted": submitted, "committed": committed,
        "rejected": sum(1 for o in outcomes if o.get("status") == "rejected"),
        "failed": sum(1 for o in outcomes if o.get("status") == "failed"),
    }
