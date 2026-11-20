export interface NodeInfo {
  id: number; code: string; country: string; status: string;
  clock_offset_ms: number; entries: number;
  raft: { role: string; term: number; leader_id: number | null }
}
export interface CountryInfo { name: string; lat: number; lon: number; nodes: number; online: number }
export interface LinkInfo { a: string; b: string; up: boolean; latency_ms: number; loss: number; dup: number }
export interface ClusterSnapshot {
  mode: string; nodes: NodeInfo[]; countries: CountryInfo[]; links: LinkInfo[];
  network: { default: { latency_ms: number; loss_rate: number; dup_rate: number }; stats: Record<string, number> };
  total_supply: number;
}
export interface TxRow {
  tx_id: string; from: string; to: string; amount: number; mode: string;
  status: string; error: string | null; sim_latency_ms: number | null; created_at: string | null;
}
export interface EventRow {
  id: number; ts: string | null; level: string; category: string;
  node_id: number | null; message: string; data: Record<string, unknown>;
}
export interface Scenario { key: string; name: string; description: string; steps: Record<string, unknown>[] }
export interface RunResult {
  run_id: number; name: string; mode: string; passed: boolean; errors: string[];
  steps: { i: number; action: string; ok: boolean; detail?: string }[];
  metrics: Record<string, number>; integrity: { ok: boolean; violations: { node: string; total: number }[] };
  consistency: { agreement: number; diverged_nodes: string[] };
  balance_diffs: { account: string; node: string; balance: number; reference: number; diff: number }[];
  per_server_sample: { node: string; status: string; entries: number; total: number }[];
  events: EventRow[]; elapsed_s: number;
}
export interface ModeInfo { id: string; description: string }
