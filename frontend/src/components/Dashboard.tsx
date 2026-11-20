import { useEffect, useState } from 'react'
import { api } from '../api'
import type { ClusterSnapshot } from '../types'
import WorldMap from './WorldMap'

export default function Dashboard({ cluster }: { cluster: ClusterSnapshot | null }) {
  const [txCounts, setTxCounts] = useState<Record<string, number>>({})
  const [consistency, setConsistency] = useState<{ agreement: number; diverged_nodes: string[] } | null>(null)
  const [integrity, setIntegrity] = useState<{ ok: boolean } | null>(null)
  const [avgLatency, setAvgLatency] = useState<number | null>(null)

  useEffect(() => {
    const load = async () => {
      try {
        const txs = await api.get<{ counts: Record<string, number>; transactions: { sim_latency_ms: number | null }[] }>('/api/transactions?limit=50')
        setTxCounts(txs.counts)
        const lat = txs.transactions.map(t => t.sim_latency_ms).filter((x): x is number => x != null)
        setAvgLatency(lat.length ? Math.round(lat.reduce((a, b) => a + b, 0) / lat.length) : null)
        const c = await api.get<{ consistency: { agreement: number; diverged_nodes: string[] }; integrity: { ok: boolean } }>('/api/consistency')
        setConsistency(c.consistency); setIntegrity(c.integrity)
      } catch { /* backend starting */ }
    }
    load()
    const t = setInterval(load, 3000)
    return () => clearInterval(t)
  }, [])

  if (!cluster) return <div className="panel">جاري الاتصال بالخادم...</div>
  const online = cluster.nodes.filter(n => n.status === 'online').length
  const total = Object.values(txCounts).reduce((a, b) => a + b, 0)
  return (
    <>
      <div className="grid cards">
        <div className="card"><div className="label">خوادم نشطة</div><div className="num" style={{ color: 'var(--green)' }}>{online}</div></div>
        <div className="card"><div className="label">خوادم متوقفة</div><div className="num" style={{ color: 'var(--red)' }}>{cluster.nodes.length - online}</div></div>
        <div className="card"><div className="label">معاملات منفذة</div><div className="num">{txCounts['committed'] ?? 0}</div></div>
        <div className="card"><div className="label">معلقة / فاشلة / مرفوضة</div><div className="num">{(txCounts['pending'] ?? 0)} / {(txCounts['failed'] ?? 0)} / {(txCounts['rejected'] ?? 0)}</div></div>
        <div className="card"><div className="label">إجمالي المعروض النقدي</div><div className="num">{cluster.total_supply.toLocaleString()}</div></div>
        <div className="card"><div className="label">متوسط زمن الاستجابة</div><div className="num">{avgLatency != null ? `${avgLatency} ms` : '—'}</div></div>
        <div className="card"><div className="label">اتساق النسخ</div>
          <div className="num" style={{ color: consistency && consistency.agreement === 1 ? 'var(--green)' : 'var(--amber)' }}>
            {consistency ? `${(consistency.agreement * 100).toFixed(1)}%` : '—'}</div></div>
        <div className="card"><div className="label">سلامة الأرصدة</div>
          <div className="num">{integrity ? (integrity.ok ? <span className="badge green">سليمة ✓</span> : <span className="badge red">انتهاك ✗</span>) : '—'}</div></div>
      </div>
      <div className="panel" style={{ marginTop: 14 }}>
        <h3>الخريطة الجغرافية للخوادم ({total} معاملة مسجلة)</h3>
        <WorldMap cluster={cluster} />
        <p className="muted" style={{ marginTop: 8 }}>
          رسائل الشبكة: أُرسل {cluster.network.stats.sent} — أُسقط {cluster.network.stats.dropped} — كُرر {cluster.network.stats.duplicated} — سُلم {cluster.network.stats.delivered}
        </p>
      </div>
    </>
  )
}
