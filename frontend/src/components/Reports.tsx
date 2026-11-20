import { useCallback, useEffect, useState } from 'react'
import { api } from '../api'

const METRIC_LABELS: Record<string, string> = {
  availability: 'التوافر', throughput: 'الإنتاجية (tx/s)', latency_avg: 'متوسط التأخير (ms)',
  latency_p95: 'p95 التأخير (ms)', consistency: 'الاتساق', integrity: 'سلامة البيانات',
  fault_tolerance: 'تحمل الأعطال', exactly_once: 'التنفيذ مرة واحدة',
}
const METRIC_ORDER = ['consistency', 'availability', 'fault_tolerance', 'latency_avg', 'throughput', 'integrity']

export default function Reports() {
  const [byMode, setByMode] = useState<Record<string, Record<string, { avg: number; samples: number }>>>({})
  const [descriptions, setDescriptions] = useState<Record<string, string>>({})
  const [note, setNote] = useState('')
  const [runs, setRuns] = useState<{ id: number; name: string; mode: string; passed: boolean | null; started_at: string | null }[]>([])

  const load = useCallback(async () => {
    try {
      const r = await api.get<{ by_mode: Record<string, Record<string, { avg: number; samples: number }>>; descriptions: Record<string, string>; note: string }>('/api/reports/summary')
      setByMode(r.by_mode); setDescriptions(r.descriptions); setNote(r.note)
      const rr = await api.get<{ runs: { id: number; name: string; mode: string; passed: boolean | null; started_at: string | null }[] }>('/api/runs?limit=20')
      setRuns(rr.runs)
    } catch { /* backend starting */ }
  }, [])
  useEffect(() => { load(); const t = setInterval(load, 5000); return () => clearInterval(t) }, [load])

  const modes = Object.keys(descriptions)
  const maxOf = (metric: string) => Math.max(1e-9, ...modes.map(m => byMode[m]?.[metric]?.avg ?? 0))

  return (
    <>
      <div className="panel">
        <h3>مقارنة الخوارزميات (متوسطات مقاسة من التشغيلات الفعلية)</h3>
        <p className="muted" style={{ marginBottom: 12 }}>{note}</p>
        {modes.length === 0 && <p className="muted">شغّل سيناريوهات من مختبر الاختبارات أولًا لتتراكم القياسات.</p>}
        {METRIC_ORDER.map(metric => (
          <div key={metric} style={{ marginBottom: 14 }}>
            <b style={{ fontSize: 13 }}>{METRIC_LABELS[metric] ?? metric}</b>
            {modes.map(m => {
              const v = byMode[m]?.[metric]?.avg
              const isRatio = ['consistency', 'availability', 'fault_tolerance', 'integrity', 'exactly_once'].includes(metric)
              const pct = v == null ? 0 : (metric === 'latency_avg' || metric === 'throughput' ? (v / maxOf(metric)) * 100 : v * 100)
              return (
                <div className="bar-row" key={m}>
                  <span className="bar-label">{m}</span>
                  <div className="bar-track">
                    <div className="bar-fill" style={{ width: `${Math.min(100, pct)}%`, background: v == null ? 'var(--panel2)' : undefined }} />
                  </div>
                  <span className="bar-val">{v == null ? 'لا بيانات' : isRatio ? `${(v * 100).toFixed(1)}%` : v.toLocaleString()}</span>
                </div>
              )
            })}
          </div>
        ))}
      </div>
      <div className="panel">
        <h3>خصائص كل خوارزمية</h3>
        <table>
          <thead><tr><th>الخوارزمية</th><th>الخصائص والقيود الفعلية</th></tr></thead>
          <tbody>{modes.map(m => <tr key={m}><td><b>{m}</b></td><td className="muted">{descriptions[m]}</td></tr>)}</tbody>
        </table>
      </div>
      <div className="panel">
        <h3>سجل التشغيلات</h3>
        <div className="controls-row">
          <a href="/api/reports/export?format=json" download><button>⬇ تصدير JSON</button></a>
          <a href="/api/reports/export?format=csv" download><button>⬇ تصدير CSV</button></a>
        </div>
        <table>
          <thead><tr><th>#</th><th>السيناريو</th><th>الوضع</th><th>النتيجة</th><th>الوقت</th></tr></thead>
          <tbody>{runs.map(r => (
            <tr key={r.id}>
              <td>{r.id}</td><td>{r.name}</td><td>{r.mode}</td>
              <td>{r.passed == null ? '—' : r.passed ? <span className="badge green">نجح</span> : <span className="badge red">فشل</span>}</td>
              <td className="muted">{r.started_at ? new Date(r.started_at).toLocaleTimeString('ar-EG') : ''}</td>
            </tr>
          ))}</tbody>
        </table>
      </div>
    </>
  )
}
