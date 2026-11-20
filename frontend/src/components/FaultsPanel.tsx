import { useState } from 'react'
import { api } from '../api'
import type { ClusterSnapshot } from '../types'

const COUNTRIES = ['Egypt', 'Germany', 'USA', 'Japan', 'Brazil']

export default function FaultsPanel({ cluster, onChange }: { cluster: ClusterSnapshot | null; onChange: () => void }) {
  const [a, setA] = useState('Egypt')
  const [b, setB] = useState('USA')
  const [latency, setLatency] = useState('150')
  const [loss, setLoss] = useState('0.3')
  const [dup, setDup] = useState('0.3')
  const [busy, setBusy] = useState(false)
  if (!cluster) return <div className="panel">جاري التحميل...</div>

  const doPost = async (path: string, body?: unknown) => {
    setBusy(true)
    try { await api.post(path, body) } finally { setBusy(false); onChange() }
  }
  const sel = (v: string, set: (s: string) => void) => (
    <select value={v} onChange={e => set(e.target.value)}>
      {COUNTRIES.map(c => <option key={c}>{c}</option>)}
    </select>
  )

  return (
    <>
      <div className="panel">
        <h3>قطع الاتصال بين الدول</h3>
        <div className="form-row">
          {sel(a, setA)} <span className="muted">⇄</span> {sel(b, setB)}
          <button className="danger" disabled={busy || a === b} onClick={() => doPost('/api/faults/link/cut', { a, b })}>قطع</button>
          <button className="ok" disabled={busy || a === b} onClick={() => doPost('/api/faults/link/restore', { a, b })}>استعادة</button>
        </div>
        {cluster.links.length > 0 && (
          <table>
            <thead><tr><th>الرابط</th><th>الحالة</th><th>تأخير</th><th>فقدان</th><th>تكرار</th></tr></thead>
            <tbody>
              {cluster.links.map(l => (
                <tr key={l.a + l.b}>
                  <td>{l.a} ⇄ {l.b}</td>
                  <td>{l.up ? <span className="badge green">متصل</span> : <span className="badge red">مقطوع</span>}</td>
                  <td>{l.latency_ms}ms</td><td>{(l.loss * 100).toFixed(0)}%</td><td>{(l.dup * 100).toFixed(0)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
      <div className="panel">
        <h3>ظروف الشبكة العامة</h3>
        <div className="form-row">
          <label>التأخير (ms)</label>
          <input style={{ width: 90 }} value={latency} onChange={e => setLatency(e.target.value)} />
          <button disabled={busy} onClick={() => doPost('/api/faults/latency', { value: +latency })}>تطبيق</button>
        </div>
        <div className="form-row">
          <label>فقدان الرسائل (0-1)</label>
          <input style={{ width: 90 }} value={loss} onChange={e => setLoss(e.target.value)} />
          <button disabled={busy} onClick={() => doPost('/api/faults/loss', { value: +loss })}>تطبيق</button>
        </div>
        <div className="form-row">
          <label>تكرار الرسائل (0-1)</label>
          <input style={{ width: 90 }} value={dup} onChange={e => setDup(e.target.value)} />
          <button disabled={busy} onClick={() => doPost('/api/faults/duplication', { value: +dup })}>تطبيق</button>
        </div>
        <p className="muted">الحالية: تأخير {cluster.network.default.latency_ms}ms — فقدان {(cluster.network.default.loss_rate * 100).toFixed(0)}% — تكرار {(cluster.network.default.dup_rate * 100).toFixed(0)}%</p>
      </div>
      <div className="panel">
        <h3>إعادة الضبط</h3>
        <div className="form-row">
          <button className="ok" disabled={busy} onClick={() => doPost('/api/faults/reset')}>مسح كل الأعطال</button>
          <button className="danger" disabled={busy} onClick={() => doPost('/api/reset')}>إعادة ضبط المحاكاة بالكامل</button>
        </div>
      </div>
    </>
  )
}
