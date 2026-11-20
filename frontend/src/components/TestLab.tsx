import { useCallback, useEffect, useState } from 'react'
import { api } from '../api'
import type { RunResult, Scenario } from '../types'

export default function TestLab() {
  const [scenarios, setScenarios] = useState<Scenario[]>([])
  const [running, setRunning] = useState<string | null>(null)
  const [result, setResult] = useState<RunResult | null>(null)
  const [customSteps, setCustomSteps] = useState('[\n  {"action": "set_loss", "rate": 0.2},\n  {"action": "transfer_batch", "count": 5},\n  {"action": "wait_idle"},\n  {"action": "assert_integrity"}\n]')
  const [error, setError] = useState('')

  useEffect(() => {
    api.get<{ scenarios: Scenario[] }>('/api/scenarios').then(r => setScenarios(r.scenarios)).catch(() => {})
  }, [])

  const run = async (key: string) => {
    setRunning(key); setError('')
    try { setResult(await api.post<RunResult>('/api/scenarios/run', { key })) }
    catch (e) { setError(String(e)) }
    finally { setRunning(null) }
  }
  const runCustom = async () => {
    setRunning('custom'); setError('')
    try {
      const steps = JSON.parse(customSteps)
      setResult(await api.post<RunResult>('/api/scenarios/run', { steps, name: 'سيناريو مخصص' }))
    } catch (e) { setError(String(e)) }
    finally { setRunning(null) }
  }

  return (
    <>
      <div className="panel">
        <h3>سيناريوهات جاهزة</h3>
        {scenarios.map(s => (
          <div className="scenario" key={s.key}>
            <b>{s.name}</b>
            <div className="desc">{s.description}</div>
            <button onClick={() => run(s.key)} disabled={running !== null}>
              {running === s.key ? '⏳ جاري التنفيذ...' : '▶ تشغيل'}
            </button>
          </div>
        ))}
      </div>
      <div className="panel">
        <h3>سيناريو مخصص (JSON)</h3>
        <p className="muted">الأوامر المتاحة: set_mode, stop_node, start_node, crash_node, cut_link, restore_link, set_latency, set_loss, set_dup, set_clock_skew, arm_crash, transfer, duplicate_transfer, concurrent_transfers, transfer_batch, split_spend, wait_ms, wait_idle, assert_integrity, assert_consistency, check_node_catchup, reset_faults</p>
        <textarea dir="ltr" style={{ width: '100%', height: 140, background: 'var(--panel2)', color: 'var(--text)', border: '1px solid var(--border)', borderRadius: 8, padding: 10, fontFamily: 'monospace', fontSize: 12 }}
                  value={customSteps} onChange={e => setCustomSteps(e.target.value)} />
        <button style={{ marginTop: 8 }} onClick={runCustom} disabled={running !== null}>
          {running === 'custom' ? '⏳ جاري التنفيذ...' : '▶ تشغيل المخصص'}
        </button>
        {error && <p style={{ color: 'var(--red)', marginTop: 8 }}>{error}</p>}
      </div>
      {result && (
        <div className="panel">
          <h3>نتيجة: {result.name} — {result.passed ? <span className="badge green">نجح ✓</span> : <span className="badge red">أخطاء مكتشفة ✗</span>}</h3>
          <div className="two-col">
            <div>
              <h4 style={{ marginBottom: 8 }}>خطوات التنفيذ</h4>
              {result.steps.map(s => (
                <div key={s.i} className={`step-line ${s.ok ? 'ok' : 'bad'}`}>
                  {s.ok ? '✓' : '✗'} [{s.i}] {s.action} {s.detail ? `— ${s.detail}` : ''}
                </div>
              ))}
            </div>
            <div>
              <h4 style={{ marginBottom: 8 }}>المقاييس المقاسة</h4>
              {Object.entries(result.metrics).map(([k, v]) => (
                <div className="bar-row" key={k}>
                  <span className="bar-label">{k}</span>
                  <div className="bar-track"><div className="bar-fill" style={{ width: `${Math.min(100, (typeof v === 'number' && v <= 1 ? v * 100 : Math.min(100, v as number)))}%` }} /></div>
                  <span className="bar-val">{typeof v === 'number' ? v.toFixed(2) : v}</span>
                </div>
              ))}
              <p className="muted">الاتساق: {(result.consistency.agreement * 100).toFixed(1)}% — السلامة: {result.integrity.ok ? '✓' : '✗'} — المدة: {result.elapsed_s}s</p>
            </div>
          </div>
          {result.errors.length > 0 && (
            <>
              <h4 style={{ margin: '10px 0 6px', color: 'var(--red)' }}>الأخطاء المكتشفة</h4>
              {result.errors.map((e, i) => <div key={i} className="step-line bad">✗ {e}</div>)}
            </>
          )}
          {result.balance_diffs.length > 0 && (
            <>
              <h4 style={{ margin: '10px 0 6px' }}>فروقات الأرصدة بين النسخ</h4>
              <table>
                <thead><tr><th>الحساب</th><th>الخادم</th><th>الرصيد</th><th>المرجع</th><th>الفرق</th></tr></thead>
                <tbody>{result.balance_diffs.slice(0, 20).map((d, i) => (
                  <tr key={i}><td>{d.account}</td><td>{d.node}</td><td>{d.balance}</td><td>{d.reference}</td><td>{d.diff}</td></tr>
                ))}</tbody>
              </table>
            </>
          )}
          <h4 style={{ margin: '10px 0 6px' }}>عينة من الخوادم</h4>
          <table>
            <thead><tr><th>الخادم</th><th>الحالة</th><th>عدد القيود</th><th>الإجمالي</th></tr></thead>
            <tbody>{result.per_server_sample.map(s => (
              <tr key={s.node}><td>{s.node}</td><td>{s.status}</td><td>{s.entries}</td><td>{s.total.toLocaleString()}</td></tr>
            ))}</tbody>
          </table>
          <h4 style={{ margin: '10px 0 6px' }}>الأحداث أثناء السيناريو ({result.events.length})</h4>
          <div className="events">
            {result.events.map((e, i) => (
              <div key={i} className={`event-line ${e.level}`}>
                <span className="cat">[{e.category}]</span>{e.message}
              </div>
            ))}
          </div>
        </div>
      )}
    </>
  )
}
