import { useCallback, useEffect, useState } from 'react'
import { api } from '../api'
import type { TxRow } from '../types'

export default function TransactionsPanel() {
  const [accounts, setAccounts] = useState<{ id: string; country: string; balance: number }[]>([])
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')
  const [amount, setAmount] = useState('100')
  const [txs, setTxs] = useState<TxRow[]>([])
  const [counts, setCounts] = useState<Record<string, number>>({})
  const [lastResult, setLastResult] = useState('')
  const [genCount, setGenCount] = useState('20')
  const [busy, setBusy] = useState(false)

  const load = useCallback(async () => {
    try {
      const a = await api.get<{ accounts: { id: string; country: string; balance: number }[] }>('/api/accounts')
      setAccounts(a.accounts)
      setFrom(f => f || a.accounts[0]?.id || '')
      setTo(t => t || a.accounts[1]?.id || '')
      const t = await api.get<{ transactions: TxRow[]; counts: Record<string, number> }>('/api/transactions?limit=40')
      setTxs(t.transactions); setCounts(t.counts)
    } catch { /* backend starting */ }
  }, [])
  useEffect(() => { load(); const t = setInterval(load, 3000); return () => clearInterval(t) }, [load])

  const submit = async () => {
    setBusy(true)
    try {
      const r = await api.post<{ status: string; error?: string; detail?: string; sim_latency_ms?: number }>(
        '/api/transactions', { from_account: from, to_account: to, amount: +amount })
      setLastResult(`${r.status}${r.error ? ': ' + r.error : ''}${r.sim_latency_ms ? ` (${r.sim_latency_ms} ms)` : ''}`)
    } finally { setBusy(false); load() }
  }
  const generate = async () => {
    setBusy(true)
    try {
      const r = await api.post<{ committed: number; rejected: number; failed: number; throughput_tx_per_s: number }>(
        '/api/transactions/generate', { count: +genCount })
      setLastResult(`توليد ${genCount}: نجح ${r.committed} — رُفض ${r.rejected} — فشل ${r.failed} — ${r.throughput_tx_per_s} معاملة/ث`)
    } finally { setBusy(false); load() }
  }

  return (
    <>
      <div className="panel">
        <h3>تحويل مالي جديد</h3>
        <div className="form-row">
          <label>من</label>
          <select value={from} onChange={e => setFrom(e.target.value)}>
            {accounts.map(a => <option key={a.id} value={a.id}>{a.id} ({a.balance.toLocaleString()})</option>)}
          </select>
          <label>إلى</label>
          <select value={to} onChange={e => setTo(e.target.value)}>
            {accounts.map(a => <option key={a.id} value={a.id}>{a.id}</option>)}
          </select>
          <label>المبلغ</label>
          <input style={{ width: 100 }} value={amount} onChange={e => setAmount(e.target.value)} />
          <button className="ok" disabled={busy} onClick={submit}>تنفيذ التحويل</button>
        </div>
        <div className="form-row">
          <label>توليد حمل:</label>
          <input style={{ width: 70 }} value={genCount} onChange={e => setGenCount(e.target.value)} />
          <button disabled={busy} onClick={generate}>توليد معاملات عشوائية</button>
        </div>
        {lastResult && <p className="muted">النتيجة: {lastResult}</p>}
      </div>
      <div className="panel">
        <h3>سجل المعاملات (منفذة {counts['committed'] ?? 0} — فاشلة {counts['failed'] ?? 0} — مرفوضة {counts['rejected'] ?? 0} — معلقة {counts['pending'] ?? 0})</h3>
        <table>
          <thead><tr><th>المعاملة</th><th>من</th><th>إلى</th><th>المبلغ</th><th>الوضع</th><th>الحالة</th><th>زمن (محاكى)</th><th>السبب</th></tr></thead>
          <tbody>
            {txs.map(t => (
              <tr key={t.tx_id}>
                <td title={t.tx_id}>{t.tx_id.slice(0, 8)}…</td>
                <td>{t.from}</td><td>{t.to}</td><td>{t.amount}</td><td>{t.mode}</td>
                <td className={`status-${t.status}`}>{t.status}</td>
                <td>{t.sim_latency_ms != null ? `${Math.round(t.sim_latency_ms)}ms` : '—'}</td>
                <td className="muted">{t.error ?? ''}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}
