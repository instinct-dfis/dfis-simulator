import { useState } from 'react'
import { api } from '../api'
import type { ClusterSnapshot } from '../types'

export default function NodesPanel({ cluster, onChange }: { cluster: ClusterSnapshot | null; onChange: () => void }) {
  const [skewNode, setSkewNode] = useState('0')
  const [skewMs, setSkewMs] = useState('5000')
  const [busy, setBusy] = useState(false)
  if (!cluster) return <div className="panel">جاري التحميل...</div>

  const act = async (path: string) => {
    setBusy(true)
    try { await api.post(path) } finally { setBusy(false); onChange() }
  }
  const applySkew = async () => {
    setBusy(true)
    try { await api.post('/api/faults/clock-skew', { node_id: +skewNode, offset_ms: +skewMs }) }
    finally { setBusy(false); onChange() }
  }

  return (
    <div className="panel">
      <h3>الخوادم الافتراضية ({cluster.nodes.length}) — {cluster.mode === 'raft' ? '👑 = قائد Raft' : ''}</h3>
      <div className="controls-row">
        <label>انحراف الساعة:</label>
        <select value={skewNode} onChange={e => setSkewNode(e.target.value)}>
          {cluster.nodes.map(n => <option key={n.id} value={n.id}>{n.code}</option>)}
        </select>
        <input style={{ width: 100 }} value={skewMs} onChange={e => setSkewMs(e.target.value)} />
        <span className="muted">ms</span>
        <button onClick={applySkew} disabled={busy}>تطبيق الانحراف</button>
      </div>
      <div className="nodes-grid">
        {cluster.nodes.map(n => (
          <div key={n.id} className={`node-cell ${n.status}`}>
            {n.raft.role === 'leader' && cluster.mode === 'raft' && <span className="role leader">👑</span>}
            <div className="code">{n.code}</div>
            <div className="meta">{n.status === 'online' ? `${n.entries} قيدًا` : n.status === 'crashed' ? 'منهار' : 'متوقف'}</div>
            {n.clock_offset_ms !== 0 && <div className="meta">⏱ {n.clock_offset_ms > 0 ? '+' : ''}{n.clock_offset_ms}ms</div>}
            <div className="actions">
              {n.status === 'online' ? (
                <>
                  <button className="danger" disabled={busy} onClick={() => act(`/api/faults/node/${n.id}/stop`)}>إيقاف</button>
                  <button className="danger" disabled={busy} onClick={() => act(`/api/faults/node/${n.id}/crash`)}>انهيار</button>
                </>
              ) : (
                <button className="ok" disabled={busy} onClick={() => act(`/api/faults/node/${n.id}/start`)}>تشغيل</button>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
