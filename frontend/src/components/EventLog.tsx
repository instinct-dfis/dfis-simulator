import { useEffect, useState } from 'react'
import { api } from '../api'
import type { EventRow } from '../types'

export default function EventLog({ liveEvents }: { liveEvents: EventRow[] }) {
  const [history, setHistory] = useState<EventRow[]>([])
  const [filter, setFilter] = useState('')
  useEffect(() => {
    api.get<{ events: EventRow[] }>('/api/events?limit=200').then(r => setHistory(r.events)).catch(() => {})
  }, [liveEvents.length])
  const all = [...liveEvents, ...history.filter(h => !liveEvents.some(l => l.id === h.id))]
  const filtered = filter ? all.filter(e => e.category === filter) : all
  const cats = [...new Set(all.map(e => e.category))]
  return (
    <div className="panel">
      <h3>سجل الأحداث التفصيلي (مباشر)</h3>
      <div className="controls-row">
        <select value={filter} onChange={e => setFilter(e.target.value)}>
          <option value="">كل الفئات</option>
          {cats.map(c => <option key={c}>{c}</option>)}
        </select>
        <span className="muted">{filtered.length} حدث</span>
      </div>
      <div className="events">
        {filtered.map(e => (
          <div key={e.id} className={`event-line ${e.level}`}>
            <span className="muted">{e.ts ? new Date(e.ts).toLocaleTimeString('ar-EG') : ''}</span>
            <span className="cat">[{e.category}]</span>
            {e.message}
          </div>
        ))}
      </div>
    </div>
  )
}
