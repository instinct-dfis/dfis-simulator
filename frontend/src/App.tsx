import { useCallback, useEffect, useRef, useState } from 'react'
import { api } from './api'
import type { ClusterSnapshot, EventRow, ModeInfo } from './types'
import Dashboard from './components/Dashboard'
import NodesPanel from './components/NodesPanel'
import FaultsPanel from './components/FaultsPanel'
import TransactionsPanel from './components/TransactionsPanel'
import TestLab from './components/TestLab'
import Reports from './components/Reports'
import EventLog from './components/EventLog'

type Tab = 'dashboard' | 'nodes' | 'faults' | 'transactions' | 'lab' | 'reports' | 'events'
const TABS: { id: Tab; label: string }[] = [
  { id: 'dashboard', label: 'لوحة التحكم' },
  { id: 'nodes', label: 'الخوادم' },
  { id: 'faults', label: 'حقن الأعطال' },
  { id: 'transactions', label: 'المعاملات' },
  { id: 'lab', label: 'مختبر الاختبارات' },
  { id: 'reports', label: 'التقارير' },
  { id: 'events', label: 'سجل الأحداث' },
]

export default function App() {
  const [tab, setTab] = useState<Tab>('dashboard')
  const [cluster, setCluster] = useState<ClusterSnapshot | null>(null)
  const [modes, setModes] = useState<ModeInfo[]>([])
  const [liveEvents, setLiveEvents] = useState<EventRow[]>([])
  const [wsState, setWsState] = useState('متصل...')
  const idRef = useRef(1000000)

  const refresh = useCallback(async () => {
    try {
      setCluster(await api.get<ClusterSnapshot>('/api/cluster'))
    } catch { /* backend not up yet */ }
  }, [])

  useEffect(() => {
    refresh()
    api.get<{ modes: ModeInfo[] }>('/api/modes').then(r => setModes(r.modes)).catch(() => {})
    const t = setInterval(refresh, 2000)
    return () => clearInterval(t)
  }, [refresh])

  useEffect(() => {
    const proto = location.protocol === 'https:' ? 'wss' : 'ws'
    const ws = new WebSocket(`${proto}://${location.host}/ws`)
    ws.onopen = () => setWsState('مباشر 🟢')
    ws.onclose = () => setWsState('غير متصل 🔴')
    ws.onmessage = ev => {
      try {
        const msg = JSON.parse(ev.data)
        if (msg.type === 'event') {
          const e = msg.event
          setLiveEvents(prev => [{
            id: ++idRef.current, ts: new Date().toISOString(), level: e.level,
            category: e.category, node_id: e.node_id, message: e.message, data: e.data,
          }, ...prev].slice(0, 400))
          if (e.category === 'fault' || e.category === 'cluster' || e.category === 'tx') refresh()
        }
      } catch { /* ignore malformed */ }
    }
    return () => ws.close()
  }, [refresh])

  const setMode = async (mode: string) => {
    await api.post('/api/cluster/mode', { mode })
    refresh()
  }

  return (
    <div className="app">
      <header>
        <h1>
          Distributed Financial Intelligence Simulator
          <small>محاكي شبكة مالية موزعة: 50 خادمًا في 5 دول — {wsState}</small>
        </h1>
        <div className="mode-picker">
          <span className="muted">خوارزمية الاتساق:</span>
          {modes.map(m => (
            <button key={m.id} className={cluster?.mode === m.id ? 'active' : ''}
                    title={m.description} onClick={() => setMode(m.id)}>
              {m.id}
            </button>
          ))}
        </div>
      </header>
      <nav>
        {TABS.map(t => (
          <button key={t.id} className={tab === t.id ? 'active' : ''} onClick={() => setTab(t.id)}>
            {t.label}
          </button>
        ))}
      </nav>
      {tab === 'dashboard' && <Dashboard cluster={cluster} />}
      {tab === 'nodes' && <NodesPanel cluster={cluster} onChange={refresh} />}
      {tab === 'faults' && <FaultsPanel cluster={cluster} onChange={refresh} />}
      {tab === 'transactions' && <TransactionsPanel />}
      {tab === 'lab' && <TestLab />}
      {tab === 'reports' && <Reports />}
      {tab === 'events' && <EventLog liveEvents={liveEvents} />}
    </div>
  )
}
