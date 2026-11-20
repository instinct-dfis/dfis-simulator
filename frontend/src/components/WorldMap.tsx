import type { ClusterSnapshot } from '../types'

// equirectangular projection of the 5 country locations onto a simple world grid
function project(lat: number, lon: number, w: number, h: number) {
  return { x: ((lon + 180) / 360) * w, y: ((90 - lat) / 180) * h }
}

export default function WorldMap({ cluster }: { cluster: ClusterSnapshot }) {
  const W = 640, H = 320
  const pts = Object.fromEntries(cluster.countries.map(c => [c.name, project(c.lat, c.lon, W, H)]))
  const pairs: [string, string][] = []
  for (let i = 0; i < cluster.countries.length; i++)
    for (let j = i + 1; j < cluster.countries.length; j++)
      pairs.push([cluster.countries[i].name, cluster.countries[j].name])
  const linkState = (a: string, b: string) => {
    const l = cluster.links.find(l => (l.a === a && l.b === b) || (l.a === b && l.b === a))
    return l && !l.up ? 'cut' : 'up'
  }
  return (
    <div className="map-wrap">
      <svg viewBox={`0 0 ${W} ${H}`} style={{ width: '100%' }}>
        <rect x="0" y="0" width={W} height={H} fill="#0e1530" rx="8" />
        {Array.from({ length: 11 }, (_, i) => (
          <line key={'v' + i} x1={(i * W) / 10} y1="0" x2={(i * W) / 10} y2={H} stroke="#1b2447" strokeWidth="1" />
        ))}
        {Array.from({ length: 5 }, (_, i) => (
          <line key={'h' + i} x1="0" y1={(i * H) / 4} x2={W} y2={(i * H) / 4} stroke="#1b2447" strokeWidth="1" />
        ))}
        {pairs.map(([a, b]) => (
          <line key={a + b} x1={pts[a].x} y1={pts[a].y} x2={pts[b].x} y2={pts[b].y}
                stroke={linkState(a, b) === 'cut' ? '#ff5c6c' : '#2a3560'}
                strokeWidth={linkState(a, b) === 'cut' ? 2 : 1}
                strokeDasharray={linkState(a, b) === 'cut' ? '5,4' : undefined} />
        ))}
        {cluster.countries.map(c => (
          <g key={c.name}>
            <circle cx={pts[c.name].x} cy={pts[c.name].y} r="16"
                    fill={c.online === c.nodes ? '#2ecc71' : c.online === 0 ? '#ff5c6c' : '#ffb020'}
                    opacity="0.85" />
            <text x={pts[c.name].x} y={pts[c.name].y + 4} textAnchor="middle" fontSize="11" fontWeight="bold" fill="#0b1020">
              {c.online}
            </text>
            <text x={pts[c.name].x} y={pts[c.name].y + 32} textAnchor="middle" fontSize="11" fill="#e8ecf8">
              {c.name} ({c.online}/{c.nodes})
            </text>
          </g>
        ))}
      </svg>
    </div>
  )
}
