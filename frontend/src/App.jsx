import { useEffect, useState } from 'react'
import { api, wsUrl } from './api.js'

const empty = { instrument: 'NIFTY', ce_strike: '', pe_strike: '', lots: 1, target_points: 20, stop_points: 10, strategy: 'market_exit', trail_distance: 5, product: 'MIS', entry_mode: 'MKT' }

export default function App() {
  const [token, setToken] = useState(localStorage.getItem('token') || '')
  const [login, setLogin] = useState({ username: '', password: '' })
  const [totp, setTotp] = useState('')
  const [kotakIn, setKotakIn] = useState(false)
  const [ticks, setTicks] = useState({})
  const [orders, setOrders] = useState([])
  const [synthetic, setSynthetic] = useState([])
  const [positions, setPositions] = useState({})
  const [f, setF] = useState(empty)
  const [msg, setMsg] = useState('')
  const [expiries, setExpiries] = useState([])
  const [strikes, setStrikes] = useState({ ce: [], pe: [] })

  const auth = { headers: { Authorization: `Bearer ${token}` } }

  useEffect(() => {
    if (!token) return
    const ws = new WebSocket(wsUrl())
    ws.onmessage = (e) => {
      try {
        const m = JSON.parse(e.data)
        if (m.kind === 'tick' && m.tick?.tk) setTicks((t) => ({ ...t, [m.tick.tk]: m.tick }))
        if (m.kind === 'order') setOrders((o) => [m.order, ...o].slice(0, 100))
      } catch {}
    }
    return () => ws.close()
  }, [token])

  const doLogin = async () => {
    const r = await api.post('/auth/login', login)
    localStorage.setItem('token', r.data.token)
    setToken(r.data.token)
    setMsg('App login OK. Now do Kotak TOTP login.')
  }
  const kotakLogin = async () => {
    await api.post('/auth/kotak/initiate', {}, auth)
    await api.post('/auth/kotak/complete', { totp }, auth)
    setKotakIn(true)
    refresh()
  }
  const refresh = async () => {
    const [s, p] = await Promise.all([
      api.get('/orders/synthetic', auth).catch(() => ({ data: { orders: [] } })),
      api.get('/market/positions', auth).catch(() => ({ data: { positions: {} } })),
    ])
    setSynthetic(s.data.orders || [])
    setPositions(p.data.positions || {})
    const st = await api.get('/auth/kotak/status', auth).catch(() => null)
    if (st) setKotakIn(st.data.logged_in)
    loadExpiries(f.instrument, f.expiry)
  }
  const loadExpiries = async (instrument, keep) => {
    try {
      const r = await api.get(`/market/expiries?instrument=${instrument}`, auth)
      const list = r.data.expiries || []
      setExpiries(list)
      const exp = keep && list.includes(keep) ? keep : list[0] || ''
      if (exp && exp !== f.expiry) setF((v) => ({ ...v, expiry: exp }))
      if (exp) loadStrikes(instrument, exp)
    } catch { setExpiries([]) }
  }
  const loadStrikes = async (instrument, expiry) => {
    try {
      const r = await api.get(`/market/strikes?instrument=${instrument}&expiry=${expiry}&spot=0`, auth)
      setStrikes({ ce: r.data.ce || [], pe: r.data.pe || [] })
    } catch {}
  }
  const setInstrument = (v) => { setF((x) => ({ ...x, instrument: v, expiry: '' })); loadExpiries(v, '') }
  const setExpiry = (v) => { setF((x) => ({ ...x, expiry: v })); loadStrikes(f.instrument, v) }

  const place = async (side) => {
    const ceLtp = prompt('Current CE LTP (from ticker)?')
    if (side === 'BOTH') {
      const peLtp = prompt('Current PE LTP?')
      const r = await api.post(`/orders/straddle?ce_ltp=${ceLtp}&pe_ltp=${peLtp}`, f, auth)
      setMsg('Straddle placed: ' + JSON.stringify(r.data))
    } else {
      const body = { ...f, option_type: side, strike: side === 'CE' ? f.ce_strike : f.pe_strike, entry_offset: 0 }
      const r = await api.post(`/orders/single?ref_ltp=${ceLtp}`, body, auth)
      setMsg('Order placed: ' + JSON.stringify(r.data))
    }
    refresh()
  }

  if (!token) return (
    <div style={{ maxWidth: 360, margin: '60px auto', fontFamily: 'system-ui' }}>
      <h2>KotakAPI Trader — login</h2>
      <input placeholder="username" value={login.username} onChange={(e) => setLogin({ ...login, username: e.target.value })} /><br />
      <input placeholder="password" type="password" value={login.password} onChange={(e) => setLogin({ ...login, password: e.target.value })} /><br />
      <button onClick={doLogin}>Login</button>
      <p>{msg}</p>
    </div>
  )

  return (
    <div style={{ fontFamily: 'system-ui', padding: 16, maxWidth: 1100, margin: '0 auto' }}>
      <h2>KotakAPI Trader {kotakIn ? '🟢 Kotak live' : '🔴 Kotak logged out'}</h2>
      {!kotakIn && <div>
        <input placeholder="TOTP" value={totp} onChange={(e) => setTotp(e.target.value)} />
        <button onClick={kotakLogin}>Kotak TOTP login</button>
      </div>}
      <button onClick={refresh}>Refresh</button> <span>{msg}</span>

      <h3>Order ticket (synthetic BO + native BO)</h3>
      <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
        <label>instrument<br /><select value={f.instrument} onChange={(e) => setInstrument(e.target.value)}>
          <option>NIFTY</option><option>SENSEX</option></select></label>
        <label>expiry<br /><select value={f.expiry} onChange={(e) => setExpiry(e.target.value)}>
          {expiries.map((x) => <option key={x} value={x}>{x}</option>)}
        </select></label>
        <label>ce_strike<br /><select value={f.ce_strike} onChange={(e) => setF({ ...f, ce_strike: e.target.value })}>
          {strikes.ce.map((x) => <option key={x} value={x}>{x}</option>)}
        </select></label>
        <label>pe_strike<br /><select value={f.pe_strike} onChange={(e) => setF({ ...f, pe_strike: e.target.value })}>
          {strikes.pe.map((x) => <option key={x} value={x}>{x}</option>)}
        </select></label>
        {['lots', 'target_points', 'stop_points', 'trail_distance'].map((k) => (
          <label key={k}>{k}<br /><input value={f[k]} onChange={(e) => setF({ ...f, [k]: e.target.value })} style={{ width: 110 }} /></label>
        ))}
        <label>entry_mode<br /><select value={f.entry_mode} onChange={(e) => setF({ ...f, entry_mode: e.target.value })}>
          <option>MKT</option><option>ML</option><option>L-1</option><option>L</option></select></label>
        <label>strategy<br /><select value={f.strategy} onChange={(e) => setF({ ...f, strategy: e.target.value })}>
          <option value="market_exit">market_exit</option><option value="trailing_stop">trailing_stop</option></select></label>
        <label>product<br /><select value={f.product} onChange={(e) => setF({ ...f, product: e.target.value })}>
          <option>MIS</option><option>BO</option><option>NRML</option></select></label>
      </div>
      <div style={{ marginTop: 8, display: 'flex', gap: 8 }}>
        <button onClick={() => place('CE')}>Buy CE</button>
        <button onClick={() => place('PE')}>Buy PE</button>
        <button onClick={() => place('BOTH')}>Buy Both (straddle)</button>
      </div>

      <h3>Synthetic / bracket legs</h3>
      <pre style={{ background: '#f5f5f5', padding: 8, overflow: 'auto' }}>{JSON.stringify(synthetic, null, 1)}</pre>
      <h3>Positions</h3>
      <pre style={{ background: '#f5f5f5', padding: 8 }}>{JSON.stringify(positions, null, 1)}</pre>
      <h3>Live order feed ({orders.length})</h3>
      <pre style={{ background: '#111', color: '#0f0', padding: 8, maxHeight: 260, overflow: 'auto' }}>{JSON.stringify(orders.slice(0, 10), null, 1)}</pre>
    </div>
  )
}
