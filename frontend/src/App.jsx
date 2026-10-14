import { useEffect, useState } from 'react'
import { api, wsUrl } from './api.js'

const blank = {
  instrument: 'NIFTY', expiry: '', ce_strike: '', pe_strike: '',
  lots: 1, target_points: 20, stop_points: 10, trail_distance: 5,
  entry_mode: 'MKT', strategy: 'market_exit', product: 'MIS',
}

const feedClass = (st = '') => {
  const s = st.toLowerCase()
  if (s.includes('complete') || s === 'filled') return 's-complete'
  if (s.includes('reject')) return 's-rejected'
  if (s.includes('cancel') || s.includes('expir')) return 's-cancelled'
  return 's-pending'
}

const idxPx = (ticks, key) => {
  const t = ticks[key]
  if (!t) return '—'
  return t.ltp ?? t.iv ?? '—'
}

export default function App() {
  const [token, setToken] = useState(localStorage.getItem('token') || '')
  const [login, setLogin] = useState({ username: '', password: '' })
  const [loginErr, setLoginErr] = useState('')
  const [totp, setTotp] = useState('')
  const [kotakIn, setKotakIn] = useState(false)
  const [kotakBusy, setKotakBusy] = useState(false)
  const [ticks, setTicks] = useState({})
  const [orders, setOrders] = useState([])
  const [synthetic, setSynthetic] = useState([])
  const [positions, setPositions] = useState({})
  const [f, setF] = useState(blank)
  const [expiries, setExpiries] = useState([])
  const [strikes, setStrikes] = useState({ ce: [], pe: [] })
  const [ltp, setLtp] = useState({ ce: null, pe: null })
  const [quoting, setQuoting] = useState(false)
  const [msg, setMsg] = useState(null)

  const auth = { headers: { Authorization: `Bearer ${token}` } }
  const say = (kind, text) => setMsg({ kind, text })

  /* live socket */
  useEffect(() => {
    if (!token) return
    const ws = new WebSocket(wsUrl())
    ws.onmessage = (e) => {
      try {
        const m = JSON.parse(e.data)
        if (m.kind === 'tick' && m.tick?.tk) setTicks((t) => ({ ...t, [m.tick.tk]: m.tick }))
        if (m.kind === 'order') setOrders((o) => [m.order, ...o].slice(0, 100))
      } catch { /* ignore */ }
    }
    return () => ws.close()
  }, [token])

  /* positions auto-refresh while Kotak session is live */
  useEffect(() => {
    if (!token || !kotakIn) return
    const id = setInterval(() => {
      api.get('/market/positions', auth).then(
        (r) => setPositions(r.data.positions || {}), () => {})
    }, 30000)
    return () => clearInterval(id)
  }, [token, kotakIn])

  /* ---- auth ---- */
  const doLogin = async () => {
    setLoginErr('')
    try {
      const r = await api.post('/auth/login', login)
      localStorage.setItem('token', r.data.token)
      setToken(r.data.token)
      say('info', 'App login OK. Now connect your Kotak session below.')
    } catch {
      setLoginErr('Login failed — check username / password.')
    }
  }
  const logout = () => { localStorage.removeItem('token'); setToken(''); setKotakIn(false) }

  const kotakLogin = async () => {
    setKotakBusy(true)
    try {
      await api.post('/auth/kotak/initiate', {}, auth)
      await api.post('/auth/kotak/complete', { totp }, auth)
      setKotakIn(true)
      say('ok', 'Kotak session live. Expiries loading…')
      await refresh(true)
    } catch (e) {
      say('err', `Kotak login failed: ${e.response?.data?.detail || e.message}`)
    } finally {
      setKotakBusy(false)
    }
  }

  /* ---- data ---- */
  const refresh = async (justLoggedIn = false) => {
    try {
      const [s, p, st] = await Promise.all([
        api.get('/orders/synthetic', auth),
        api.get('/market/positions', auth),
        api.get('/auth/kotak/status', auth),
      ])
      setSynthetic(s.data.orders || [])
      setPositions(p.data.positions || {})
      const live = !!st.data.logged_in
      setKotakIn(live)
      if (live) await loadExpiries(f.instrument, justLoggedIn ? '' : f.expiry)
    } catch (e) {
      say('err', `Refresh failed: ${e.response?.data?.detail || e.message}`)
    }
  }

  const loadExpiries = async (instrument, keep) => {
    try {
      const r = await api.get(`/market/expiries?instrument=${instrument}`, auth)
      const list = r.data.expiries || []
      setExpiries(list)
      const exp = keep && list.includes(keep) ? keep : list[0] || ''
      setF((v) => ({ ...v, expiry: exp, ce_strike: '', pe_strike: '' }))
      setLtp({ ce: null, pe: null })
      if (exp) await loadStrikes(instrument, exp)
      else setStrikes({ ce: [], pe: [] })
    } catch (e) {
      say('err', `Expiries failed: ${e.response?.data?.detail || e.message}`)
      setExpiries([])
    }
  }

  const loadStrikes = async (instrument, expiry) => {
    try {
      const r = await api.get(`/market/strikes?instrument=${instrument}&expiry=${expiry}&spot=0`, auth)
      const ce = r.data.ce || [], pe = r.data.pe || []
      setStrikes({ ce, pe })
      setF((v) => ({ ...v, ce_strike: ce[0] || '', pe_strike: pe[0] || '' }))
      setLtp({ ce: null, pe: null })
    } catch { /* keep old */ }
  }

  const quoteLeg = async (otype) => {
    const strike = otype === 'CE' ? f.ce_strike : f.pe_strike
    if (!f.expiry || !strike) return null
    setQuoting(true)
    try {
      const r = await api.get(
        `/market/quote?instrument=${f.instrument}&expiry=${f.expiry}&strike=${strike}&option_type=${otype}`, auth)
      const px = r.data.ltp
      setLtp((v) => ({ ...v, [otype.toLowerCase()]: px }))
      return px
    } catch (e) {
      say('err', `Quote failed (${otype} ${strike}): ${e.response?.data?.detail || e.message}`)
      return null
    } finally {
      setQuoting(false)
    }
  }

  /* ---- orders ---- */
  const needTicket = !kotakIn || !f.expiry

  const place = async (side) => {
    try {
      if (side === 'BOTH') {
        const ce = ltp.ce ?? await quoteLeg('CE')
        const pe = ltp.pe ?? await quoteLeg('PE')
        if (ce == null || pe == null) { say('err', 'Need both CE and PE quotes first (press ↻).'); return }
        const r = await api.post(`/orders/straddle?ce_ltp=${ce}&pe_ltp=${pe}`, f, auth)
        say('ok', `Straddle placed: ${r.data.legs.map((l) => l.symbol).join(' + ')}`)
      } else {
        const key = side.toLowerCase()
        const px = ltp[key] ?? await quoteLeg(side)
        if (px == null) { say('err', `Need a ${side} quote first (press ↻).`); return }
        const body = { ...f, option_type: side, strike: side === 'CE' ? f.ce_strike : f.pe_strike, entry_offset: 0 }
        const r = await api.post(`/orders/single?ref_ltp=${px}`, body, auth)
        say('ok', `Order placed: ${r.data.symbol} @ ${r.data.entry} → TGT ${r.data.target} / SL ${r.data.stop}`)
      }
      refresh()
    } catch (e) {
      say('err', `Order failed: ${e.response?.data?.detail || e.message}`)
    }
  }

  /* ---- render ---- */
  if (!token) return (
    <div className="page">
      <div className="login-wrap">
        <div className="card login-card">
          <h2>KotakAPI Trader</h2>
          <p className="sub">Sign in to reach the desk. (Kotak session connects after this.)</p>
          <div className="field"><label>Username</label>
            <input value={login.username} onChange={(e) => setLogin({ ...login, username: e.target.value })} /></div>
          <div className="field"><label>Password</label>
            <input type="password" value={login.password}
              onChange={(e) => setLogin({ ...login, password: e.target.value })}
              onKeyDown={(e) => e.key === 'Enter' && doLogin()} /></div>
          <button className="btn" onClick={doLogin}>Login</button>
          {loginErr && <p className="msg err" style={{ marginTop: 10 }}>{loginErr}</p>}
        </div>
      </div>
    </div>
  )

  return (
    <div className="page">
      <div className="topbar">
        <h1>KotakAPI Trader</h1>
        <span className={`pill ${kotakIn ? 'on' : 'off'}`}>
          <span className="dot" />{kotakIn ? 'Kotak live' : 'Kotak logged out'}
        </span>
        <span className="spacer" />
        <button className="btn ghost sm" onClick={() => refresh()}>Refresh</button>
        <button className="btn ghost sm" onClick={logout}>Logout</button>
      </div>

      <div className="tickers">
        <div className="ticker"><div className="sym">Nifty 50</div><div className="px">{idxPx(ticks, 'Nifty 50')}</div></div>
        <div className="ticker"><div className="sym">Sensex</div><div className="px">{idxPx(ticks, 'SENSEX')}</div></div>
      </div>

      {msg && <p className={`msg ${msg.kind}`}>{msg.text}</p>}

      {!kotakIn && (
        <div className="card">
          <h2>Connect Kotak session</h2>
          <p className="sub">Valid for today only — reconnect each morning.</p>
          <div className="totp-help">
            <b>No SMS will arrive — nothing to request.</b><br />
            Open your authenticator app (the same TOTP you use in Kotak Neo / TransTrader —
            Google Authenticator or the Kotak Neo app's TOTP section), read the current
            6-digit code and type it below. Codes rotate every 30 seconds, so submit promptly.
          </div>
          <div className="row">
            <div className="field" style={{ minWidth: 160 }}>
              <label>6-digit TOTP</label>
              <input value={totp} onChange={(e) => setTotp(e.target.value)} placeholder="e.g. 482916"
                onKeyDown={(e) => e.key === 'Enter' && kotakLogin()} />
            </div>
            <button className="btn" onClick={kotakLogin} disabled={kotakBusy || !totp}>
              {kotakBusy ? 'Connecting…' : 'Kotak TOTP login'}
            </button>
          </div>
        </div>
      )}

      <div className={`card ${needTicket ? 'disabled' : ''}`}>
        <h2>Order ticket — synthetic bracket</h2>
        <p className="sub">
          {kotakIn
            ? 'Entry fills → SL auto-placed. Target hit → market exit, or trailing SL if selected.'
            : 'Connect the Kotak session above to enable trading.'}
        </p>
        <div className="grid">
          <div className="field"><label>Instrument</label>
            <select value={f.instrument} disabled={!kotakIn}
              onChange={(e) => { const v = e.target.value; setF({ ...blank, instrument: v }); loadExpiries(v, ''); }}>
              <option>NIFTY</option><option>SENSEX</option>
            </select></div>
          <div className="field"><label>Expiry</label>
            <select value={f.expiry} disabled={!kotakIn || !expiries.length}
              onChange={(e) => { setF({ ...f, expiry: e.target.value }); loadStrikes(f.instrument, e.target.value); }}>
              {!f.expiry && <option value="">—</option>}
              {expiries.map((x) => <option key={x} value={x}>{x}</option>)}
            </select></div>
          <div className="field"><label>Lots</label>
            <input type="number" min="1" value={f.lots} disabled={!kotakIn}
              onChange={(e) => setF({ ...f, lots: e.target.value })} /></div>
          <div className="field"><label>Entry</label>
            <select value={f.entry_mode} disabled={!kotakIn}
              onChange={(e) => setF({ ...f, entry_mode: e.target.value })}>
              <option>MKT</option><option>ML</option><option>L-1</option><option>L</option>
            </select></div>
          <div className="field"><label>Target (pts)</label>
            <input type="number" value={f.target_points} disabled={!kotakIn}
              onChange={(e) => setF({ ...f, target_points: e.target.value })} /></div>
          <div className="field"><label>Stop (pts)</label>
            <input type="number" value={f.stop_points} disabled={!kotakIn}
              onChange={(e) => setF({ ...f, stop_points: e.target.value })} /></div>
          <div className="field"><label>Trail dist</label>
            <input type="number" step="0.5" value={f.trail_distance} disabled={!kotakIn}
              onChange={(e) => setF({ ...f, trail_distance: e.target.value })} /></div>
          <div className="field"><label>Exit strategy</label>
            <select value={f.strategy} disabled={!kotakIn}
              onChange={(e) => setF({ ...f, strategy: e.target.value })}>
              <option value="market_exit">Exit @ MKT on target</option>
              <option value="trailing_stop">Trail SL on target</option>
            </select></div>
          <div className="field"><label>Product</label>
            <select value={f.product} disabled={!kotakIn}
              onChange={(e) => setF({ ...f, product: e.target.value })}>
              <option>MIS</option><option>NRML</option><option>CNC</option>
            </select></div>
        </div>

        <div className="row" style={{ marginTop: 12 }}>
          <div className="field" style={{ minWidth: 150 }}><label>CE strike</label>
            <select value={f.ce_strike} disabled={!kotakIn || !strikes.ce.length}
              onChange={(e) => { setF({ ...f, ce_strike: e.target.value }); setLtp((v) => ({ ...v, ce: null })); }}>
              {!f.ce_strike && <option value="">—</option>}
              {strikes.ce.map((x) => <option key={x} value={x}>{x}</option>)}
            </select></div>
          <button className="btn ghost sm" disabled={!kotakIn || !f.ce_strike || quoting}
            onClick={() => quoteLeg('CE')}>↻ quote</button>
          <span className="ltp">{ltp.ce ?? '—'}</span>
          <div className="field" style={{ minWidth: 150 }}><label>PE strike</label>
            <select value={f.pe_strike} disabled={!kotakIn || !strikes.pe.length}
              onChange={(e) => { setF({ ...f, pe_strike: e.target.value }); setLtp((v) => ({ ...v, pe: null })); }}>
              {!f.pe_strike && <option value="">—</option>}
              {strikes.pe.map((x) => <option key={x} value={x}>{x}</option>)}
            </select></div>
          <button className="btn ghost sm" disabled={!kotakIn || !f.pe_strike || quoting}
            onClick={() => quoteLeg('PE')}>↻ quote</button>
          <span className="ltp">{ltp.pe ?? '—'}</span>
        </div>

        <div className="row" style={{ marginTop: 14 }}>
          <button className="btn buy" disabled={needTicket || !f.ce_strike} onClick={() => place('CE')}>Buy CE</button>
          <button className="btn buy" disabled={needTicket || !f.pe_strike} onClick={() => place('PE')}>Buy PE</button>
          <button className="btn" disabled={needTicket || !f.ce_strike || !f.pe_strike}
            onClick={() => place('BOTH')}>Buy both (straddle)</button>
        </div>
      </div>

      <div className="card">
        <h2>Bracket legs</h2>
        <p className="sub">Synthetic SL / target state per entry order.</p>
        {synthetic.length === 0 ? <div className="empty">No working legs.</div> : (
          <table className="tbl"><thead><tr>
            <th>Symbol</th><th>Qty</th><th>Target</th><th>Stop</th><th>Strategy</th><th>Status</th>
          </tr></thead><tbody>
            {synthetic.map((o) => (
              <tr key={o.entry_order_id}>
                <td>{o.trading_symbol}</td><td>{o.quantity}</td>
                <td>{o.target_price}</td><td>{o.stop_loss_price}</td>
                <td>{o.strategy}</td>
                <td><span className={`st ${o.status}`}>{o.status}</span></td>
              </tr>
            ))}
          </tbody></table>
        )}
      </div>

      <div className="card">
        <h2>Positions</h2>
        <p className="sub">Net broker positions (auto-refreshes every 30s).</p>
        {Object.keys(positions).length === 0 ? <div className="empty">Flat — no open positions.</div> : (
          <table className="tbl"><thead><tr><th>Symbol</th><th>Qty</th><th>Avg price</th></tr></thead>
            <tbody>{Object.entries(positions).map(([s, p]) => (
              <tr key={s}><td>{s}</td><td>{p.qty}</td><td>{p.avg_prc}</td></tr>
            ))}</tbody></table>
        )}
      </div>

      <div className="card">
        <h2>Live order feed ({orders.length})</h2>
        <p className="sub">Pushed over the exchange websocket via the server.</p>
        <div className="feed">
          {orders.length === 0 && <div className="empty">No order updates yet this session.</div>}
          {orders.slice(0, 30).map((o, i) => (
            <div key={`${o.nOrdNo}-${i}`} className={`ord ${feedClass(o.ordSt)}`}>
              <span className="sym">{o.trdSym}</span>
              <span className={`side-${o.trnsTp}`}>{o.trnsTp === 'B' ? 'BUY' : o.trnsTp === 'S' ? 'SELL' : o.trnsTp} {o.qty || o.brdLtQty}</span>
              <span>@ {o.prc}</span>
              <span className="status">{(o.ordSt || '?').toUpperCase()}</span>
              {o.rejRsn && o.rejRsn !== '--' && <span className="reason">Reason: {o.rejRsn}</span>}
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
