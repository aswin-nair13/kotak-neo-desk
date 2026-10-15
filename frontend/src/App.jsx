import { useEffect, useMemo, useRef, useState } from 'react'
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

const num = (v) => {
  const n = parseFloat(v)
  return Number.isFinite(n) ? n : 0
}

const stepFor = (instrument) => (instrument === 'SENSEX' ? 100 : 50)

const atmOf = (spot, step) => {
  if (!spot || spot <= 0) return null
  return step * Math.round(spot / step)
}

const filterWindow = (list, spot, step, width = 10) => {
  if (!Array.isArray(list) || !list.length) return []
  const atm = atmOf(spot, step)
  if (atm == null) return list
  const lo = atm - width * step, hi = atm + width * step
  const out = list.filter((s) => {
    const n = parseInt(s, 10)
    return Number.isFinite(n) && n >= lo && n <= hi
  })
  return out.length ? out : list
}

const closestStrike = (list, atm) => {
  if (!list.length) return ''
  if (atm == null) return list[Math.min(Math.floor(list.length / 2), list.length - 1)] || ''
  let best = list[0], bd = Infinity
  for (const s of list) {
    const d = Math.abs(parseInt(s, 10) - atm)
    if (d < bd) { bd = d; best = s }
  }
  return best
}

const symOf = (instrument, expiry, strike, otype) => `${instrument}${expiry}${strike}${otype}`

// click-to-fill latency formatting (client-measured wall clock)
const fmtLat = (ms) => {
  if (ms == null || !Number.isFinite(ms) || ms < 0) return null
  if (ms < 1000) return `${Math.round(ms)}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${(ms / 60000).toFixed(1)}m`
}

export default function App() {
  const [token, setToken] = useState(localStorage.getItem('token') || '')
  const [login, setLogin] = useState({ username: '', password: '' })
  const [loginErr, setLoginErr] = useState('')
  const [totp, setTotp] = useState('')
  const [kotakIn, setKotakIn] = useState(false)
  const [kotakBusy, setKotakBusy] = useState(false)
  const [egressIp, setEgressIp] = useState(null)
  const [ticks, setTicks] = useState({})
  const [orders, setOrders] = useState([])
  const [synthetic, setSynthetic] = useState([])
  const [positions, setPositions] = useState({})
  const [f, setF] = useState(blank)
  const [expiries, setExpiries] = useState([])
  const [strikes, setStrikes] = useState({ ce: [], pe: [], atm: null })
  const [ltp, setLtp] = useState({ ce: null, pe: null })
  const [quoting, setQuoting] = useState(false)
  const [msg, setMsg] = useState(null)
  // multi-strike watchlist: [{ key, otype, strike }]
  const [legs, setLegs] = useState([])
  // REST fallback LTPs until ws ticks arrive: { symbol: ltp }
  const [px, setPx] = useState({})
  const [busyKey, setBusyKey] = useState('')
  const [exiting, setExiting] = useState('')
  // click-to-fill latency: clickRef maps nOrdNo -> { clickMs, brokerMs, symbol };
  // lat maps nOrdNo -> { totalMs, brokerMs } computed when the ws update lands.
  // totalMs uses the browser clock only (no server skew); brokerMs is the
  // server-internal place_order RTT. Two Date.now() calls per order — ~ns.
  const clickRef = useRef({})
  const [lat, setLat] = useState({})

  const rememberClick = (entryId, clickMs, timing, symbol) => {
    if (!entryId) return
    clickRef.current[String(entryId)] = {
      clickMs, brokerMs: timing?.broker_rtt_ms ?? null, symbol: symbol || '',
    }
  }

  const auth = { headers: { Authorization: `Bearer ${token}` } }
  const say = (kind, text) => setMsg({ kind, text })

  /* live socket — index ticks by token AND by symbol (ts) so option legs resolve */
  useEffect(() => {
    if (!token) return
    const ws = new WebSocket(wsUrl())
    ws.onmessage = (e) => {
      try {
        const m = JSON.parse(e.data)
        if (m.kind === 'tick' && m.tick?.tk) {
          const t = m.tick
          setTicks((prev) => {
            const next = { ...prev, [t.tk]: t }
            if (t.ts && t.ts !== t.tk) next[t.ts] = t
            return next
          })
        }
        if (m.kind === 'order' && m.order) {
          const od = m.order
          setOrders((o) => [od, ...o].slice(0, 100))
          const hit = clickRef.current[String(od.nOrdNo)]
          if (hit) {
            const totalMs = Date.now() - hit.clickMs
            setLat((prev) => {
              const next = { ...prev, [String(od.nOrdNo)]: { totalMs, brokerMs: hit.brokerMs } }
              const keys = Object.keys(next)
              if (keys.length > 200) {
                // prune oldest (insertion-ordered) to bound memory
                keys.slice(0, keys.length - 200).forEach((k) => delete next[k])
              }
              return next
            })
          }
        }
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

  const spot = useMemo(() => {
    const key = f.instrument === 'SENSEX' ? 'SENSEX' : 'Nifty 50'
    return num(idxPx(ticks, key) === '—' ? 0 : (ticks[key]?.ltp ?? ticks[key]?.iv ?? 0))
  }, [ticks, f.instrument])

  const step = stepFor(f.instrument)
  const atm = strikes.atm ?? atmOf(spot, step)

  // visible dropdown options: backend already sends ATM±10 when spot was known
  // at fetch time; re-apply the window client-side so a full list (fetched
  // before the index tick arrived) still collapses to ±10 once spot is live.
  const visCe = useMemo(() => filterWindow(strikes.ce, spot, step), [strikes.ce, spot, step])
  const visPe = useMemo(() => filterWindow(strikes.pe, spot, step), [strikes.pe, spot, step])

  const legLtp = (symbol) => ticks[symbol]?.ltp ?? ticks[symbol]?.iv ?? px[symbol] ?? null

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
      try {
        const ip = await api.get('/auth/kotak/egress-ip', auth)
        setEgressIp(ip.data.ip || null)
      } catch { setEgressIp(null) }
    } catch (e) {
      say('err', `Kotak login failed: ${e.response?.data?.detail || e.message}`)
    } finally {
      setKotakBusy(false)
    }
  }

  /* ---- data (selection-preserving) ---- */
  // Light refresh: synthetic + positions + session only. Never touches
  // expiry/strike dropdowns or the watchlist.
  const refreshData = async () => {
    try {
      const [s, p, st] = await Promise.all([
        api.get('/orders/synthetic', auth),
        api.get('/market/positions', auth),
        api.get('/auth/kotak/status', auth),
      ])
      setSynthetic(s.data.orders || [])
      setPositions(p.data.positions || {})
      setKotakIn(!!st.data.logged_in)
    } catch (e) {
      say('err', `Refresh failed: ${e.response?.data?.detail || e.message}`)
    }
  }

  const refresh = async (justLoggedIn = false) => {
    await refreshData()
    if (kotakIn || justLoggedIn) {
      try {
        const st = await api.get('/auth/kotak/status', auth)
        if (st.data.logged_in) {
          setKotakIn(true)
          await loadExpiries(f.instrument, justLoggedIn ? '' : f.expiry, true)
        }
      } catch { /* session probe failed — keep form as is */ }
    }
  }

  const loadExpiries = async (instrument, keep, preserveStrikes = true) => {
    try {
      const r = await api.get(`/market/expiries?instrument=${instrument}`, auth)
      const list = r.data.expiries || []
      setExpiries(list)
      const exp = keep && list.includes(keep) ? keep : list[0] || ''
      const expiryChanged = exp !== f.expiry
      setF((v) => ({ ...v, instrument, expiry: exp }))
      if (expiryChanged) {
        // instrument/expiry change resets working selection (per spec)
        setLegs([])
        setPx({})
        setLtp({ ce: null, pe: null })
      }
      if (exp) await loadStrikes(instrument, exp, preserveStrikes && !expiryChanged)
      else setStrikes({ ce: [], pe: [], atm: null })
    } catch (e) {
      say('err', `Expiries failed: ${e.response?.data?.detail || e.message}`)
      setExpiries([])
    }
  }

  const loadStrikes = async (instrument, expiry, preserve = true) => {
    try {
      const key = instrument === 'SENSEX' ? 'SENSEX' : 'Nifty 50'
      const t = ticks[key]
      const liveSpot = num(t?.ltp ?? t?.iv ?? 0)
      const r = await api.get(
        `/market/strikes?instrument=${instrument}&expiry=${expiry}&spot=${liveSpot || 0}`, auth)
      const ce = r.data.ce || [], pe = r.data.pe || []
      setStrikes({ ce, pe, atm: r.data.atm ?? null })
      setF((v) => {
        // preserve user selection when the strike still exists (order placed,
        // manual refresh, quote) — only default when empty/invalid
        const keepCe = preserve && v.ce_strike && ce.includes(String(v.ce_strike))
        const keepPe = preserve && v.pe_strike && pe.includes(String(v.pe_strike))
        const stepL = stepFor(instrument)
        const atmL = r.data.atm ?? atmOf(liveSpot, stepL)
        return {
          ...v, instrument, expiry,
          ce_strike: keepCe ? v.ce_strike : (closestStrike(filterWindow(ce, liveSpot, stepL), atmL) || ce[0] || ''),
          pe_strike: keepPe ? v.pe_strike : (closestStrike(filterWindow(pe, liveSpot, stepL), atmL) || pe[0] || ''),
        }
      })
      if (!preserve) setLtp({ ce: null, pe: null })
    } catch { /* keep old */ }
  }

  // Once the index tick arrives, a strike list fetched with spot=0 (full
  // chain) is narrowed to ATM±10 without losing the user's selection.
  useEffect(() => {
    if (!kotakIn || !f.expiry) return
    if (!spot) return
    if (strikes.ce.length > 25) loadStrikes(f.instrument, f.expiry, true)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [spot > 0, kotakIn, f.expiry])

  /* ---- watchlist websocket subscription + REST fallback quotes ---- */
  useEffect(() => {
    if (!kotakIn || !f.expiry || !legs.length) return
    const body = {
      legs: legs.map((l) => ({
        instrument: f.instrument, expiry: f.expiry,
        strike: String(l.strike), option_type: l.otype,
      })),
    }
    api.post('/market/watch', body, auth).catch(() => {})
    api.post('/market/batch-quotes', body, auth).then((r) => {
      const ltps = r.data.ltps || {}
      setPx((prev) => ({ ...prev, ...ltps }))
      setLtp((v) => {
        const next = { ...v }
        const ceSym = f.ce_strike ? symOf(f.instrument, f.expiry, f.ce_strike, 'CE') : null
        const peSym = f.pe_strike ? symOf(f.instrument, f.expiry, f.pe_strike, 'PE') : null
        if (ceSym && ltps[ceSym] != null) next.ce = ltps[ceSym]
        if (peSym && ltps[peSym] != null) next.pe = ltps[peSym]
        return next
      })
    }).catch(() => {})
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [JSON.stringify(legs), f.instrument, f.expiry, kotakIn])

  const quoteFor = async (otype, strike) => {
    if (!f.expiry || !strike) return null
    setQuoting(true)
    try {
      const r = await api.get(
        `/market/quote?instrument=${f.instrument}&expiry=${f.expiry}&strike=${strike}&option_type=${otype}`, auth)
      const price = r.data.ltp
      const sym = symOf(f.instrument, f.expiry, strike, otype)
      setPx((v) => ({ ...v, [sym]: price }))
      return price
    } catch (e) {
      say('err', `Quote failed (${otype} ${strike}): ${e.response?.data?.detail || e.message}`)
      return null
    } finally {
      setQuoting(false)
    }
  }

  const quoteLeg = async (otype) => {
    const strike = otype === 'CE' ? f.ce_strike : f.pe_strike
    const price = await quoteFor(otype, strike)
    if (price != null) setLtp((v) => ({ ...v, [otype.toLowerCase()]: price }))
    return price
  }

  /* ---- orders (buy AND sell) ---- */
  const needTicket = !kotakIn || !f.expiry

  const place = async (otype, side = 'B') => {
    const tag = side === 'S' ? 'Sell' : 'Buy'
    const tClick = Date.now()
    try {
      if (otype === 'BOTH') {
        const ceSym = symOf(f.instrument, f.expiry, f.ce_strike, 'CE')
        const peSym = symOf(f.instrument, f.expiry, f.pe_strike, 'PE')
        const ce = legLtp(ceSym) ?? ltp.ce ?? await quoteLeg('CE')
        const pe = legLtp(peSym) ?? ltp.pe ?? await quoteLeg('PE')
        if (ce == null || pe == null) { say('err', 'Need both CE and PE quotes first (press ↻).'); return }
        const body = { ...f, side }
        const r = await api.post(`/orders/straddle?ce_ltp=${ce}&pe_ltp=${pe}`, body, auth)
        const apiMs = Date.now() - tClick
        for (const l of r.data.legs || []) rememberClick(l.entry_order_id, tClick, l.timing, l.symbol)
        const b0 = r.data.legs?.[0]?.timing?.broker_rtt_ms
        say('ok', `${tag} straddle placed: ${(r.data.legs || []).map((l) => l.symbol).join(' + ')} (api ${fmtLat(apiMs)}${b0 != null ? `, broker ${fmtLat(b0)}` : ''} — fill time lands in the feed below)`)
      } else {
        const strike = otype === 'CE' ? f.ce_strike : f.pe_strike
        const sym = symOf(f.instrument, f.expiry, strike, otype)
        const price = legLtp(sym) ?? ltp[otype.toLowerCase()] ?? await quoteLeg(otype)
        if (price == null) { say('err', `Need a ${otype} quote first (press ↻).`); return }
        const body = { ...f, option_type: otype, strike: String(strike), entry_offset: 0, side }
        const r = await api.post(`/orders/single?ref_ltp=${price}`, body, auth)
        const apiMs = Date.now() - tClick
        rememberClick(r.data.entry_order_id, tClick, r.data.timing, r.data.symbol)
        const b = r.data.timing?.broker_rtt_ms
        say('ok', `${tag} ${r.data.symbol} @ ${r.data.entry} → TGT ${r.data.target} / SL ${r.data.stop} (api ${fmtLat(apiMs)}${b != null ? `, broker ${fmtLat(b)}` : ''} — fill time lands in the feed below)`)
      }
      refreshData() // keep dropdowns + watchlist intact
    } catch (e) {
      say('err', `Order failed: ${e.response?.data?.detail || e.message}`)
    }
  }

  const placeRow = async (leg, side = 'B') => {
    const key = `${leg.otype}-${leg.strike}-${side}`
    setBusyKey(key)
    const tClick = Date.now()
    try {
      const sym = symOf(f.instrument, f.expiry, leg.strike, leg.otype)
      const price = legLtp(sym) ?? await quoteFor(leg.otype, leg.strike)
      if (price == null) { say('err', `Need a ${leg.otype} ${leg.strike} quote first.`); return }
      const body = {
        ...f, option_type: leg.otype, strike: String(leg.strike),
        entry_offset: 0, side,
      }
      const r = await api.post(`/orders/single?ref_ltp=${price}`, body, auth)
      const apiMs = Date.now() - tClick
      rememberClick(r.data.entry_order_id, tClick, r.data.timing, r.data.symbol)
      const b = r.data.timing?.broker_rtt_ms
      say('ok', `${side === 'S' ? 'Sell' : 'Buy'} ${r.data.symbol} @ ${r.data.entry} → TGT ${r.data.target} / SL ${r.data.stop} (api ${fmtLat(apiMs)}${b != null ? `, broker ${fmtLat(b)}` : ''})`)
      refreshData()
    } catch (e) {
      say('err', `Order failed: ${e.response?.data?.detail || e.message}`)
    } finally {
      setBusyKey('')
    }
  }

  const addLeg = (otype) => {
    const strike = otype === 'CE' ? f.ce_strike : f.pe_strike
    if (!strike) return
    const key = `${otype}-${strike}`
    setLegs((v) => (v.some((l) => l.key === key) ? v : [...v, { key, otype, strike: String(strike) }]))
  }

  const exitPosition = async (symbol) => {
    if (!window.confirm(`Flatten ${symbol} at market and cancel its SL/target?`)) return
    setExiting(symbol)
    const tClick = Date.now()
    try {
      const r = await api.post('/orders/exit-position', { symbol }, auth)
      const apiMs = Date.now() - tClick
      rememberClick(r.data.exit_order?.nOrdNo, tClick, r.data.timing, r.data.symbol)
      const b = r.data.timing?.broker_rtt_ms
      say('ok', `Exited ${r.data.symbol} (${r.data.exit_side} ${r.data.quantity}) — ${r.data.closed_legs} bracket leg(s) closed. (api ${fmtLat(apiMs)}${b != null ? `, broker ${fmtLat(b)}` : ''})`)
      refreshData()
    } catch (e) {
      say('err', `Exit failed: ${e.response?.data?.detail || e.message}`)
    } finally {
      setExiting('')
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
        {atm != null && (
          <div className="ticker"><div className="sym">{f.instrument} ATM (±10×{step})</div><div className="px">{atm}</div></div>
        )}
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

      {kotakIn && (
        <div className="card">
          <h2>Kotak session</h2>
          <p className="sub">
            Kotak sees this server as <code className="inline">{egressIp || '…resolving'}</code>.
            Since Apr-2026, <b>order APIs accept only whitelisted static IPv4</b> (NEO app → Trade API → Add IP,
            max 2, change allowed weekly) — and the session must be created from that same IP.
            Anything else fails orders with <code className="inline">100008 unauthorized</code> while
            quotes/positions keep working. Home broadband dynamic IPs will NOT work for orders;
            use an EC2 Elastic IP (static) for the live setup.
          </p>
        </div>
      )}

      <div className={`card ${needTicket ? 'disabled' : ''}`}>
        <h2>Order ticket — synthetic bracket</h2>
        <p className="sub">
          {kotakIn
            ? 'Entry fills → SL auto-placed from fill price. Target hit → market exit, or trailing SL if selected. Selection survives orders/refresh.'
            : 'Connect the Kotak session above to enable trading.'}
        </p>
        <div className="grid">
          <div className="field"><label>Instrument</label>
            <select value={f.instrument} disabled={!kotakIn}
              onChange={(e) => { const v = e.target.value; setF({ ...blank, instrument: v }); setLegs([]); setPx({}); loadExpiries(v, ''); }}>
              <option>NIFTY</option><option>SENSEX</option>
            </select></div>
          <div className="field"><label>Expiry</label>
            <select value={f.expiry} disabled={!kotakIn || !expiries.length}
              onChange={(e) => {
                const exp = e.target.value
                setF({ ...f, expiry: exp })
                setLegs([]); setPx({}); setLtp({ ce: null, pe: null })
                loadStrikes(f.instrument, exp, false)
              }}>
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
          <div className="field" style={{ minWidth: 150 }}><label>CE strike (ATM ±10)</label>
            <select value={f.ce_strike} disabled={!kotakIn || !visCe.length}
              onChange={(e) => { setF({ ...f, ce_strike: e.target.value }); setLtp((v) => ({ ...v, ce: null })); }}>
              {!f.ce_strike && <option value="">—</option>}
              {visCe.map((x) => <option key={x} value={x}>{x}</option>)}
            </select></div>
          <button className="btn ghost sm" disabled={!kotakIn || !f.ce_strike || quoting}
            onClick={() => quoteLeg('CE')}>↻ quote</button>
          <span className="ltp">{(f.ce_strike && legLtp(symOf(f.instrument, f.expiry, f.ce_strike, 'CE'))) ?? ltp.ce ?? '—'}</span>
          <button className="btn ghost sm" disabled={!kotakIn || !f.ce_strike} onClick={() => addLeg('CE')}>+ list</button>
          <div className="field" style={{ minWidth: 150 }}><label>PE strike (ATM ±10)</label>
            <select value={f.pe_strike} disabled={!kotakIn || !visPe.length}
              onChange={(e) => { setF({ ...f, pe_strike: e.target.value }); setLtp((v) => ({ ...v, pe: null })); }}>
              {!f.pe_strike && <option value="">—</option>}
              {visPe.map((x) => <option key={x} value={x}>{x}</option>)}
            </select></div>
          <button className="btn ghost sm" disabled={!kotakIn || !f.pe_strike || quoting}
            onClick={() => quoteLeg('PE')}>↻ quote</button>
          <span className="ltp">{(f.pe_strike && legLtp(symOf(f.instrument, f.expiry, f.pe_strike, 'PE'))) ?? ltp.pe ?? '—'}</span>
          <button className="btn ghost sm" disabled={!kotakIn || !f.pe_strike} onClick={() => addLeg('PE')}>+ list</button>
        </div>

        <div className="row" style={{ marginTop: 14 }}>
          <button className="btn buy" disabled={needTicket || !f.ce_strike} onClick={() => place('CE', 'B')}>Buy CE</button>
          <button className="btn sell" disabled={needTicket || !f.ce_strike} onClick={() => place('CE', 'S')}>Sell CE</button>
          <button className="btn buy" disabled={needTicket || !f.pe_strike} onClick={() => place('PE', 'B')}>Buy PE</button>
          <button className="btn sell" disabled={needTicket || !f.pe_strike} onClick={() => place('PE', 'S')}>Sell PE</button>
          <button className="btn" disabled={needTicket || !f.ce_strike || !f.pe_strike}
            onClick={() => place('BOTH', 'B')}>Buy both (straddle)</button>
          <button className="btn ghost" disabled={needTicket || !f.ce_strike || !f.pe_strike}
            onClick={() => place('BOTH', 'S')}>Sell both</button>
        </div>
      </div>

      <div className="card">
        <h2>Strike list — instant execution ({legs.length})</h2>
        <p className="sub">Add strikes above, then Buy/Sell each row directly. Live websocket LTP per leg; list survives orders and refreshes, clears only on instrument/expiry change.</p>
        {legs.length === 0 ? <div className="empty">Empty — pick a CE/PE strike above and press “+ list”.</div> : (
          <table className="tbl"><thead><tr>
            <th>Contract</th><th>Type</th><th>Strike</th><th>Live LTP</th><th>Buy</th><th>Sell</th><th></th>
          </tr></thead><tbody>
            {legs.map((l) => {
              const sym = symOf(f.instrument, f.expiry, l.strike, l.otype)
              const live = legLtp(sym)
              return (
                <tr key={l.key}>
                  <td>{sym}</td><td>{l.otype}</td><td>{l.strike}</td>
                  <td className="ltp">{live ?? '…'}</td>
                  <td><button className="btn buy sm" disabled={needTicket || busyKey === `${l.key}-B`}
                    onClick={() => placeRow(l, 'B')}>{busyKey === `${l.key}-B` ? '…' : 'Buy'}</button></td>
                  <td><button className="btn sell sm" disabled={needTicket || busyKey === `${l.key}-S`}
                    onClick={() => placeRow(l, 'S')}>{busyKey === `${l.key}-S` ? '…' : 'Sell'}</button></td>
                  <td><button className="btn ghost sm"
                    onClick={() => setLegs((v) => v.filter((x) => x.key !== l.key))}>✕</button></td>
                </tr>
              )
            })}
          </tbody></table>
        )}
      </div>

      <div className="card">
        <h2>Bracket legs</h2>
        <p className="sub">Synthetic SL / target state per entry order. Fill lat = broker accept → exchange fill (server clock).</p>
        {synthetic.length === 0 ? <div className="empty">No working legs.</div> : (
          <table className="tbl"><thead><tr>
            <th>Symbol</th><th>Side</th><th>Qty</th><th>Target</th><th>Stop</th><th>Strategy</th><th>Status</th><th>Fill lat</th>
          </tr></thead><tbody>
            {synthetic.map((o) => (
              <tr key={o.entry_order_id}>
                <td>{o.trading_symbol}</td>
                <td>{o.side === 'S' ? 'SELL' : 'BUY'}</td>
                <td>{o.quantity}</td>
                <td>{o.target_price}</td><td>{o.stop_loss_price}</td>
                <td>{o.strategy}</td>
                <td><span className={`st ${o.status}`}>{o.status}</span></td>
                <td className="ltp">{fmtLat(o.fill_latency_ms) ?? '—'}</td>
              </tr>
            ))}
          </tbody></table>
        )}
      </div>

      <div className="card">
        <h2>Positions</h2>
        <p className="sub">Net broker positions (auto-refreshes every 30s). Exit flattens at market and cancels that symbol's entry/SL + synthetic target.</p>
        {Object.keys(positions).length === 0 ? <div className="empty">Flat — no open positions.</div> : (
          <table className="tbl"><thead><tr><th>Symbol</th><th>Qty</th><th>Avg price</th><th></th></tr></thead>
            <tbody>{Object.entries(positions).map(([s, p]) => (
              <tr key={s}><td>{s}</td><td>{p.qty}</td><td>{p.avg_prc}</td>
                <td><button className="btn sell sm" disabled={exiting === s}
                  onClick={() => exitPosition(s)}>{exiting === s ? 'Exiting…' : 'Exit'}</button></td>
              </tr>
            ))}</tbody></table>
        )}
      </div>

      <div className="card">
        <h2>Live order feed ({orders.length})</h2>
        <p className="sub">Pushed over the exchange websocket via the server. ⚡ = your click → this update (browser clock); broker = server-internal place_order RTT.</p>
        <div className="feed">
          {orders.length === 0 && <div className="empty">No order updates yet this session.</div>}
          {orders.slice(0, 30).map((o, i) => {
            const l = lat[String(o.nOrdNo)]
            const total = l ? fmtLat(l.totalMs) : null
            const brk = l?.brokerMs != null ? fmtLat(l.brokerMs) : null
            return (
              <div key={`${o.nOrdNo}-${i}`} className={`ord ${feedClass(o.ordSt)}`}>
                <span className="sym">{o.trdSym}</span>
                <span className={`side-${o.trnsTp}`}>{o.trnsTp === 'B' ? 'BUY' : o.trnsTp === 'S' ? 'SELL' : o.trnsTp} {o.qty || o.brdLtQty}</span>
                <span>@ {o.prc}</span>
                <span className="status">{(o.ordSt || '?').toUpperCase()}</span>
                {total && <span className="lat" title="click → this update (total) + server broker RTT">⚡ {total}{brk ? ` (brk ${brk})` : ''}</span>}
                {o.rejRsn && o.rejRsn !== '--' && <span className="reason">Reason: {o.rejRsn}</span>}
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
