import { useEffect, useMemo, useRef, useState } from 'react'
import { api, wsUrl } from './api.js'

const blank = {
  instrument: 'NIFTY', expiry: '', ce_strike: '', pe_strike: '',
  lots: 1, target_points: 20, stop_points: 10, trail_distance: 5,
  entry_mode: 'MKT', strategy: 'market_exit', product: 'MIS',
  offset: 1, ce_limit: '', pe_limit: '',
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

// M5: how long a placed order keeps its button locked if no terminal order-feed
// event arrives (dropped feed safety net).
const INFLIGHT_MS = 30000

// compact, broker-safe per-click idempotency tag (Kotak echoes it as GuiOrdId)
const newTag = () => {
  const raw = (crypto.randomUUID && crypto.randomUUID()) || `${Date.now()}${Math.random()}`
  return `t${raw.replace(/[^a-z0-9]/gi, '').slice(0, 15)}`
}

// One watch session per browser tab. Keeping it in sessionStorage means a
// remount/refresh reuses the same server-side ref instead of leaking a new one;
// the backend expires it if the tab disappears without a clean unwatch.
const getSessionId = () => {
  const key = 'sfeed-watch-session'
  try {
    const cur = sessionStorage.getItem(key)
    if (cur) return cur
    const next = (crypto.randomUUID && crypto.randomUUID()) || `s${Date.now()}`
    sessionStorage.setItem(key, next)
    return next
  } catch {
    return (crypto.randomUUID && crypto.randomUUID()) || `s${Date.now()}`
  }
}

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
  const [masterOk, setMasterOk] = useState(false)
  const [kotakBusy, setKotakBusy] = useState(false)
  const [egressIp, setEgressIp] = useState(null)
  const [health, setHealth] = useState(null)
  const [feedEpoch, setFeedEpoch] = useState(0)
  const [ticks, setTicks] = useState({})
  const [orders, setOrders] = useState([])
  const [synthetic, setSynthetic] = useState([])
  const [positions, setPositions] = useState({})
  // Plan 06 WP7: a failed/stale positions read keeps the last good rows and
  // shows an explicit banner; it is never rendered as a flat account.
  const [posMeta, setPosMeta] = useState({ ok: true, stale: false, error: '', at: null })
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
  // A failed batch quote is surfaced as stale/unavailable, never blank or 0.
  const [pxMeta, setPxMeta] = useState({ stale: false, error: '', at: null })
  const [busyKey, setBusyKey] = useState('')
  const [exiting, setExiting] = useState('')
  const [forcing, setForcing] = useState('')
  // Plan 06 WP6: broker order book (structured, freshness-aware) + per-action
  // in-flight locks. A failed/stale read is shown as an error, never an empty
  // table that looks like a flat book.
  const [book, setBook] = useState({ ok: true, orders: [], count: 0, stale: false })
  const [bookBusy, setBookBusy] = useState({})
  // One poll at a time: a slow/wedged read must not stack 5s requests (and the
  // backend worker read has its own short timeout).
  const bookLoadingRef = useRef(false)
  // click-to-fill latency: clickRef maps nOrdNo -> { clickMs, brokerMs, symbol };
  // lat maps nOrdNo -> { totalMs, brokerMs } computed when the ws update lands.
  // totalMs uses the browser clock only (no server skew); brokerMs is the
  // server-internal place_order RTT. Two Date.now() calls per order — ~ns.
  const clickRef = useRef({})
  const [lat, setLat] = useState({})
  // M5 in-flight lock: key -> true while the order is being placed/awaited.
  // A click locks the triggering button(s); they re-enable when the entry hits
  // a terminal state on the order feed, or after INFLIGHT_MS.
  const [placing, setPlacing] = useState({})
  const placeTimers = useRef({})
  const pendingIds = useRef({}) // entry order id -> button lock key

  const releaseKey = (key) => {
    const t = placeTimers.current[key]
    if (t) { clearTimeout(t); delete placeTimers.current[key] }
    setPlacing((p) => {
      if (!p[key]) return p
      const n = { ...p }; delete n[key]; return n
    })
  }
  const lockKey = (key) => {
    setPlacing((p) => (p[key] ? p : { ...p, [key]: true }))
    if (placeTimers.current[key]) clearTimeout(placeTimers.current[key])
    placeTimers.current[key] = setTimeout(() => releaseKey(key), INFLIGHT_MS)
  }
  const notePending = (key, ids) => {
    for (const id of ids) if (id) pendingIds.current[String(id)] = key
  }

  const rememberClick = (entryId, clickMs, timing, symbol) => {
    if (!entryId) return
    clickRef.current[String(entryId)] = {
      clickMs, brokerMs: timing?.broker_rtt_ms ?? null, symbol: symbol || '',
    }
  }

  const auth = { headers: { Authorization: `Bearer ${token}` } }
  const say = (kind, text) => setMsg({ kind, text })
  // Server-side placement warnings (WP11 passive-limit guard); shown but never
  // treated as a failure — the order is live.
  const warnText = (warnings) => (
    warnings && warnings.length ? ` Warning: ${warnings.join('; ')}` : ''
  )

  const applyPositions = (r) => {
    setPositions(r.data.positions || {})
    setPosMeta({ ok: true, stale: false, error: '', at: Date.now() })
  }
  const positionsFailed = (e) => {
    const d = e?.response?.data
    setPosMeta((m) => ({
      ...m, ok: false, stale: true,
      error: d?.error || d?.detail || e?.message || 'positions unavailable',
    }))
  }

  const loadOrderbook = async () => {
    if (bookLoadingRef.current) return
    bookLoadingRef.current = true
    try {
      const r = await api.get('/orders/orderbook', auth)
      setBook(r.data)
    } catch (e) {
      const d = e.response?.data
      setBook({
        ok: false, orders: [], count: 0, stale: true,
        error: d?.error || d?.detail || e.message,
        fetched_at_ms: d?.fetched_at_ms ?? null,
      })
    } finally {
      bookLoadingRef.current = false
    }
  }

  /* live socket — index ticks by token AND by symbol (ts) so option legs resolve */
  useEffect(() => {
    if (!token) return
    let stopped = false
    let retry = null
    let ws = null
    const connect = () => {
      if (stopped) return
      ws = new WebSocket(wsUrl())
      // First frame authenticates the feed; the server sends nothing before it.
      ws.onopen = () => {
        if (stopped) {
          ws.close()
          return
        }
        ws.send(JSON.stringify({ token }))
        // A fresh backend has no watch refs; let the watch effect re-register
        // this tab immediately instead of waiting for the TTL/heartbeat.
        setFeedEpoch((n) => n + 1)
      }
      ws.onclose = (e) => {
        // 1008 = auth/origin policy rejection -> drop the stale token, re-login.
        if (e.code === 1008) {
          localStorage.removeItem('token')
          setToken('')
          setKotakIn(false)
          return
        }
        if (!stopped) retry = setTimeout(connect, 3000)
      }
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
          // M5: release the button lock once this entry reaches a terminal state
          const st = String(od.ordSt || '').toLowerCase()
          const terminal = st === 'complete' || st === 'filled'
            || st.includes('reject') || st.includes('cancel') || st.includes('expir')
          const lockedBy = pendingIds.current[String(od.nOrdNo)]
          if (lockedBy && terminal) {
            releaseKey(lockedBy)
            delete pendingIds.current[String(od.nOrdNo)]
          }
          // stamp THIS update with its own arrival age (browser clock). The old
          // code kept one shared total per nOrdNo, so every row for an order
          // ended up showing the latest (COMPLETE) time — early rows like PUT
          // ORDER REQ RECEIVED looked as slow as the fill. Per-row stamps fix that.
          const hit = clickRef.current[String(od.nOrdNo)]
          const stamped = hit
            ? { ...od, _ageMs: Date.now() - hit.clickMs, _brkMs: hit.brokerMs ?? null }
            : od
          setOrders((o) => [stamped, ...o].slice(0, 100))
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
    }
    connect()
    return () => {
      stopped = true
      if (retry) clearTimeout(retry)
      if (ws) ws.close()
    }
  }, [token])

  /* feed/reconcile freshness (Plan 06 WP2) — compact topbar indicator */
  useEffect(() => {
    if (!token) return
    let live = true
    const load = () => api.get('/health').then(
      (r) => { if (live) setHealth(r.data) },
      () => { if (live) setHealth(null) })
    load()
    const id = setInterval(load, 5000)
    return () => { live = false; clearInterval(id) }
  }, [token])

  /* positions auto-refresh while Kotak session is live */
  useEffect(() => {
    if (!token || !kotakIn) return
    const id = setInterval(() => {
      api.get('/market/positions', auth).then(applyPositions, positionsFailed)
    }, 30000)
    return () => clearInterval(id)
  }, [token, kotakIn])

  /* broker order book auto-refresh (Plan 06 WP6) */
  useEffect(() => {
    if (!token || !kotakIn) return
    let live = true
    const load = async () => {
      if (bookLoadingRef.current) return
      bookLoadingRef.current = true
      try {
        const r = await api.get('/orders/orderbook', auth)
        if (live) setBook(r.data)
      } catch (e) {
        if (!live) return
        const d = e.response?.data
        setBook({
          ok: false, orders: [], count: 0, stale: true,
          error: d?.error || d?.detail || e.message,
          fetched_at_ms: d?.fetched_at_ms ?? null,
        })
      } finally {
        bookLoadingRef.current = false
      }
    }
    load()
    const id = setInterval(load, 5000)
    return () => { live = false; clearInterval(id) }
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
  const logout = () => {
    api.post('/market/unwatch', { session_id: sessionRef.current }, auth).catch(() => {})
    watchSentRef.current = false
    localStorage.removeItem('token'); setToken(''); setKotakIn(false)
  }

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
        api.get('/market/positions', auth).then(
          (r) => { applyPositions(r); return r },
          (e) => { positionsFailed(e); return null }),
        api.get('/auth/kotak/status', auth),
      ])
      setSynthetic(s.data.orders || [])
      if (p === null) { /* banner already set; keep last good rows */ }
      setKotakIn(!!st.data.logged_in)
      // Scrip master drives the symbol->token map behind every order; when it
      // failed to load the server reports trading_enabled=false and we block entry.
      setMasterOk(!!st.data.trading_enabled)
    } catch (e) {
      say('err', `Refresh failed: ${e.response?.data?.detail || e.message}`)
    }
  }

  const refresh = async (justLoggedIn = false) => {
    // A failed scrip-master load disables trading; give Refresh a chance to
    // re-download it before pulling status back in.
    if (kotakIn && !masterOk) {
      try { await api.post('/market/masters/refresh', {}, auth) } catch { /* surfaced via status */ }
    }
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
  // /watch is sent when the selection changes, on tab mount, on websocket
  // reconnect, and as a heartbeat so the backend keeps this session's refs
  // alive (Plan 06 WP9). The backend dedupes identical sets, so a heartbeat does
  // not re-subscribe the watchlist.
  // /batch-quotes hits Kotak REST which rate-limits hard (429 + dropped
  // connections), so REST is throttled: fresh fetch only when a leg lacks any
  // price or the last fetch is >15s old. Live ws ticks are the primary source.
  const sessionRef = useRef(getSessionId())
  const watchSentRef = useRef(false)
  const watchSig = useMemo(() => {
    if (!f.expiry || !legs.length) return ''
    return legs
      .map((l) => `${f.instrument}|${f.expiry}|${l.strike}|${l.otype}`)
      .sort().join(',')
  }, [JSON.stringify(legs), f.instrument, f.expiry]) // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!kotakIn) {
      if (watchSentRef.current) {
        watchSentRef.current = false
        api.post('/market/unwatch', { session_id: sessionRef.current }, auth)
          .catch(() => {})
      }
      return
    }
    const send = () => {
      if (!watchSig) {
        watchSentRef.current = false
        api.post('/market/unwatch', { session_id: sessionRef.current }, auth)
          .catch(() => {})
        return
      }
      const body = {
        session_id: sessionRef.current,
        legs: legs.map((l) => ({
          instrument: f.instrument, expiry: f.expiry,
          strike: String(l.strike), option_type: l.otype,
        })),
      }
      watchSentRef.current = true
      api.post('/market/watch', body, auth).catch(() => {})
    }
    send()
    // Heartbeat refreshes the backend session TTL even when the selection has
    // not changed (and re-registers it after a backend/broker reconnect).
    const id = setInterval(send, 240000)
    const onVisible = () => {
      if (document.visibilityState === 'visible') send()
    }
    const onPageHide = () => {
      if (!watchSentRef.current) return
      watchSentRef.current = false
      const t = localStorage.getItem('token')
      fetch('/api/market/unwatch', {
        method: 'POST',
        keepalive: true,
        headers: {
          'Content-Type': 'application/json',
          ...(t ? { Authorization: `Bearer ${t}` } : {}),
        },
        body: JSON.stringify({ session_id: sessionRef.current }),
      }).catch(() => {})
    }
    const onPageShow = (e) => {
      if (e.persisted) send()
    }
    document.addEventListener('visibilitychange', onVisible)
    window.addEventListener('pagehide', onPageHide)
    window.addEventListener('pageshow', onPageShow)
    return () => {
      clearInterval(id)
      document.removeEventListener('visibilitychange', onVisible)
      window.removeEventListener('pagehide', onPageHide)
      window.removeEventListener('pageshow', onPageShow)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [watchSig, kotakIn, token, feedEpoch])

  const lastBatchRef = useRef(0)
  useEffect(() => {
    if (!kotakIn || !f.expiry || !legs.length) return
    const body = {
      legs: legs.map((l) => ({
        instrument: f.instrument, expiry: f.expiry,
        strike: String(l.strike), option_type: l.otype,
      })),
    }
    const missing = legs.some((l) => {
      const s = symOf(f.instrument, f.expiry, l.strike, l.otype)
      return (ticks[s]?.ltp ?? ticks[s]?.iv ?? px[s]) == null
    })
    if (!missing && Date.now() - lastBatchRef.current < 15000) return
    lastBatchRef.current = Date.now()
    api.post('/market/batch-quotes', body, auth).then((r) => {
      const ltps = r.data.ltps || {}
      setPx((prev) => ({ ...prev, ...ltps }))
      setPxMeta({ stale: false, error: '', at: Date.now() })
      setLtp((v) => {
        const next = { ...v }
        const ceSym = f.ce_strike ? symOf(f.instrument, f.expiry, f.ce_strike, 'CE') : null
        const peSym = f.pe_strike ? symOf(f.instrument, f.expiry, f.pe_strike, 'PE') : null
        if (ceSym && ltps[ceSym] != null) next.ce = ltps[ceSym]
        if (peSym && ltps[peSym] != null) next.pe = ltps[peSym]
        return next
      })
    }).catch((e) => {
      const d = e?.response?.data
      setPxMeta((m) => ({
        ...m, stale: true,
        error: d?.error || d?.detail || e?.message || 'quotes unavailable',
      }))
    })
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
      const d = e?.response?.data
      say('err', `Quote failed (${otype} ${strike}): ${d?.error || d?.detail || e.message}`)
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
  const needTicket = !kotakIn || !masterOk || !f.expiry

  const place = async (otype, side = 'B') => {
    const label = side === 'S' ? 'Sell' : 'Buy'
    const lockName = otype === 'BOTH' ? `BOTH-${side}` : `${otype}-${side}`
    if (placing[lockName]) return
    const tClick = Date.now()
    const off = num(f.offset)
    if (f.entry_mode === 'L-1' && (!Number.isFinite(off) || off < 0)) {
      say('err', 'Set a valid Offset (>= 0) for L-1 entry.'); return
    }
    const idemTag = newTag()
    try {
      if (otype === 'BOTH') {
        if (f.entry_mode === 'L') {
          say('err', 'Straddle does not support L mode — add the legs to the strike list and fire them with their Limit prices.'); return
        }
        const ceSym = symOf(f.instrument, f.expiry, f.ce_strike, 'CE')
        const peSym = symOf(f.instrument, f.expiry, f.pe_strike, 'PE')
        const ce = legLtp(ceSym) ?? ltp.ce ?? await quoteLeg('CE')
        const pe = legLtp(peSym) ?? ltp.pe ?? await quoteLeg('PE')
        if (ce == null || pe == null) { say('err', 'Need both CE and PE quotes first (press ↻).'); return }
        lockKey(lockName) // disable both straddle buttons until terminal/timeout
        const body = { ...f, side, client_order_tag: idemTag }
        const r = await api.post(`/orders/straddle?ce_ltp=${ce}&pe_ltp=${pe}`, body, auth)
        const apiMs = Date.now() - tClick
        const ids = []
        for (const l of r.data.legs || []) { rememberClick(l.entry_order_id, tClick, l.timing, l.symbol); ids.push(l.entry_order_id) }
        notePending(lockName, ids)
        const b0 = r.data.legs?.[0]?.timing?.broker_rtt_ms
        const straddleWarnings = (r.data.legs || []).flatMap((l) => l.warnings || [])
        say('ok', `${label} straddle placed: ${(r.data.legs || []).map((l) => l.symbol).join(' + ')} (api ${fmtLat(apiMs)}${b0 != null ? `, broker ${fmtLat(b0)}` : ''} — fill time lands in the feed below)${warnText(straddleWarnings)}`)
      } else {
        const strike = otype === 'CE' ? f.ce_strike : f.pe_strike
        const sym = symOf(f.instrument, f.expiry, strike, otype)
        let limit = null
        if (f.entry_mode === 'L') {
          limit = parseFloat(otype === 'CE' ? f.ce_limit : f.pe_limit)
          if (!Number.isFinite(limit) || limit <= 0) {
            say('err', `Set a Limit price for ${sym} — entry L needs one (type it next to the quote).`); return
          }
        }
        const live = legLtp(sym)
        const price = live ?? (f.entry_mode === 'L' ? limit : await quoteLeg(otype))
        if (price == null) { say('err', `Need a ${otype} quote first (press ↻).`); return }
        lockKey(lockName)
        const body = {
          ...f, option_type: otype, strike: String(strike), entry_offset: 0,
          offset: off, limit_price: limit, side, client_order_tag: idemTag,
        }
        const r = await api.post(`/orders/single?ref_ltp=${price}`, body, auth)
        const apiMs = Date.now() - tClick
        rememberClick(r.data.entry_order_id, tClick, r.data.timing, r.data.symbol)
        notePending(lockName, [r.data.entry_order_id])
        const b = r.data.timing?.broker_rtt_ms
        say('ok', `${label} ${r.data.symbol} @ ${r.data.entry} → TGT ${r.data.target} / SL ${r.data.stop} (api ${fmtLat(apiMs)}${b != null ? `, broker ${fmtLat(b)}` : ''} — fill time lands in the feed below)${warnText(r.data.warnings)}`)
      }
      refreshData() // keep dropdowns + watchlist intact
    } catch (e) {
      say('err', `Order failed: ${e.response?.data?.detail || e.message}`)
      // Leave the lock armed: the request may have reached the broker before the
      // response failed. The terminal feed event or the 30s timeout releases it.
    }
  }

  const placeRow = async (leg, side = 'B') => {
    const key = `${leg.otype}-${leg.strike}-${side}`
    setBusyKey(key)
    const tClick = Date.now()
    const mode = leg.entry_mode || 'MKT'
    const off = num(leg.offset)
    if (mode === 'L-1' && (!Number.isFinite(off) || off < 0)) {
      say('err', `Set a valid Offset (>= 0) for ${leg.otype} ${leg.strike} (L-1 needs one).`); setBusyKey(''); return
    }
    let limit = null
    if (mode === 'L') {
      limit = parseFloat(leg.limit_price)
      if (!Number.isFinite(limit) || limit <= 0) {
        say('err', `Set a Limit price for ${leg.otype} ${leg.strike} — entry L needs one (type it in the Limit column).`); setBusyKey(''); return
      }
    }
    try {
      const sym = symOf(f.instrument, f.expiry, leg.strike, leg.otype)
      const live = legLtp(sym)
      const price = live ?? (mode === 'L' ? limit : await quoteFor(leg.otype, leg.strike))
      if (price == null) { say('err', `Need a ${leg.otype} ${leg.strike} quote first.`); return }
      const body = {
        ...f, entry_mode: mode, target_points: leg.target_points,
        stop_points: leg.stop_points, offset: off, limit_price: limit,
        option_type: leg.otype, strike: String(leg.strike),
        entry_offset: 0, side, client_order_tag: newTag(),
      }
      const r = await api.post(`/orders/single?ref_ltp=${price}`, body, auth)
      const apiMs = Date.now() - tClick
      rememberClick(r.data.entry_order_id, tClick, r.data.timing, r.data.symbol)
      const b = r.data.timing?.broker_rtt_ms
      say('ok', `${side === 'S' ? 'Sell' : 'Buy'} ${r.data.symbol} @ ${r.data.entry} → TGT ${r.data.target} / SL ${r.data.stop} (api ${fmtLat(apiMs)}${b != null ? `, broker ${fmtLat(b)}` : ''})${warnText(r.data.warnings)}`)
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
    // Entry/Tgt/Stop/Offset inherit the globals at add time, then live per row.
    const leg = {
      key, otype, strike: String(strike),
      entry_mode: f.entry_mode, target_points: f.target_points,
      stop_points: f.stop_points, offset: f.offset,
      limit_price: otype === 'CE' ? f.ce_limit : f.pe_limit,
    }
    setLegs((v) => (v.some((l) => l.key === key) ? v : [...v, leg]))
  }

  const updateLeg = (key, patch) =>
    setLegs((v) => v.map((l) => (l.key === key ? { ...l, ...patch } : l)))

  const exitPosition = async (symbol) => {
    if (!window.confirm(`Flatten ${symbol} at market and cancel its SL/target?`)) return
    setExiting(symbol)
    const tClick = Date.now()
    const idemTag = newTag()
    try {
      const r = await api.post('/orders/exit-position',
        { symbol, client_order_tag: idemTag }, auth)
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

  // Operator escape hatch for EXIT_BLOCKED_SL_STILL_WORKING / naked legs.
  // The server still cancels + confirms the SL before firing one market exit,
  // so this cannot double-exit; it supplies the explicit human confirmation.
  const forceExit = async (entryId, symbol, blocked) => {
    if (!window.confirm(
      `FORCE EXIT ${symbol}?\n\n${blocked || 'Leg is ERRORED/blocked.'}\n` +
      'The server will cancel the SL, confirm it is terminal, then place ONE market exit. ' +
      'This is audited (user/time/order/reason).')) return
    setForcing(entryId)
    try {
      const r = await api.post('/orders/force-exit', {
        entry_order_id: entryId,
        reason: blocked || 'operator force exit',
      }, auth)
      say('ok', `Force exit ${symbol}: ${r.data.result}` +
        `${r.data.leg?.exit_order_id ? ` (exit ${r.data.leg.exit_order_id})` : ''}`)
      refreshData()
    } catch (e) {
      const d = e.response?.data
      say('err', `Force exit blocked: ${d?.result || d?.detail || e.message}`)
      refreshData()
    } finally {
      setForcing('')
    }
  }

  /* ---- order management (Plan 06 WP6) ---- */
  const withBookBusy = async (key, fn) => {
    setBookBusy((p) => ({ ...p, [key]: true }))
    try { await fn() } finally {
      setBookBusy((p) => {
        const n = { ...p }
        delete n[key]
        return n
      })
    }
  }

  const cancelOrder = (o) => withBookBusy(`cancel:${o.nOrdNo}`, async () => {
    if (!window.confirm(`Cancel order ${o.nOrdNo} (${o.trdSym})?`)) return
    try {
      const r = await api.post(`/orders/cancel/${encodeURIComponent(o.nOrdNo)}`, {}, auth)
      say('ok', r.data.already
        ? `Cancel ${o.nOrdNo}: already requested`
        : `Cancel ${o.nOrdNo}: ${r.data.result?.stCode === 200 ? 'accepted' : 'sent'}`)
      await loadOrderbook()
      refreshData()
    } catch (e) {
      const d = e.response?.data
      say('err', `Cancel ${o.nOrdNo} failed: ${d?.error || d?.detail || e.message}`)
      loadOrderbook()
    }
  })

  const modifySl = (o) => withBookBusy(`modify:${o.nOrdNo}`, async () => {
    const leg = synthetic.find((s) =>
      String(s.sl_order_id) === String(o.nOrdNo)
      || String(s.entry_order_id) === String(o.nOrdNo))
    const cur = leg?.stop_loss_price || o.trigger_price || ''
    const raw = window.prompt(
      `New SL trigger for ${o.trdSym} (order ${o.nOrdNo}).\nQuantity comes from the tracker.`, cur)
    if (raw == null) return
    const trigger = Number(raw)
    if (!Number.isFinite(trigger) || trigger <= 0) {
      say('err', 'SL trigger must be a positive number.')
      return
    }
    if (!window.confirm(`Modify SL ${o.nOrdNo} trigger to ${trigger}?`)) return
    try {
      const r = await api.post(
        `/orders/modify-sl/${encodeURIComponent(o.nOrdNo)}?trigger=${encodeURIComponent(trigger)}`,
        {}, auth)
      say('ok', `SL ${o.nOrdNo} modify accepted at ${r.data.trigger} — waiting for broker/feed sync.`)
      await loadOrderbook()
      refreshData()
    } catch (e) {
      const d = e.response?.data
      say('err', `SL modify failed: ${d?.error || d?.detail || e.message}`)
      loadOrderbook()
    }
  })

  const cancelSymbol = (symbol) => withBookBusy(`cancelall:${symbol}`, async () => {
    if (!window.confirm(`Cancel every tracked entry/SL for ${symbol}?`)) return
    try {
      const r = await api.post(`/orders/cancel-all/${encodeURIComponent(symbol)}`, {}, auth)
      say('ok', `Cancel-all ${symbol}: ${r.data.cancelled_count}/${r.data.requested_count} sent` +
        `${r.data.already_count ? `, ${r.data.already_count} already requested` : ''}.`)
      await loadOrderbook()
      refreshData()
    } catch (e) {
      const d = e.response?.data
      const failed = (d?.results || []).filter((r) => !(r.ok || r.already))
      const perId = failed.map((r) =>
        `${r.order_id}: ${r.error || 'failed'}`).join('; ')
      say('err', `Cancel-all ${symbol} failed: ${d?.error || d?.detail || e.message}` +
        (perId ? ` [${perId}]` : ''))
      loadOrderbook()
    }
  })

  /* ---- render ---- */
  const healthAlerts = []
  if (health && kotakIn) {
    if (health.feed?.stale || !health.feed?.order_connected) {
      healthAlerts.push(`Order feed down${health.feed?.feed_stale_age_ms ? ` for ${fmtLat(health.feed.feed_stale_age_ms)}` : ''}`)
    } else if (health.feed?.quiet) {
      healthAlerts.push(`Order feed quiet ${fmtLat(health.feed_order_age_ms) ?? ''}`)
    }
    if (health.naked_legs > 0) healthAlerts.push(`${health.naked_legs} NAKED leg(s) — no protective SL`)
    if (health.errored_legs > 0) healthAlerts.push(`${health.errored_legs} ERRORED leg(s) — manual action needed`)
  }
  if (posMeta.stale) healthAlerts.push('Positions unavailable — last good snapshot shown')

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
        {health?.feed && (
          <span className={`pill ${health.feed.stale || !health.feed.order_connected ? 'off' : 'on'}`}
            title={`order feed age ${fmtLat(health.order_age_ms) ?? '—'}, ` +
              `last reconcile ${fmtLat(health.feed.last_reconcile_age_ms) ?? '—'}, ` +
              `worker queue ${health.worker?.queue_depth ?? 0}`}>
            <span className="dot" />
            {health.feed.stale ? 'Feed down'
              : health.feed.quiet ? `Feed quiet ${fmtLat(health.order_age_ms) ?? '—'}`
                : health.feed.order_connected ? `Feed ${fmtLat(health.order_age_ms) ?? '—'}` : 'Feed down'}
          </span>
        )}
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
      {healthAlerts.length > 0 && (
        <p className="msg err">
          {healthAlerts.join(' · ')}
        </p>
      )}

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

      {kotakIn && !masterOk && (
        <div className="card" style={{ borderColor: '#c0392b' }}>
          <h2>⚠ Scrip master unavailable — trading disabled</h2>
          <p className="sub">
            The symbol→token map failed to load (TLS / network / parse error), so orders
            are blocked to avoid submitting contracts that resolve to no token.
            Quote, positions and order feed still work. Press <b>Refresh</b> to retry the
            download (if it keeps failing, check the server log for the exact TLS error
            and optionally set <code className="inline">SCRIP_MASTER_CA_BUNDLE</code>).
          </p>
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
          {!kotakIn
            ? 'Connect the Kotak session above to enable trading.'
            : !masterOk
              ? 'Trading disabled — scrip master unavailable (see banner above). Press Refresh to retry.'
              : 'Entry fills → SL auto-placed from fill price. Target hit → market exit, or trailing SL if selected. Selection survives orders/refresh.'}
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
          <div className="field"><label>Offset (L-1)</label>
            <input type="number" min="0" step="0.5" value={f.offset} disabled={!kotakIn}
              title="Price improvement for L-1: BUY fills at LTP−offset, SELL at LTP+offset"
              onChange={(e) => setF({ ...f, offset: e.target.value })} /></div>
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
          <div className="field" style={{ minWidth: 96 }}><label>CE limit ₹</label>
            <input type="number" min="0" step="0.05" placeholder="L mode" value={f.ce_limit} disabled={!kotakIn}
              onChange={(e) => setF({ ...f, ce_limit: e.target.value })} /></div>
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
          <div className="field" style={{ minWidth: 96 }}><label>PE limit ₹</label>
            <input type="number" min="0" step="0.05" placeholder="L mode" value={f.pe_limit} disabled={!kotakIn}
              onChange={(e) => setF({ ...f, pe_limit: e.target.value })} /></div>
          <button className="btn ghost sm" disabled={!kotakIn || !f.pe_strike} onClick={() => addLeg('PE')}>+ list</button>
        </div>

        <div className="row" style={{ marginTop: 14 }}>
          <button className="btn buy" disabled={needTicket || !f.ce_strike || placing['CE-B']}
            onClick={() => place('CE', 'B')}>{placing['CE-B'] ? '…' : 'Buy CE'}</button>
          <button className="btn sell" disabled={needTicket || !f.ce_strike || placing['CE-S']}
            onClick={() => place('CE', 'S')}>{placing['CE-S'] ? '…' : 'Sell CE'}</button>
          <button className="btn buy" disabled={needTicket || !f.pe_strike || placing['PE-B']}
            onClick={() => place('PE', 'B')}>{placing['PE-B'] ? '…' : 'Buy PE'}</button>
          <button className="btn sell" disabled={needTicket || !f.pe_strike || placing['PE-S']}
            onClick={() => place('PE', 'S')}>{placing['PE-S'] ? '…' : 'Sell PE'}</button>
          <button className="btn" disabled={needTicket || !f.ce_strike || !f.pe_strike || placing['BOTH-B']}
            onClick={() => place('BOTH', 'B')}>{placing['BOTH-B'] ? '…' : 'Buy both (straddle)'}</button>
          <button className="btn ghost" disabled={needTicket || !f.ce_strike || !f.pe_strike || placing['BOTH-S']}
            onClick={() => place('BOTH', 'S')}>{placing['BOTH-S'] ? '…' : 'Sell both'}</button>
        </div>
      </div>

      <div className="card">
        <h2>Strike list — instant execution ({legs.length})</h2>
        <p className="sub">Entry/Tgt/Stop/Offset inherit the ticket at add time, then edit per row. L needs its Limit ₹ cell filled; L-1 uses the Offset cell (BUY = LTP−off, SELL = LTP+off). List survives orders and refreshes, clears only on instrument/expiry change.</p>
        {pxMeta.stale && (
          <p className="msg err">
            Watchlist quotes unavailable{pxMeta.error ? `: ${pxMeta.error}` : ''}
            {pxMeta.at ? ` (last updated ${new Date(pxMeta.at).toLocaleTimeString()})` : ''}
          </p>
        )}
        {legs.length === 0 ? <div className="empty">Empty — pick a CE/PE strike above and press “+ list”.</div> : (
          <table className="tbl"><thead><tr>
            <th>Contract</th><th>Live LTP</th><th>Entry</th><th>Tgt</th><th>Stop</th><th>Off</th><th>Limit ₹</th><th>Buy</th><th>Sell</th><th></th>
          </tr></thead><tbody>
            {legs.map((l) => {
              const sym = symOf(f.instrument, f.expiry, l.strike, l.otype)
              const live = legLtp(sym)
              return (
                <tr key={l.key}>
                  <td>{sym}</td>
                  <td className="ltp" title={pxMeta.stale ? pxMeta.error || 'quotes unavailable' : ''}>
                    {live ?? (pxMeta.stale ? 'unavailable' : '…')}
                  </td>
                  <td><select className="mini" value={l.entry_mode || 'MKT'}
                    onChange={(e) => updateLeg(l.key, { entry_mode: e.target.value })}>
                    <option>MKT</option><option>ML</option><option>L-1</option><option>L</option>
                  </select></td>
                  <td><input className="mini" type="number" min="0" value={l.target_points}
                    onChange={(e) => updateLeg(l.key, { target_points: e.target.value })} /></td>
                  <td><input className="mini" type="number" min="0" value={l.stop_points}
                    onChange={(e) => updateLeg(l.key, { stop_points: e.target.value })} /></td>
                  <td><input className="mini" type="number" min="0" step="0.5" value={l.offset}
                    title="L-1 offset" onChange={(e) => updateLeg(l.key, { offset: e.target.value })} /></td>
                  <td><input className="mini wide" type="number" min="0" step="0.05" placeholder="L req."
                    value={l.limit_price || ''} onChange={(e) => updateLeg(l.key, { limit_price: e.target.value })} /></td>
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
            <th>Symbol</th><th>Side</th><th>Qty</th><th>Target</th><th>Stop</th><th>Strategy</th><th>Status</th><th>Safety</th><th>Sync</th><th>Fill lat</th>
          </tr></thead><tbody>
            {synthetic.map((o) => {
              const canForce = o.naked_position || o.exit_ambiguous || [
                'EXIT_BLOCKED_SL_STILL_WORKING', 'EXIT_REJECTED', 'EXIT_CANCELLED',
                'EXIT_UNKNOWN', 'EXIT_AMBIGUOUS', 'POSITION_MISMATCH', 'SL_MISSING',
                'SL_TERMINAL', 'SL_PLACEMENT_REJECTED', 'SL_CANCELLED_MANUAL',
              ].includes(o.blocked_reason)
              return (
                <tr key={o.entry_order_id}>
                  <td>{o.trading_symbol}</td>
                  <td>{o.side === 'S' ? 'SELL' : 'BUY'}</td>
                  <td>{o.quantity}</td>
                  <td>{o.target_price}</td><td>{o.stop_loss_price}</td>
                  <td>{o.strategy}</td>
                  <td><span className={`st ${o.status}`} title={o.error || ''}>{o.status}</span></td>
                  <td>
                    {o.naked_position && !o.exit_pending && <span className="st ERRORED" title={o.error || 'naked position'}>NAKED</span>}
                    {o.exit_pending && <span className="st ACTIVE" title={o.exit_order_id ? `exit ${o.exit_order_id} working` : 'exit pending'}>EXIT PENDING</span>}
                    {o.blocked_reason && o.blocked_reason !== 'EXIT_BLOCKED_SL_STILL_WORKING' &&
                      <span className="st PENDING_ENTRY" title={o.error || o.blocked_reason}>{o.blocked_reason}</span>}
                    {o.blocked_reason === 'EXIT_BLOCKED_SL_STILL_WORKING' &&
                      <span className="st ERRORED" title={o.error || ''}>EXIT BLOCKED</span>}
                    {o.sl_modify_pending && (
                      <span className="st PENDING_ENTRY" title={`manual SL modify to ${o.sl_modify_target} in flight`}>
                        SL MODIFY PENDING
                      </span>
                    )}
                    {canForce && (
                      <button className="btn sell sm" style={{ marginLeft: 6 }}
                        disabled={forcing === o.entry_order_id}
                        onClick={() => forceExit(o.entry_order_id, o.trading_symbol, o.error || o.blocked_reason)}>
                        {forcing === o.entry_order_id ? '…' : 'Force exit'}
                      </button>
                    )}
                    <span className="ltp" style={{ marginLeft: 6 }}
                      title={`SL guarantee due ${o.sl_guarantee_due_ms ? new Date(o.sl_guarantee_due_ms).toLocaleTimeString() : '-'}; last successful per-leg reconcile`}>
                      {o.last_reconcile_ms ? new Date(o.last_reconcile_ms).toLocaleTimeString() : '-'}
                    </span>
                    {o.sl_guarantee_due_ms && (
                      <span className="ltp" style={{ marginLeft: 6 }}
                        title="SL guarantee deadline (accepted entry must be protected)">
                        SL due {new Date(o.sl_guarantee_due_ms).toLocaleTimeString()}
                      </span>
                    )}
                  </td>
                  <td className="ltp">
                    <span title={`last sync source: ${o.source || 'unknown'}`}>
                      {o.source || '—'}
                    </span>
                    <span className="ltp" style={{ marginLeft: 6 }}
                      title="last synced from feed/book/API">
                      {o.last_synced_ms ? new Date(o.last_synced_ms).toLocaleTimeString() : '—'}
                    </span>
                  </td>
                  <td className="ltp">{fmtLat(o.fill_latency_ms) ?? '—'}</td>
                </tr>
              )
            })}
          </tbody></table>
        )}
      </div>

      <div className="card">
        <h2>Positions</h2>
        <p className="sub">Net broker positions (auto-refreshes every 30s). Exit flattens at market and cancels that symbol's entry/SL + synthetic target.</p>
        {!posMeta.ok && (
          <p className="msg err">
            Positions unavailable{posMeta.error ? `: ${posMeta.error}` : ''}
            {posMeta.at ? ` (last updated ${new Date(posMeta.at).toLocaleTimeString()})` : ' (no successful snapshot yet)'}
          </p>
        )}
        {Object.keys(positions).length === 0
          ? (posMeta.ok ? <div className="empty">Flat — no open positions.</div> : null)
          : (
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
        <h2>Broker order book ({book.count || 0})</h2>
        <p className="sub">
          Live broker book (auto-refresh 5s). Actions run through the single-writer worker;
          the tracker is updated and a reconcile is scheduled. A failed/stale read is shown
          as an error, never as an empty book.
        </p>
        {!book.ok && (
          <p className="msg err">
            Order book unavailable{book.error ? `: ${book.error}` : ''}
            {book.fetched_at_ms ? ` (last attempt ${new Date(book.fetched_at_ms).toLocaleTimeString()})` : ''}
          </p>
        )}
        {book.ok && (book.orders || []).length === 0 && (
          <div className="empty">No working or recent orders in the broker book.</div>
        )}
        {book.ok && (book.orders || []).length > 0 && (
          <table className="tbl"><thead><tr>
            <th>Order</th><th>Symbol</th><th>Side</th><th>Qty / Filled</th><th>Price</th>
            <th>Trigger</th><th>Status</th><th>Age</th><th>Actions</th>
          </tr></thead><tbody>
            {(book.orders || []).map((o) => {
              const leg = synthetic.find((s) =>
                String(s.entry_order_id) === String(o.nOrdNo)
                || String(s.sl_order_id) === String(o.nOrdNo))
              const st = String(o.ordSt || '').toLowerCase()
              const terminal = st.includes('complete') || st.includes('filled')
                || st.includes('reject') || st.includes('cancel') || st.includes('expir')
              const busy = bookBusy[`cancel:${o.nOrdNo}`] || bookBusy[`modify:${o.nOrdNo}`]
              const symBusy = bookBusy[`cancelall:${o.trdSym}`]
              return (
                <tr key={o.nOrdNo}>
                  <td title={o.nOrdNo}>{o.nOrdNo}{leg ? ` (${o.nOrdNo === String(leg.entry_order_id) ? 'entry' : 'SL'})` : ''}</td>
                  <td>{o.trdSym}</td>
                  <td className={`side-${o.trnsTp}`}>{o.trnsTp === 'B' ? 'BUY' : o.trnsTp === 'S' ? 'SELL' : o.trnsTp}</td>
                  <td>{o.qty} / {o.fldQty}</td>
                  <td>{o.prc}</td>
                  <td>{o.trigger_price || '—'}</td>
                  <td><span className={`st ${feedClass(o.ordSt)}`}>{(o.ordSt || '?').toUpperCase()}</span></td>
                  <td className="ltp" title={o.ord_time ? `broker time ${o.ord_time}` : ''}>
                    {fmtLat(o.age_ms) ?? '—'}
                  </td>
                  <td>
                    <button className="btn ghost sm" disabled={!o.trdSym || symBusy}
                      title="Cancel every tracked entry/SL for this symbol"
                      onClick={() => cancelSymbol(o.trdSym)}>
                      {symBusy ? '…' : 'Cancel all'}
                    </button>
                    {!terminal && (
                      <button className="btn sell sm" style={{ marginLeft: 6 }}
                        disabled={busy}
                        onClick={() => cancelOrder(o)}>
                        {bookBusy[`cancel:${o.nOrdNo}`] ? '…' : 'Cancel'}
                      </button>
                    )}
                    {!terminal && leg && String(leg.sl_order_id) === String(o.nOrdNo) && (
                      <button className="btn buy sm" style={{ marginLeft: 6 }}
                        disabled={busy}
                        onClick={() => modifySl(o)}>
                        {bookBusy[`modify:${o.nOrdNo}`] ? '…' : 'Modify SL'}
                      </button>
                    )}
                    {o.trdSym && (
                      <button className="btn sell sm" style={{ marginLeft: 6 }}
                        disabled={exiting === o.trdSym}
                        onClick={() => exitPosition(o.trdSym)}>
                        {exiting === o.trdSym ? '…' : 'Exit'}
                      </button>
                    )}
                  </td>
                </tr>
              )
            })}
          </tbody></table>
        )}
      </div>

      <div className="card">
        <h2>Live order feed ({orders.length})</h2>
        <p className="sub">Pushed over the exchange websocket via the server. ⚡ = your click → this update (browser clock); broker = server-internal place_order RTT.</p>
        <div className="feed">
          {orders.length === 0 && <div className="empty">No order updates yet this session.</div>}
          {orders.slice(0, 30).map((o, i) => {
            const l = lat[String(o.nOrdNo)]
            // prefer this row's own arrival stamp; fall back to the shared map
            const total = fmtLat(o._ageMs ?? l?.totalMs)
            const brk = o._brkMs != null ? fmtLat(o._brkMs) : (l?.brokerMs != null ? fmtLat(l.brokerMs) : null)
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
