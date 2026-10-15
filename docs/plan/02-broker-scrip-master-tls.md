# Plan 02 — Broker Scrip-Master TLS Verification (C5)

Status: **IMPLEMENTED** (verify=True by default with optional
`SCRIP_MASTER_CA_BUNDLE`; failures recorded and surfaced as a
trading-disabled state that blocks order entry) · Priority: P1 · Scope:
`backend/app/kotak_client.py`

## Problem
`kotak_client.py:137`:
```python
resp = requests.get(url, timeout=30, verify=False)  # noqa: S501 (broker file, cert quirk)
```
TLS verification is disabled for the scrip-master CSV download. The downloaded
`pTrdSymbol -> pSymbol` map is what every order's `instrument_token` is derived
from (`token_for`, `kotak_client.py:150-152`; used at `routes/orders.py:105` and
for websocket subscriptions). A man-in-the-middle can inject a forged mapping
so orders/streams resolve to attacker-chosen instruments **while carrying the
real trading session**.

## Is there a reason it must be disabled?
No confirmed one. The comment says "cert quirk" but gives no evidence. The
broker CSV host is a normal public host; verification should work. Enable it and
diagnose any failure rather than disabling globally.

## Design
1. Flip to `verify=True` (default) first — simplest possible change:
   ```python
   resp = requests.get(url, timeout=30)   # verify=True by default
   ```
2. If it fails, capture the exact error and fix the root cause instead of
   blanket-disabling:
   - `SSLCertVerificationError: unable to get local issuer certificate` →
     supply the correct CA chain via `certifi` or a mounted bundle
     (`REQUESTS_CA_BUNDLE` / `verify=<path>`), never `verify=False`.
   - Hostname mismatch → investigate the actual URL returned by
     `client.scrip_master(...)`.
3. On failure, **do not silently return an empty map** (current behavior at
   `:146-148`). Log at error and surface a clear "scrip master unavailable —
   trading disabled" state so the UI can block order entry rather than submit
   symbols that resolve to no token.

## Files / touch points
- `backend/app/kotak_client.py` — `_scrip_master` (verification + failure
  surfacing); optionally allow an override CA path via settings.
- `backend/app/config.py` — optional `scrip_master_ca_bundle: str = ""`.
- `backend/app/routes/market.py` / UI — reflect an unavailable master.
- `.env.example` — document the optional CA-bundle setting.

## Verification
- `python -c` snippet hitting the real scrip-master URL with `verify=True`
  succeeds on a normal network.
- Simulated cert failure (bad CA) → error logged, empty map **not** returned
  silently, orders blocked.
- Existing SDK-compat tests still pass.

## Risks / rollback
- If the broker host genuinely presents a broken chain, we may need a pinned CA
  bundle — still strictly better than `verify=False`. Rollback = restore the old
  line (discouraged).
