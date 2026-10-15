# Remediation Plan Index — KotakAPI Trader Security/Architecture Audit

Status: **DRAFT — not implemented.** These are planning documents only. Do not
start work without picking a group and confirming scope.

Source: security audit + adversarial review of the backend (`backend/app/*`),
frontend (`frontend/src/*`), deploy configs, and installed `kotakneoapi` 3.0.7.

## Grouped change sets

| # | Plan | Covers | Priority |
|---|------|--------|----------|
| 01 | [Network & transport security](01-network-and-transport-security.md) | C1, C2, C3, C4 | P0 |
| 02 | [Broker scrip-master TLS verification](02-broker-scrip-master-tls.md) | C5 | P1 |
| 03 | [Decouple order feed from market feed](03-decouple-order-feed.md) | H1 | P0 (trading safety) |
| 04 | [Order-execution integrity](04-order-execution-integrity.md) | H3, M5, M6 | P0 (trading safety) |
| 05 | [Backlog — remaining findings](05-backlog-findings.md) | H2, M1–M4, Low, Perf | P2/P3 |

Deferred by owner: **C6** (JWT hygiene — accepted for now), **H2** (state
persistence — future separate storage session).

## Decisions captured from review

- **C2 — TLS without owning a domain?** Yes. Options, best → acceptable:
  1. **Tailscale (recommended here).** `tailscale serve` (or bind Caddy to the
     tailnet IP) terminates TLS with a **trusted** cert on your `*.ts.net` name;
     no public 80/443 at all. Already documented in `deploy/tailscale.md`.
     Best fit for a single-user private desk behind a security group.
  2. **`sslip.io` / `nip.io` + Let's Encrypt.** `<ip>.sslip.io` resolves to
     `<ip>` via public DNS (no domain ownership, no registration). Caddy gets a
     real, browser-trusted cert with the HTTP-01 challenge, so ports 80 and 443
     must be publicly reachable (security-group scoped). Caveat: public DNS
     dependency and the hostname appears in Certificate Transparency logs.
  3. **Caddy `tls internal`.** Self-signed cert auto-trusted only locally →
     browser warnings on remote clients. Fallback only.
  4. Emerging: Let's Encrypt short-lived **IP-address certificates** (no
     hostname at all). Verify current Caddy/ACME client support before relying
     on it.
- **C3 —** A security group already exists on the Linux box (primary network
  control). We still fix the application-layer allowlist defects (spoofable
  `X-Forwarded-For`, `/ws` uncovered, 500-instead-of-403) as defense in depth.
- **C5 —** No confirmed reason verification must stay disabled; enable it and
  diagnose any failure (likely a chain/CA quirk on the broker CSV host) rather
  than disabling globally.
- **C6 —** Accepted for now (non-default secret in `.env`, short-ish TTL).

## Ground rules for implementers

- One group per branch/PR; keep the change reviewable.
- No secret values in code, logs, commits, or these docs.
- Run the existing suite before/after: `cd backend; uv run pytest tests/ -q`
  (currently 25 passing). Add tests for each behavioral change.
- Any change touching order placement (`orders.py`, `synthetic_engine.py`,
  `kotak_client.py`) must preserve the synthetic-bracket lifecycle invariants
  (see `04-order-execution-integrity.md`).
