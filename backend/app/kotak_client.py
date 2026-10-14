"""Thin wrapper over Kotak Neo SDK v3 (PyPI: kotakneoapi, module: neo_api_client).

Port of TransTrader/broker.py concepts to a headless service:
  - totp two-step login (initiate -> complete with TOTP)
  - scrip-master cache for nse_fo / bse_fo (trading_symbol -> token)
  - place / modify / cancel, positions aggregation

v3 notes (verified against installed kotakneoapi 3.0.7):
  - ctor: NeoAPI(consumer_key=..., environment=...)
  - scrip_master(exchange_segment) -> CSV URL str (or {"Error": ...} dict on failure)
  - place_order() has NO market_protection/pf/BO kwargs -> bracket behaviour comes
    solely from SyntheticEngine (synthetic_engine.py), not native BO product.
  - modify_order() REQUIRES quantity -> callers must pass it (tracked per leg).
  - full day order book = order_report(), NOT order_history() (needs an order_id).
"""

import io
import logging
import re
from datetime import date

import pandas as pd
import requests

log = logging.getLogger("kotak")

try:
    from neo_api_client import NeoAPI
except ImportError:  # dev machine without SDK installed
    NeoAPI = None  # type: ignore


class KotakService:
    def __init__(self, consumer_key: str):
        self.consumer_key = consumer_key
        self.client = None
        self.pending_client = None
        self.login_date: date | None = None
        self.nse_fo_tokens: dict[str, str] = {}
        self.bse_fo_tokens: dict[str, str] = {}
        self.nse_fo_df = None
        self.bse_fo_df = None

    # ---------- session ----------
    @property
    def logged_in(self) -> bool:
        return self.client is not None and self.login_date == date.today()

    def initiate_login(self):
        if NeoAPI is None:
            raise RuntimeError("kotakneoapi not installed (cd backend && uv sync)")
        # Same style as SwingTrader KotakProvider: consumer_key + environment only.
        self.pending_client = NeoAPI(consumer_key=self.consumer_key, environment="prod")
        return True

    @staticmethod
    def _assert_session_ok(resp, step: str):
        """v3 totp calls return error dicts instead of raising — a bad/stale TOTP
        would otherwise look like a live session until the first order 401s."""
        if isinstance(resp, dict) and isinstance(resp.get("data"), dict):
            return resp["data"]
        detail = ""
        if isinstance(resp, dict):
            detail = (resp.get("errMsg") or resp.get("message")
                      or resp.get("Error") or resp.get("stat") or str(resp)[:200])
        raise RuntimeError(f"{step} failed: {detail or 'no session data returned'}")

    def complete_login(self, mobile: str, ucc: str, totp: str, mpin: str):
        if not self.pending_client:
            raise ValueError("Call initiate_login first")
        mobile, ucc, totp, mpin = (s.strip() for s in (mobile, ucc, totp, mpin))
        self._assert_session_ok(
            self.pending_client.totp_login(mobile_number=mobile, ucc=ucc, totp=totp),
            "TOTP login (check the 6-digit code is current)")
        self._assert_session_ok(
            self.pending_client.totp_validate(mpin=mpin),
            "MPIN validation (check KOTAK_MPIN in .env)")
        cfg = getattr(getattr(self.pending_client, "api_client", None), "configuration", None)
        if not getattr(cfg, "edit_token", None):
            raise RuntimeError("MPIN validation returned no trading token — login incomplete")
        self.client = self.pending_client
        self.pending_client = None
        self.login_date = date.today()
        self.nse_fo_tokens = self._scrip_master("nse_fo")
        self.bse_fo_tokens = self._scrip_master("bse_fo")
        log.info("Kotak login OK; scrip masters cached: NFO=%d BFO=%d",
                 len(self.nse_fo_tokens), len(self.bse_fo_tokens))
        return True

    def _require(self):
        if not self.logged_in or self.client is None:
            raise RuntimeError("Kotak session expired — login again (TOTP)")
        return self.client

    # ---------- masters ----------
    # Trading symbols look like NIFTY25N1825450CE = <UNDERLYING><EXPIRY><STRIKE><CE|PE>.
    # Expiry segment (e.g. 25N18) is extracted via regex so the UI can offer a
    # dropdown instead of a hardcoded value.
    _SYM_RE = re.compile(r"^(NIFTY|SENSEX|BANKNIFTY|FINNIFTY)(\d{2}[A-Z]\d{2})(\d+)(CE|PE|XX|CF|PF)$")

    def _scrip_master(self, segment: str) -> dict[str, str]:
        client = self._require()
        try:
            url = client.scrip_master(exchange_segment=segment)
            if not isinstance(url, str):  # v3 returns {"Error": ...} dict on failure
                log.error("scrip master %s error: %s", segment, url)
                return {}
            resp = requests.get(url, timeout=30, verify=False)  # noqa: S501 (broker file, cert quirk)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text), dtype=str).fillna("")
            out = dict(zip(df["pTrdSymbol"], df["pSymbol"]))
            if segment == "nse_fo":
                self.nse_fo_df = df
            else:
                self.bse_fo_df = df
            return out
        except Exception as e:
            log.error("scrip master %s failed: %s", segment, e)
            return {}

    def token_for(self, trading_symbol: str, exchange: str) -> str | None:
        m = self.bse_fo_tokens if exchange == "bse_fo" else self.nse_fo_tokens
        return m.get(trading_symbol)

    def expiries(self, instrument: str) -> list[str]:
        """Sorted unique expiry codes (e.g. ['25N18', '25D24']) for an underlying."""
        codes: set[str] = set()
        for sym in list(self.nse_fo_tokens) + list(self.bse_fo_tokens):
            m = self._SYM_RE.match(sym)
            if m and m.group(1) == instrument:
                codes.add(m.group(2))
        return sorted(codes)

    def master_strikes(self, instrument: str, expiry: str, option_type: str) -> list[str]:
        """Actual strikes listed in the scrip master for instrument+expiry, numeric sort."""
        out: set[str] = set()
        for sym in list(self.nse_fo_tokens) + list(self.bse_fo_tokens):
            m = self._SYM_RE.match(sym)
            if m and m.group(1) == instrument and m.group(2) == expiry and m.group(4) == option_type:
                out.add(m.group(3))
        return sorted(out, key=lambda s: int(s))

    # ---------- orders (kwargs limited to v3 place_order signature) ----------
    def place_order(self, **kw):
        client = self._require()
        log.info("place_order %s", kw)
        return client.place_order(
            exchange_segment=kw["exchange_segment"],
            product=kw.get("product", "MIS"),
            price=str(kw.get("price", "0")),
            order_type=kw["order_type"],
            quantity=str(kw["quantity"]),
            validity=kw.get("validity", "DAY"),
            trading_symbol=kw["trading_symbol"],
            transaction_type=kw.get("transaction_type", "B"),
            amo=kw.get("amo", "NO"),
            trigger_price=str(kw.get("trigger_price", "0")),
            disclosed_quantity=str(kw.get("disclosed_quantity", "0")),
            tag=kw.get("tag"),
        )

    def place_stop_loss(self, exchange_segment: str, quantity: str, trading_symbol: str, trigger_price: str):
        return self.place_order(exchange_segment=exchange_segment, product="MIS",
                                order_type="SL", quantity=quantity, trading_symbol=trading_symbol,
                                transaction_type="S", trigger_price=trigger_price,
                                price=str(float(trigger_price) - 1))

    def place_market_exit(self, exchange_segment: str, quantity: str, trading_symbol: str):
        return self.place_order(exchange_segment=exchange_segment, product="MIS",
                                order_type="MKT", price="", quantity=quantity,
                                trading_symbol=trading_symbol, transaction_type="S")

    def modify_sl(self, order_id: str, quantity: str, new_trigger: str):
        """v3 modify_order REQUIRES quantity — pass the leg's original qty."""
        client = self._require()
        return client.modify_order(order_id=str(order_id),
                                   price=str(float(new_trigger) - 1),
                                   order_type="SL",
                                   quantity=str(quantity),
                                   validity="DAY",
                                   trigger_price=str(new_trigger))

    def cancel(self, order_id: str):
        client = self._require()
        return client.cancel_order(order_id=str(order_id))

    def order_book(self):
        """Full day order book (v3: order_report; order_history needs one order_id)."""
        return self._require().order_report()

    def order_history(self, order_id: str):
        return self._require().order_history(order_id=str(order_id))

    def session_check(self) -> dict:
        """Lightweight session-gated call (margin/limits need a trading session).
        Distinguishes 'session dead' (401 here too) from 'orders specifically broken'."""
        try:
            res = self._require().limits()
        except Exception as e:
            return {"ok": False, "detail": str(e)[:300]}
        if isinstance(res, dict) and res.get("stat") == "Ok":
            return {"ok": True}
        return {"ok": False, "detail": str(res)[:300]}

    def quote_ltp(self, trading_symbol: str, exchange: str) -> float | None:
        """Live LTP for one F&O symbol via v3 quotes(). Returns None on any failure."""
        tok = self.token_for(trading_symbol, exchange)
        if not tok:
            log.warning("quote: no token for %s", trading_symbol)
            return None
        try:
            res = self._require().quotes(
                instrument_tokens=[{"instrument_token": str(tok), "exchange_segment": exchange}])
        except Exception as e:
            log.error("quote %s failed: %s", trading_symbol, e)
            return None
        # Defensive parse — backend shape varies; try common containers/keys.
        cands: list = []
        if isinstance(res, dict):
            data = res.get("data", res)
            cands = data if isinstance(data, list) else [data]
        elif isinstance(res, list):
            cands = res
        for c in cands:
            if not isinstance(c, dict):
                continue
            for k in ("ltp", "iv", "lastTradedPrice", "lastPrice", "c", "close"):
                try:
                    if c.get(k) not in (None, ""):
                        return float(c[k])
                except (TypeError, ValueError):
                    continue
        log.warning("quote: no LTP parsed for %s: %s", trading_symbol, str(res)[:200])
        return None

    # ---------- positions (same aggregation as TransTrader.get_positions) ----------
    def positions(self) -> dict:
        try:
            raw = self._require().positions()
        except Exception as e:
            log.error("positions call failed: %s", e)
            return {}
        if not isinstance(raw, dict) or raw.get("stCode") != 200 or not raw.get("data"):
            return {}
        df = pd.DataFrame(raw["data"])
        num_cols = ["cfBuyQty", "flBuyQty", "cfSellQty", "flSellQty", "cfBuyAmt", "buyAmt",
                    "cfSellAmt", "sellAmt", "lotSz", "multiplier", "genNum", "genDen", "prcNum", "prcDen"]
        for c in num_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce") if c in df.columns else 0
        df.fillna(0, inplace=True)
        df["total_buy_qty"] = df["cfBuyQty"] + df["flBuyQty"]
        df["total_sell_qty"] = df["cfSellQty"] + df["flSellQty"]
        fno = df["exSeg"].isin(["nse_fo", "bse_fo"])
        df.loc[fno, "total_buy_qty"] /= df.loc[fno, "lotSz"].replace(0, 1)
        df.loc[fno, "total_sell_qty"] /= df.loc[fno, "lotSz"].replace(0, 1)
        df["net_qty"] = df["total_buy_qty"] - df["total_sell_qty"]
        df["total_buy_amt"] = df["cfBuyAmt"] + df["buyAmt"]
        df["total_sell_amt"] = df["cfSellAmt"] + df["sellAmt"]
        df["denom"] = (df["multiplier"] * (df["genNum"] / df["genDen"].replace(0, 1))
                       * (df["prcNum"] / df["prcDen"].replace(0, 1))).replace(0, 1)
        df["buy_avg"] = df["total_buy_amt"] / (df["total_buy_qty"] * df["denom"]).replace(0, 1)
        df["sell_avg"] = df["total_sell_amt"] / (df["total_sell_qty"] * df["denom"]).replace(0, 1)
        df["avg_prc"] = df.apply(lambda r: r["buy_avg"] if r["net_qty"] > 0 else r["sell_avg"], axis=1)
        out = {}
        for sym, g in df.groupby("trdSym"):
            q = float(g["net_qty"].sum())
            if q:
                out[sym] = {"qty": q, "avg_prc": round(float(g["avg_prc"].mean()), 2)}
        return out

    # ---------- strike helper (TransTrader logic) ----------
    @staticmethod
    def strikes(instrument: str, option_type: str, spot: float) -> list[str]:
        step = 100 if instrument == "SENSEX" else 50
        base = 50 * round(int(float(spot)) / 50)
        if option_type == "PE":
            return [str(base + step - i * step) for i in range(4)]
        return [str(base - step + i * step) for i in range(4)]
