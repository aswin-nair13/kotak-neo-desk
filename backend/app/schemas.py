from typing import Literal
from pydantic import BaseModel, Field


class LoginBody(BaseModel):
    username: str
    password: str


class KotakInitBody(BaseModel):
    pass  # consumer key comes from server env


class KotakCompleteBody(BaseModel):
    totp: str = Field(min_length=4, max_length=10)


class SingleOrderBody(BaseModel):
    instrument: Literal["NIFTY", "SENSEX"]
    expiry: str                      # e.g. 25N18 — pick from GET /api/market/expiries
    option_type: Literal["CE", "PE"]
    strike: str
    lots: int = 1
    entry_mode: Literal["MKT", "ML", "L", "L-1"] = "MKT"
    entry_offset: float = 0          # used when entry_mode == L
    target_points: float = 20
    stop_points: float = 10
    strategy: Literal["market_exit", "trailing_stop"] = "market_exit"
    trail_distance: float = 5.0
    # side: B = buy (long), S = sell / short. SL + exits auto-invert for shorts.
    side: Literal["B", "S"] = "B"
    # SDK v3 place_order has no BO/CO kwargs — bracket behaviour is synthetic
    # (entry + auto SL + target/trailing via SyntheticEngine), so MIS/NRML only.
    product: Literal["MIS", "NRML", "CNC"] = "MIS"


class StraddleBody(BaseModel):
    instrument: Literal["NIFTY", "SENSEX"] = "NIFTY"
    expiry: str = ""                 # e.g. 25N18 — pick from GET /api/market/expiries
    ce_strike: str
    pe_strike: str
    lots: int = 1
    entry_mode: Literal["MKT", "ML", "L-1"] = "MKT"
    target_points: float = 20
    stop_points: float = 10
    strategy: Literal["market_exit", "trailing_stop"] = "market_exit"
    trail_distance: float = 5.0
    # side applies to both legs: B = buy straddle, S = sell straddle.
    side: Literal["B", "S"] = "B"
    product: Literal["MIS", "NRML", "CNC"] = "MIS"


class WatchLeg(BaseModel):
    instrument: Literal["NIFTY", "SENSEX"] = "NIFTY"
    expiry: str = ""
    strike: str = ""
    option_type: Literal["CE", "PE"] = "CE"


class WatchBody(BaseModel):
    legs: list[WatchLeg] = []


class ExitBody(BaseModel):
    symbol: str = Field(min_length=3, max_length=60)
    # product defaults to the tracked leg's product (or broker position's),
    # override only if you moved the position across products.
    product: str | None = None
