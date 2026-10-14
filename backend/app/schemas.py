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
    # native BO passthrough (optional — only sent if product == BO)
    product: Literal["MIS", "BO", "CO", "NRML"] = "MIS"
    bo_square_off: float | None = None
    bo_stop_loss: float | None = None
    bo_trailing: str | None = None  # 'Y'/'N'


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
    product: Literal["MIS", "BO"] = "MIS"
