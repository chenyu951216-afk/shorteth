"""Frozen ETH short momentum state, entry buffer, close-confirmed exits and sizing.

Only fully closed candles are admitted. No historical candle creates a past order.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timezone
import json
import math
from pathlib import Path

import httpx

HOUR_MS = 3_600_000
START_MS = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
ALLOCATION = Decimal('0.0195')
TARGET_LEVERAGE = 150
NOTIONAL_CAP = Decimal('200000')
MOMENTUM_HOURS = 1440
STOP_FRACTION = Decimal('0.06')
MAX_HOLD_HOURS = 168
STRATEGY_ID = 'eth-short-m1440-ema720-buffer0125-v1'


@dataclass(frozen=True)
class Signal:
    bar_open_ms: int
    decided_at_ms: int
    desired_short: bool
    momentum: float
    ema_slow: float
    last_close: float
    candles: int
    entry_allowed: bool
    last_flat_bar_ms: int


def evaluate(closes: list[tuple[int, float]], now_ms: int) -> Signal:
    if len(closes) < MOMENTUM_HOURS + 2:
        raise ValueError('NEED_1442_COMPLETE_HOURLY_BARS')
    if any(not math.isfinite(price) or not (price > 0) for _, price in closes):
        raise ValueError('INVALID_CLOSE')
    if any(b - a != HOUR_MS for (a, _), (b, _) in zip(closes, closes[1:])):
        raise ValueError('MISSING_OR_DUPLICATE_HOUR')
    last = closes[-1][0]
    if last + HOUR_MS > now_ms or now_ms - (last + HOUR_MS) >= HOUR_MS:
        raise ValueError('LAST_COMPLETE_HOUR_MISSING_OR_FUTURE')
    return list(signal_stream(closes))[-1]


def signal_stream(closes):
    """O(n) full-history state reconstruction; no position or simulated order state.

    Original research: entry r1440 < -5%, close < EMA720; exit r>=0 OR
    close>=EMA720. Entry is warmed through index 1440. Buffer is entry-only,
    and must never turn the hysteretic raw state off while a short is held.
    Each result is actionable only AFTER this candle's close.
    """
    active=False;slow=float(closes[0][1]);last_flat=closes[0][0]
    alpha=2/721
    for i,(stamp,close) in enumerate(closes):
        if i:slow=alpha*close+(1-alpha)*slow
        momentum=close/closes[i-MOMENTUM_HOURS][1]-1 if i>=MOMENTUM_HOURS else 0.
        if active and (momentum>=0 or close>=slow):active=False
        elif not active and i>=MOMENTUM_HOURS+1 and momentum<-.05 and close<slow:active=True
        if not active:last_flat=stamp
        yield Signal(stamp,stamp+HOUR_MS,active,momentum,slow,float(close),i+1,
                     active and close<slow*.99875,last_flat)


def exit_reason(signal: Signal, entry_price: str, entry_hour_ms: int) -> str | None:
    """Priority exactly matches the frozen engine. No intrabar 6% hard stop."""
    if not signal.desired_short:return 'signal_reset'
    if signal.decided_at_ms>entry_hour_ms and Decimal(str(signal.last_close))>=Decimal(str(entry_price))*(1+STOP_FRACTION):
        return 'close_stop_6pct'
    if signal.decided_at_ms-entry_hour_ms>=MAX_HOLD_HOURS*HOUR_MS:return 'max_hold_168h'
    return None


def target_notional(account_equity_usdt: str) -> Decimal:
    equity = Decimal(str(account_equity_usdt))
    if not equity.is_finite() or equity <= 0:
        raise ValueError('ACCOUNT_EQUITY_NOT_POSITIVE')
    return min(equity * ALLOCATION * TARGET_LEVERAGE, NOTIONAL_CAP)


async def fetch_complete_closes(client: httpx.AsyncClient, now_ms: int,
                                cache_path: str | Path | None = None) -> list[tuple[int, float]]:
    """Fetch Binance futures hourly closes from 2020 and reject any gap.

    Full-history seeding avoids changing EMA720 when the program restarts.
    This public feed is the frozen backtest signal source; execution quotes come
    independently from Bitget.
    """
    latest = (now_ms // HOUR_MS - 1) * HOUR_MS
    cursor = START_MS
    closes: list[tuple[int, float]] = []
    if cache_path and Path(cache_path).exists():
        raw = json.loads(Path(cache_path).read_text(encoding='utf-8'))
        closes = [(int(t), float(p)) for t, p in raw]
        if closes and (closes[0][0] != START_MS or
                       any(b-a != HOUR_MS for (a,_),(b,_) in zip(closes,closes[1:]))):
            raise ValueError('LOCAL_CANDLE_CACHE_GAP')
        closes = [(t,p) for t,p in closes if t <= latest]
        if closes: cursor = closes[-1][0] + HOUR_MS
    while cursor <= latest:
        response = await client.get('/fapi/v1/klines', params={
            'symbol': 'ETHUSDT', 'interval': '1h', 'startTime': cursor,
            'endTime': latest + HOUR_MS - 1, 'limit': 1500})
        response.raise_for_status()
        rows = response.json()
        if not isinstance(rows, list) or not rows:
            raise ValueError('BINANCE_HOURLY_HISTORY_INCOMPLETE')
        for row in rows:
            opened = int(row[0])
            if opened < cursor or opened > latest or int(row[6]) >= now_ms:
                continue
            if closes and opened != closes[-1][0] + HOUR_MS:
                raise ValueError('BINANCE_HOURLY_HISTORY_GAP')
            closes.append((opened, float(row[4])))
        new_cursor = int(rows[-1][0]) + HOUR_MS
        if new_cursor <= cursor:
            raise ValueError('BINANCE_HISTORY_PAGINATION_STALLED')
        cursor = new_cursor
    if not closes or closes[-1][0] != latest:
        raise ValueError('LATEST_COMPLETE_BINANCE_HOUR_UNAVAILABLE')
    if cache_path:
        path = Path(cache_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix('.tmp')
        temp.write_text(json.dumps(closes, separators=(',',':')), encoding='utf-8')
        temp.replace(path)
    return closes
