#!/usr/bin/env python3
"""
STRAT-015: S17 VWAP Premium Fade (SHORT)
Based on quant research: 90.00% WR, +1.04R EV, PF ~25 (50 Polygon-validated trades)

Strategy Overview:
- Single-day SHORT: Stock opens 3-10% above VWAP
- Entry SHORT at 9:31 open (OPEN-KNOWABLE)
- Stop: PM_High + $0.10
- Take profit: entry * 0.85 (15% drop, VWAP is natural target)
- EOD exit at 15:55 ET
- Sub-band sizing:
  - 3-5% premium: FULL (GOLDEN — 100% WR in Polygon)
  - 5-7% premium: REDUCED (64.3% WR — all 5 losses here)
  - 7-10% premium: FULL (100% WR)
"""
import logging
from dataclasses import dataclass, field
from typing import Optional, Tuple, List
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger('s015-strategy')


class SignalType(Enum):
    WATCH = "WATCH"
    ENTERED = "ENTERED"


class TradeStatus(Enum):
    ACTIVE = "ACTIVE"
    WIN = "WIN"
    LOSS = "LOSS"
    EXPIRED = "EXPIRED"


class SignalStrength(Enum):
    GOLDEN = "GOLDEN"     # 3-5% premium — 100% WR
    REDUCED = "REDUCED"   # 5-7% premium — 64.3% WR (all losses here)
    FULL = "FULL"         # 7-10% premium — 100% WR


@dataclass
class S17ScreeningCriteria:
    min_vwap_premium_pct: float = 3.0    # Min 3% above VWAP
    max_vwap_premium_pct: float = 10.0   # Max 10% above VWAP
    min_gap_pct: float = 5.0             # Min gap to be on scanner
    min_price: float = 1.00
    max_price: float = 30.0
    min_pm_volume: int = 50_000


@dataclass
class S17Candidate:
    """A stock with VWAP premium in 3-10% sweet spot"""
    ticker: str
    current_price: float
    prev_close: float
    gap_pct: float
    vwap: float
    vwap_premium_pct: float
    pm_high: float
    pm_low: float
    pm_volume: int

    signal_type: SignalType = field(default=SignalType.WATCH)
    signal_strength: SignalStrength = field(default=SignalStrength.FULL)
    score: float = 0.0
    last_update: Optional[datetime] = None

    entry_triggered: bool = False
    entry_price: float = 0.0
    entry_time: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            'ticker': self.ticker,
            'price': round(self.current_price, 4),
            'gap_pct': round(self.gap_pct, 1),
            'vwap': round(self.vwap, 4),
            'vwap_premium_pct': round(self.vwap_premium_pct, 1),
            'pm_high': round(self.pm_high, 4),
            'pm_volume': self.pm_volume,
            'signal_type': self.signal_type.value,
            'signal_strength': self.signal_strength.value,
            'score': round(self.score, 1),
        }


@dataclass
class ShortTrade:
    """Trade tracking for S17 VWAP Premium Fade SHORT"""
    ticker: str
    entry_price: float = 0.0
    stop_loss: float = 0.0
    target_price: float = 0.0
    pm_high: float = 0.0
    pm_low: float = 0.0
    gap_pct: float = 0.0
    vwap: float = 0.0
    vwap_premium_pct: float = 0.0
    signal_strength: str = "FULL"

    status: TradeStatus = field(default=TradeStatus.ACTIVE)
    high_price: float = 0.0
    low_price: float = 0.0
    exit_price: float = 0.0
    entry_time: Optional[datetime] = None
    exit_time: Optional[datetime] = None
    pnl_pct: float = 0.0
    exit_reason: str = ""

    def set_entry(self, price: float) -> None:
        self.entry_price = price
        # Stop = PM_High + $0.10
        self.stop_loss = round(self.pm_high + 0.10, 4) if self.pm_high > 0 else round(price * 1.15, 4)
        # Ensure stop is at least above entry
        if self.stop_loss <= price:
            self.stop_loss = round(price * 1.05, 4)
        self.target_price = round(price * 0.85, 4)  # 15% drop
        self.high_price = price
        self.low_price = price
        self.entry_time = datetime.now(timezone.utc)

    def update_price(self, current_price: float) -> Optional[TradeStatus]:
        if current_price > self.high_price:
            self.high_price = current_price
        if self.low_price == 0 or current_price < self.low_price:
            self.low_price = current_price

        if current_price >= self.stop_loss and self.stop_loss > 0:
            self._exit(current_price, TradeStatus.LOSS, "STOP_LOSS")
            return TradeStatus.LOSS

        if current_price <= self.target_price and self.target_price > 0:
            self._exit(current_price, TradeStatus.WIN, "TARGET_15PCT_DROP")
            return TradeStatus.WIN

        return None

    def _exit(self, price: float, status: TradeStatus, reason: str) -> None:
        self.status = status
        self.exit_price = price
        self.exit_time = datetime.now(timezone.utc)
        self.pnl_pct = ((self.entry_price - price) / self.entry_price) * 100 if self.entry_price > 0 else 0
        self.exit_reason = reason

    def expire(self, current_price: float = 0.0) -> None:
        if self.status == TradeStatus.ACTIVE:
            self.status = TradeStatus.EXPIRED
            self.exit_time = datetime.now(timezone.utc)
            self.exit_reason = "EOD_1555"
            if current_price > 0 and self.entry_price > 0:
                self.exit_price = current_price
                self.pnl_pct = ((self.entry_price - current_price) / self.entry_price) * 100
            elif self.low_price > 0 and self.entry_price > 0:
                self.exit_price = self.low_price
                self.pnl_pct = ((self.entry_price - self.low_price) / self.entry_price) * 100

    def r_multiple(self) -> float:
        if self.entry_price <= 0:
            return 0.0
        risk = self.stop_loss - self.entry_price
        if risk <= 0:
            return 0.0
        pnl = self.entry_price - self.exit_price if self.exit_price > 0 else 0
        return round(pnl / risk, 2)

    def to_dict(self) -> dict:
        return {
            'ticker': self.ticker,
            'entry_price': round(self.entry_price, 4),
            'stop_loss': round(self.stop_loss, 4),
            'target_price': round(self.target_price, 4),
            'vwap': round(self.vwap, 4),
            'vwap_premium_pct': round(self.vwap_premium_pct, 1),
            'pm_high': round(self.pm_high, 4),
            'pm_low': round(self.pm_low, 4),
            'gap_pct': round(self.gap_pct, 2),
            'signal_strength': self.signal_strength,
            'status': self.status.value,
            'high_price': round(self.high_price, 4),
            'low_price': round(self.low_price, 4),
            'exit_price': round(self.exit_price, 4),
            'pnl_pct': round(self.pnl_pct, 2),
            'exit_reason': self.exit_reason,
            'entry_time': self.entry_time.isoformat() if self.entry_time else None,
            'exit_time': self.exit_time.isoformat() if self.exit_time else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ShortTrade':
        trade = cls(ticker=data['ticker'])
        trade.entry_price = data.get('entry_price', 0)
        trade.stop_loss = data.get('stop_loss', 0)
        trade.target_price = data.get('target_price', 0)
        trade.vwap = data.get('vwap', 0)
        trade.vwap_premium_pct = data.get('vwap_premium_pct', 0)
        trade.pm_high = data.get('pm_high', 0)
        trade.pm_low = data.get('pm_low', 0)
        trade.gap_pct = data.get('gap_pct', 0)
        trade.signal_strength = data.get('signal_strength', 'FULL')
        trade.status = TradeStatus(data.get('status', 'ACTIVE'))
        trade.high_price = data.get('high_price', 0)
        trade.low_price = data.get('low_price', 0)
        trade.exit_price = data.get('exit_price', 0)
        trade.pnl_pct = data.get('pnl_pct', 0)
        trade.exit_reason = data.get('exit_reason', '')
        if data.get('entry_time'):
            trade.entry_time = datetime.fromisoformat(data['entry_time'])
        if data.get('exit_time'):
            trade.exit_time = datetime.fromisoformat(data['exit_time'])
        return trade


class S17Screener:
    """Screens for S17 VWAP Premium Fade candidates"""

    def __init__(self, criteria: S17ScreeningCriteria = None):
        self.criteria = criteria or S17ScreeningCriteria()

    def check_criteria(
        self, ticker: str, price: float, prev_close: float,
        vwap: float, pm_high: float, pm_low: float, pm_volume: int
    ) -> Tuple[bool, List[str], Optional[S17Candidate]]:
        reasons = []
        c = self.criteria

        if price < c.min_price or price > c.max_price:
            reasons.append(f"Price ${price:.2f} out of range")
            return (False, reasons, None)

        gap_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0
        if gap_pct < c.min_gap_pct:
            reasons.append(f"Gap {gap_pct:.1f}% below min {c.min_gap_pct}%")
            return (False, reasons, None)

        if vwap <= 0:
            reasons.append("No VWAP data")
            return (False, reasons, None)

        vwap_premium_pct = ((price - vwap) / vwap) * 100
        if vwap_premium_pct < c.min_vwap_premium_pct:
            reasons.append(f"VWAP premium {vwap_premium_pct:.1f}% below min {c.min_vwap_premium_pct}%")
            return (False, reasons, None)
        if vwap_premium_pct > c.max_vwap_premium_pct:
            reasons.append(f"VWAP premium {vwap_premium_pct:.1f}% above max {c.max_vwap_premium_pct}%")
            return (False, reasons, None)

        if pm_volume < c.min_pm_volume:
            reasons.append(f"PM Vol {pm_volume:,} below min {c.min_pm_volume:,}")
            return (False, reasons, None)

        if pm_high <= 0:
            reasons.append("No PM high data")
            return (False, reasons, None)

        strength = self._determine_strength(vwap_premium_pct)
        score = self._calculate_score(vwap_premium_pct, gap_pct, pm_volume, pm_high, price)

        candidate = S17Candidate(
            ticker=ticker,
            current_price=price,
            prev_close=prev_close,
            gap_pct=gap_pct,
            vwap=vwap,
            vwap_premium_pct=vwap_premium_pct,
            pm_high=pm_high,
            pm_low=pm_low,
            pm_volume=pm_volume,
            signal_strength=strength,
            score=score,
            last_update=datetime.now(timezone.utc)
        )

        logger.info(
            f"[SCREENER] {ticker} PASSED | VWAP+{vwap_premium_pct:.1f}% "
            f"Gap:{gap_pct:.1f}% PMVol:{pm_volume:,} Strength:{strength.value} Score:{score:.0f}"
        )
        return (True, [], candidate)

    def _determine_strength(self, vwap_premium: float) -> SignalStrength:
        """Sub-band sizing from Polygon validation"""
        if 3.0 <= vwap_premium < 5.0:
            return SignalStrength.GOLDEN    # 100% WR
        elif 5.0 <= vwap_premium < 7.0:
            return SignalStrength.REDUCED   # 64.3% WR — all losses
        else:  # 7-10%
            return SignalStrength.FULL      # 100% WR

    def _calculate_score(
        self, vwap_premium: float, gap_pct: float, pm_volume: int,
        pm_high: float, price: float
    ) -> float:
        score = 0.0

        # VWAP premium score (35pts max) — sweet spot 3-5%
        if 3.0 <= vwap_premium < 5.0:
            score += 35  # GOLDEN zone
        elif 7.0 <= vwap_premium <= 10.0:
            score += 30
        elif 5.0 <= vwap_premium < 7.0:
            score += 15  # Danger zone

        # Gap score (25pts max)
        if 10 <= gap_pct <= 30:
            score += 25
        elif 5 <= gap_pct < 10:
            score += 15
        elif gap_pct > 30:
            score += 20

        # PM volume score (20pts max)
        if pm_volume >= 500_000:
            score += 20
        elif pm_volume >= 200_000:
            score += 15
        elif pm_volume >= 100_000:
            score += 10
        else:
            score += 5

        # Risk/reward: PM_High proximity (20pts max)
        if pm_high > 0 and price > 0:
            risk_pct = ((pm_high + 0.10 - price) / price) * 100
            if risk_pct < 5:
                score += 20  # Tight stop
            elif risk_pct < 10:
                score += 15
            else:
                score += 5

        return score
