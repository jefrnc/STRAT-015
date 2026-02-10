#!/usr/bin/env python3
"""Discord Webhook Notifier for STRAT-015 (S17 VWAP Premium Fade SHORT)"""
import logging
import httpx
from datetime import datetime, timezone
from typing import Optional
import pytz

logger = logging.getLogger('s015-discord')
ET_TZ = pytz.timezone('America/New_York')
UTC_TZ = pytz.UTC

def to_et(dt):
    if dt is None: return None
    if dt.tzinfo is None: dt = UTC_TZ.localize(dt)
    return dt.astimezone(ET_TZ)


class DiscordNotifier:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
        self.enabled = bool(webhook_url)
        self.http_client: Optional[httpx.AsyncClient] = None

    async def _ensure_client(self):
        if self.http_client is None:
            self.http_client = httpx.AsyncClient(timeout=10.0)

    async def send_message(self, content=None, embed=None) -> bool:
        if not self.enabled: return False
        await self._ensure_client()
        payload = {}
        if content: payload['content'] = content
        if embed: payload['embeds'] = [embed]
        if not payload: return False
        try:
            resp = await self.http_client.post(self.webhook_url, json=payload)
            return resp.status_code in [200, 204]
        except Exception as e:
            logger.error(f"Discord error: {e}")
            return False

    async def send_watch_notification(self, candidate) -> bool:
        from s17_strategy import S17Candidate
        if not isinstance(candidate, S17Candidate): return False

        strength_emoji = {"GOLDEN": "GOLDEN", "REDUCED": "WEAK", "FULL": "STRONG"}
        strength = strength_emoji.get(candidate.signal_strength.value, candidate.signal_strength.value)

        embed = {
            "title": f"S17 VWAP PREMIUM WATCH: {candidate.ticker}",
            "color": 0xE67E22,  # Orange
            "description": f"VWAP+{candidate.vwap_premium_pct:.1f}% premium detected — monitoring for SHORT at 9:31",
            "fields": [
                {"name": "Price", "value": f"${candidate.current_price:.4f}", "inline": True},
                {"name": "VWAP Premium", "value": f"+{candidate.vwap_premium_pct:.1f}%", "inline": True},
                {"name": "Signal Band", "value": strength, "inline": True},
                {"name": "Gap", "value": f"{candidate.gap_pct:+.1f}%", "inline": True},
                {"name": "PM High", "value": f"${candidate.pm_high:.4f}", "inline": True},
                {"name": "PM Volume", "value": f"{candidate.pm_volume:,}", "inline": True},
                {"name": "Score", "value": f"{candidate.score:.0f}", "inline": True},
            ],
            "footer": {"text": "STRAT-015 | S17 VWAP Premium Fade SHORT | 90% WR"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        return await self.send_message(embed=embed)

    async def send_entry_notification(self, candidate, trade) -> bool:
        from s17_strategy import ShortTrade
        if not isinstance(trade, ShortTrade): return False

        risk = trade.stop_loss - trade.entry_price
        reward = trade.entry_price - trade.target_price
        rr = reward / risk if risk > 0 else 0

        embed = {
            "title": f"S17 VWAP PREMIUM ENTRY: {trade.ticker} SHORT",
            "color": 0xFF0000,
            "description": "VWAP Premium Fade SHORT at 9:31 open",
            "fields": [
                {"name": "Entry", "value": f"${trade.entry_price:.4f}", "inline": True},
                {"name": "Stop (PM_H+$0.10)", "value": f"${trade.stop_loss:.4f}", "inline": True},
                {"name": "Target (-15%)", "value": f"${trade.target_price:.4f}", "inline": True},
                {"name": "VWAP+", "value": f"{trade.vwap_premium_pct:.1f}%", "inline": True},
                {"name": "R:R", "value": f"1:{rr:.1f}", "inline": True},
                {"name": "Strength", "value": trade.signal_strength, "inline": True},
            ],
            "footer": {"text": "STRAT-015 | S17 VWAP Premium Fade SHORT | 90% WR"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        content = f"@here **S17 VWAP Premium SHORT: {trade.ticker} @ ${trade.entry_price:.4f}** | VWAP+{trade.vwap_premium_pct:.1f}% | {trade.signal_strength}"
        return await self.send_message(content=content, embed=embed)

    async def send_trade_result(self, trade) -> bool:
        from s17_strategy import ShortTrade, TradeStatus
        if not isinstance(trade, ShortTrade): return False
        r_mult = trade.r_multiple()
        if trade.status == TradeStatus.WIN: color, emoji = 0x00FF00, "WIN"
        elif trade.status == TradeStatus.LOSS: color, emoji = 0xFF0000, "LOSS"
        else: color, emoji = 0x808080, "EXPIRED"

        embed = {
            "title": f"{emoji}: {trade.ticker} SHORT ({trade.exit_reason})",
            "color": color,
            "fields": [
                {"name": "Entry", "value": f"${trade.entry_price:.4f}", "inline": True},
                {"name": "Exit", "value": f"${trade.exit_price:.4f}", "inline": True},
                {"name": "P&L", "value": f"{trade.pnl_pct:+.2f}% ({r_mult:+.2f}R)", "inline": True},
                {"name": "VWAP+", "value": f"{trade.vwap_premium_pct:.1f}%", "inline": True},
                {"name": "Entry Time", "value": to_et(trade.entry_time).strftime("%H:%M ET") if trade.entry_time else "N/A", "inline": True},
                {"name": "Exit Time", "value": to_et(trade.exit_time).strftime("%H:%M ET") if trade.exit_time else "N/A", "inline": True},
            ],
            "footer": {"text": "STRAT-015 | S17 VWAP Premium Fade"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        return await self.send_message(embed=embed)

    async def send_daily_summary(self, trades, candidates_count, summary_time) -> bool:
        from s17_strategy import ShortTrade, TradeStatus
        wins = [t for t in trades if isinstance(t, ShortTrade) and t.status == TradeStatus.WIN]
        losses = [t for t in trades if isinstance(t, ShortTrade) and t.status == TradeStatus.LOSS]
        active = [t for t in trades if isinstance(t, ShortTrade) and t.status == TradeStatus.ACTIVE]
        total = len(wins) + len(losses)
        wr = (len(wins) / total * 100) if total > 0 else 0
        total_pnl = sum(t.pnl_pct for t in wins) + sum(t.pnl_pct for t in losses)
        color = 0x00FF00 if total_pnl > 0 else (0xFF0000 if total_pnl < 0 else 0x808080)

        embed = {
            "title": f"S17 VWAP Premium Summary - {summary_time} ET",
            "color": color,
            "description": f"**{len(wins)}** Wins | **{len(losses)}** Losses | **{len(active)}** Active",
            "fields": [
                {"name": "Win Rate", "value": f"{wr:.1f}%" if total > 0 else "N/A", "inline": True},
                {"name": "Total P&L", "value": f"{total_pnl:+.2f}%", "inline": True},
                {"name": "Candidates", "value": str(candidates_count), "inline": True},
            ],
            "footer": {"text": f"STRAT-015 | {datetime.now(timezone.utc).strftime('%Y-%m-%d')}"},
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        return await self.send_message(embed=embed)

    async def close(self):
        if self.http_client:
            await self.http_client.aclose()
            self.http_client = None
