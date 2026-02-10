#!/usr/bin/env python3
"""Webhook Client for STRAT-015 (S17 VWAP Premium Fade SHORT)"""
import asyncio
import logging
import time
from typing import Optional
import httpx

logger = logging.getLogger('s015-webhook')
DEFAULT_WEBHOOK_URL = "https://das-signal-bridge.amezco.cloud/api/webhook"


class WebhookClient:
    def __init__(self, webhook_url=None, discord_notifier=None, max_retries=3, retry_delay=1.0):
        self.webhook_url = webhook_url or DEFAULT_WEBHOOK_URL
        self.discord = discord_notifier
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.http_client: Optional[httpx.AsyncClient] = None
        self.enabled = bool(self.webhook_url)
        self.signals_sent = 0
        self.signals_failed = 0

    async def init_client(self):
        if not self.http_client:
            self.http_client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0), limits=httpx.Limits(max_connections=10))

    async def test_connectivity(self) -> bool:
        await self.init_client()
        try:
            resp = await self.http_client.get(self.webhook_url.replace('/api/webhook', '/health'), timeout=5.0)
            return resp.status_code < 500
        except: return False

    async def close(self):
        if self.http_client: await self.http_client.aclose(); self.http_client = None

    async def send_watch_signal(self, candidate) -> bool:
        if not self.enabled: return True
        payload = {
            "signal_id": f"S015-WATCH-{candidate.ticker}-{int(time.time())}",
            "scanner": "S015", "signal_type": "WATCH", "ticker": candidate.ticker,
            "price": round(candidate.current_price, 4), "side": "SHORT",
            "vwap_premium_pct": round(candidate.vwap_premium_pct, 1),
            "signal_strength": candidate.signal_strength.value,
            "score": int(candidate.score),
        }
        return await self._send_webhook(payload, "WATCH", candidate.ticker)

    async def send_entry_signal(self, candidate, trade) -> bool:
        if not self.enabled: return True
        payload = {
            "signal_id": f"S015-ENTRY-{trade.ticker}-{int(time.time())}",
            "scanner": "S015", "signal_type": "ENTRY", "ticker": trade.ticker,
            "price": round(trade.entry_price, 4), "side": "SHORT",
            "entry": round(trade.entry_price, 4),
            "stop_loss": round(trade.stop_loss, 4),
            "target_1": round(trade.target_price, 4),
            "vwap_premium_pct": round(trade.vwap_premium_pct, 1),
            "signal_strength": trade.signal_strength,
        }
        return await self._send_webhook(payload, "ENTRY", trade.ticker)

    async def _send_webhook(self, payload, signal_type, ticker) -> bool:
        await self.init_client()
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = await self.http_client.post(self.webhook_url, json=payload, headers={"Content-Type": "application/json"})
                if resp.status_code in (200, 201, 202, 204):
                    self.signals_sent += 1; return True
            except: pass
            if attempt < self.max_retries: await asyncio.sleep(self.retry_delay * attempt)
        self.signals_failed += 1; return False
