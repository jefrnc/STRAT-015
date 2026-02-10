#!/usr/bin/env python3
"""
STRAT-015: S17 VWAP Premium Fade SHORT Scanner Service

Based on quant research: 90.00% WR, +1.04R EV, PF ~25 (50 Polygon-validated trades)
Simplest scanner — single formula: VWAP premium 3-10%.

Signal: Stock opens 3-10% above VWAP -> SHORT at 9:31

Loops:
  1. pm_scan_loop          (30s, 4:00-9:30 ET)  - Find gappers with VWAP premium 3-10%
  2. open_entry_loop       (5s,  fires at 9:31)  - Enter SHORT at 9:31 open
  3. trade_mgmt_loop       (10s, 9:30-15:55 ET)  - SL/TP/EOD management
  4. cleanup_loop          (5min)                 - Remove stale candidates
  5. summary_loop          (30s)                  - Daily summaries
"""
import asyncio
import logging
import os
import sys
import json
import httpx
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import pytz

shared_lib_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'shared-lib')
sys.path.insert(0, shared_lib_path)

from smallcaps_shared import RedisClient, DatabaseService, HealthServer, StrategySignal, StrategySignalDispatcher

from s17_strategy import (
    S17Screener, S17ScreeningCriteria, S17Candidate,
    ShortTrade, SignalType, TradeStatus, SignalStrength
)
from discord_notifier import DiscordNotifier
from webhook_client import WebhookClient

SUMMARY_TIMES_ET = ["08:00", "11:00", "14:00", "17:00", "20:00"]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('s015-scanner')

POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '')
POLYGON_BASE_URL = "https://api.polygon.io"
DISCORD_WEBHOOK_URL = os.getenv('S015_DISCORD_WEBHOOK', '')
SIGNAL_WEBHOOK_URL = os.getenv('S015_SIGNAL_WEBHOOK', '')
SCAN_INTERVAL = int(os.getenv('SCAN_INTERVAL', '30'))

ET_TZ = pytz.timezone('America/New_York')


class STRAT015Service:
    """STRAT-015 S17 VWAP Premium Fade SHORT Scanner"""

    def __init__(self):
        self.redis = RedisClient()
        self.db = DatabaseService()
        self.health_server = HealthServer(
            service_name="strat-015-scanner",
            port=8037,
            requires_redis=True,
            requires_scanning=True,
            scan_timeout=120
        )
        self.http_client: Optional[httpx.AsyncClient] = None

        self.screener = S17Screener(S17ScreeningCriteria())

        self.discord = DiscordNotifier(DISCORD_WEBHOOK_URL)
        self.webhook = WebhookClient(
            webhook_url=SIGNAL_WEBHOOK_URL,
            discord_notifier=self.discord,
            max_retries=3
        )
        self.signal_dispatcher = StrategySignalDispatcher(db_service=self.db)

        self.candidates: Dict[str, S17Candidate] = {}
        self.active_trades: Dict[str, ShortTrade] = {}
        self.completed_trades: list = []
        self.watch_notified: set = set()
        self.signals_notified: set = set()
        self.last_summary_time: str = ""
        self.open_entries_done: bool = False

    async def start(self):
        logger.info("Starting STRAT-015 S17 VWAP Premium Fade SHORT Scanner...")

        self.http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=5.0),
            limits=httpx.Limits(max_connections=20)
        )

        await self.redis.connect()
        if self.redis.is_connected:
            logger.info("[REDIS] Connected")
            self.health_server.set_redis_status(True)
        else:
            logger.error("[REDIS] Failed to connect")
            self.health_server.set_redis_status(False)

        await self.db.init()
        self.health_server.set_db_status(True)
        logger.info("[DB] Connected")

        await self._load_state_from_redis()
        await self.webhook.test_connectivity()

        asyncio.create_task(self.health_server.start())

        logger.info("STRAT-015 Scanner ready - starting loops")

        await asyncio.gather(
            self.pm_scan_loop(),
            self.open_entry_loop(),
            self.trade_management_loop(),
            self.cleanup_loop(),
            self.summary_scheduler_loop()
        )

    async def stop(self):
        logger.info("Shutting down STRAT-015 Scanner...")
        if self.http_client:
            await self.http_client.aclose()
        await self.webhook.close()

    # ====================================================================
    # TIME HELPERS
    # ====================================================================

    def _now_et(self) -> datetime:
        return datetime.now(ET_TZ)

    def _is_premarket(self) -> bool:
        now = self._now_et()
        return now.hour >= 4 and (now.hour < 9 or (now.hour == 9 and now.minute < 30))

    def _is_market_open(self) -> bool:
        now = self._now_et()
        if now.hour == 9 and now.minute >= 30:
            return True
        return 10 <= now.hour < 16

    def _is_weekday(self) -> bool:
        return self._now_et().weekday() < 5

    async def _is_trading_day(self, date: datetime = None) -> bool:
        try:
            from smallcaps_shared.market_status import get_market_checker
            market_checker = get_market_checker()
            target_date = date.date() if date else None
            return await market_checker.is_trading_day(target_date)
        except Exception as e:
            logger.warning(f"Error checking trading day: {e}")
            check_date = date or self._now_et()
            return check_date.weekday() < 5

    def _today_str(self) -> str:
        return self._now_et().strftime("%Y-%m-%d")

    # ====================================================================
    # REDIS PERSISTENCE
    # ====================================================================

    async def _load_state_from_redis(self):
        today = self._today_str()
        try:
            watch_data = await self.redis.get(f"s015:watch_notified:{today}")
            if watch_data and isinstance(watch_data, list):
                self.watch_notified = set(watch_data)

            signals_data = await self.redis.get(f"s015:signals_notified:{today}")
            if signals_data and isinstance(signals_data, list):
                self.signals_notified = set(signals_data)

            trades_data = await self.redis.get(f"s015:active_trades:{today}")
            if trades_data and isinstance(trades_data, dict):
                for ticker, trade_json in trades_data.items():
                    if isinstance(trade_json, str):
                        trade_json = json.loads(trade_json)
                    self.active_trades[ticker] = ShortTrade.from_dict(trade_json)
                logger.info(f"[REDIS] Loaded {len(self.active_trades)} active trades")

            completed_data = await self.redis.get(f"s015:completed_trades:{today}")
            if completed_data and isinstance(completed_data, dict):
                for trade_id, trade_json in completed_data.items():
                    try:
                        td = json.loads(trade_json) if isinstance(trade_json, str) else trade_json
                        self.completed_trades.append(ShortTrade.from_dict(td))
                    except Exception as e:
                        logger.warning(f"[REDIS] Failed to restore trade {trade_id}: {e}")

            flags = await self.redis.get(f"s015:daily_flags:{today}")
            if flags and isinstance(flags, dict):
                self.open_entries_done = flags.get('open_entries_done', False)

        except Exception as e:
            logger.error(f"[REDIS] Error loading state: {e}")

    async def _save_watch_notified(self, ticker: str):
        self.watch_notified.add(ticker)
        today = self._today_str()
        try:
            await self.redis.set(f"s015:watch_notified:{today}", list(self.watch_notified), ttl=86400)
        except Exception as e:
            logger.error(f"[REDIS] Error saving watch_notified: {e}")

    async def _save_signal_notified(self, signal_id: str):
        self.signals_notified.add(signal_id)
        today = self._today_str()
        try:
            await self.redis.set(f"s015:signals_notified:{today}", list(self.signals_notified), ttl=86400)
        except Exception as e:
            logger.error(f"[REDIS] Error saving signals_notified: {e}")

    async def _save_active_trades(self):
        today = self._today_str()
        try:
            trades_data = {}
            for t_ticker, t_trade in self.active_trades.items():
                trades_data[t_ticker] = json.dumps(t_trade.to_dict())
            await self.redis.set(f"s015:active_trades:{today}", trades_data, ttl=86400)
        except Exception as e:
            logger.error(f"[REDIS] Error saving active trades: {e}")

    async def _save_completed_trade(self, trade: ShortTrade):
        today = self._today_str()
        try:
            key = f"s015:completed_trades:{today}"
            existing = await self.redis.get(key) or {}
            trade_id = f"{trade.ticker}_{int(trade.entry_time.timestamp()) if trade.entry_time else 0}"
            existing[trade_id] = json.dumps(trade.to_dict())
            await self.redis.set(key, existing, ttl=86400)
        except Exception as e:
            logger.error(f"[REDIS] Error saving completed trade: {e}")

    async def _save_daily_flags(self):
        today = self._today_str()
        try:
            await self.redis.set(f"s015:daily_flags:{today}", {'open_entries_done': self.open_entries_done}, ttl=86400)
        except Exception as e:
            logger.error(f"[REDIS] Error saving daily flags: {e}")

    # ====================================================================
    # DATA FETCHING (Polygon)
    # ====================================================================

    async def get_ticker_snapshot(self, ticker: str) -> Optional[dict]:
        if not self.http_client:
            return None
        try:
            url = f"{POLYGON_BASE_URL}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
            resp = await self.http_client.get(url, params={"apiKey": POLYGON_API_KEY})
            if resp.status_code != 200:
                return None

            data = resp.json()
            t = data.get('ticker', {})
            day = t.get('day', {})
            prev = t.get('prevDay', {})
            last_trade = t.get('lastTrade', {})

            return {
                'price': last_trade.get('p', 0) or day.get('c', 0) or day.get('vw', 0),
                'prev_close': prev.get('c', 0),
                'vwap': day.get('vw', 0),
                'volume': day.get('v', 0),
                'day_high': day.get('h', 0),
                'day_low': day.get('l', 0),
                'day_open': day.get('o', 0),
            }
        except Exception as e:
            logger.error(f"[POLYGON] Snapshot error for {ticker}: {e}")
            return None

    async def get_pm_data(self, ticker: str) -> Optional[dict]:
        if not self.http_client:
            return None
        try:
            today = self._now_et().strftime("%Y-%m-%d")
            url = f"{POLYGON_BASE_URL}/v2/aggs/ticker/{ticker}/range/1/minute/{today}/{today}"
            resp = await self.http_client.get(url, params={
                "apiKey": POLYGON_API_KEY, "adjusted": "true", "sort": "asc", "limit": 500
            })
            if resp.status_code != 200:
                return None

            results = resp.json().get('results', [])
            if not results:
                return None

            pm_candles = []
            for c in results:
                ts_ms = c.get('t', 0)
                dt = datetime.utcfromtimestamp(ts_ms / 1000).replace(tzinfo=pytz.UTC)
                et_dt = dt.astimezone(ET_TZ)
                if et_dt.hour < 9 or (et_dt.hour == 9 and et_dt.minute < 30):
                    pm_candles.append(c)

            if not pm_candles:
                return None

            return {
                'pm_high': max(c.get('h', 0) for c in pm_candles),
                'pm_low': min(c.get('l', float('inf')) for c in pm_candles),
                'pm_volume': int(sum(c.get('v', 0) for c in pm_candles)),
            }
        except Exception as e:
            logger.error(f"[POLYGON] PM data error for {ticker}: {e}")
            return None

    # ====================================================================
    # LOOP 1: PM SCAN LOOP
    # ====================================================================

    async def pm_scan_loop(self):
        logger.info("[LOOP] pm_scan_loop started")
        while True:
            try:
                if self._is_weekday() and (self._is_premarket() or self._is_market_open()):
                    await self._scan_for_candidates()
                    self.health_server.update_scan_status()
                else:
                    await asyncio.sleep(60 if self._is_weekday() else 300)
                    continue
            except Exception as e:
                logger.error(f"[LOOP] pm_scan_loop error: {e}", exc_info=True)

            await asyncio.sleep(SCAN_INTERVAL)

    async def _scan_for_candidates(self):
        for scan_type in ['gap', 'breakout', 'momentum']:
            try:
                key = f"scan_results:{scan_type}"
                data = await self.redis.get(key)
                if not data:
                    continue

                items = data if isinstance(data, list) else []
                for item in items[:50]:
                    ticker = item.get('ticker', '')
                    if not ticker or ticker in self.candidates:
                        continue
                    await self._process_candidate(ticker, item, scan_type)

            except Exception as e:
                logger.error(f"[SCAN] Error reading {scan_type}: {e}")

        logger.info(f"[SCAN] Tracking {len(self.candidates)} VWAP premium candidates")

    async def _process_candidate(self, ticker: str, scan_data: dict, source: str):
        try:
            snapshot = await self.get_ticker_snapshot(ticker)
            if not snapshot or snapshot.get('price', 0) <= 0:
                return

            vwap = snapshot.get('vwap', 0)
            price = snapshot['price']
            if vwap <= 0 or price <= vwap:
                return

            # Quick VWAP premium pre-filter
            vwap_prem = ((price - vwap) / vwap) * 100
            if vwap_prem < 3.0 or vwap_prem > 10.0:
                return

            pm_data = await self.get_pm_data(ticker)
            if not pm_data or pm_data.get('pm_high', 0) <= 0:
                return

            prev_close = snapshot.get('prev_close', 0) or scan_data.get('prev_close', 0)

            passed, reasons, candidate = self.screener.check_criteria(
                ticker=ticker,
                price=price,
                prev_close=prev_close,
                vwap=vwap,
                pm_high=pm_data['pm_high'],
                pm_low=pm_data.get('pm_low', 0),
                pm_volume=pm_data['pm_volume'],
            )

            if passed and candidate:
                candidate.signal_type = SignalType.WATCH
                self.candidates[ticker] = candidate

                if ticker not in self.watch_notified:
                    logger.info(
                        f"[WATCH] VWAP Premium SHORT: {ticker} | "
                        f"VWAP+{candidate.vwap_premium_pct:.1f}% Gap:{candidate.gap_pct:.1f}% "
                        f"Strength:{candidate.signal_strength.value}"
                    )
                    await self.discord.send_watch_notification(candidate)
                    await self.webhook.send_watch_signal(candidate)
                    await self._dispatch_signal(candidate, "WATCH")
                    await self._save_watch_notified(ticker)

        except Exception as e:
            logger.error(f"[PROCESS] Error processing {ticker}: {e}", exc_info=True)

    # ====================================================================
    # LOOP 2: OPEN ENTRY LOOP
    # ====================================================================

    async def open_entry_loop(self):
        logger.info("[LOOP] open_entry_loop started")
        while True:
            try:
                now = self._now_et()
                if (self._is_weekday() and not self.open_entries_done
                        and now.hour == 9 and now.minute >= 31):
                    await self._execute_open_entries()
                    self.open_entries_done = True
                    await self._save_daily_flags()
            except Exception as e:
                logger.error(f"[LOOP] open_entry error: {e}", exc_info=True)

            await asyncio.sleep(5)

    async def _execute_open_entries(self):
        if not self.candidates:
            logger.info("[ENTRY] No VWAP premium candidates to SHORT at 9:31")
            return

        entered = 0
        for ticker, candidate in list(self.candidates.items()):
            if candidate.signal_type != SignalType.WATCH:
                continue
            if ticker in self.active_trades:
                continue

            try:
                snapshot = await self.get_ticker_snapshot(ticker)
                if not snapshot or snapshot.get('price', 0) <= 0:
                    continue

                current_price = snapshot['price']
                vwap = snapshot.get('vwap', candidate.vwap)

                if vwap > 0 and current_price <= vwap:
                    logger.info(f"[ENTRY] {ticker}: no longer above VWAP, skipping")
                    continue

                # Re-check VWAP premium is still in range at entry
                new_premium = ((current_price - vwap) / vwap) * 100 if vwap > 0 else 0
                if new_premium < 2.0 or new_premium > 12.0:
                    logger.info(f"[ENTRY] {ticker}: VWAP premium {new_premium:.1f}% out of range, skipping")
                    continue

                trade = ShortTrade(
                    ticker=ticker,
                    vwap=vwap,
                    vwap_premium_pct=new_premium,
                    pm_high=candidate.pm_high,
                    pm_low=candidate.pm_low,
                    gap_pct=candidate.gap_pct,
                    signal_strength=candidate.signal_strength.value,
                )
                trade.set_entry(current_price)

                candidate.signal_type = SignalType.ENTERED
                candidate.entry_triggered = True
                candidate.entry_price = current_price
                candidate.entry_time = trade.entry_time

                self.active_trades[ticker] = trade
                entered += 1

                logger.info(
                    f"[ENTRY] {ticker} SHORT at ${current_price:.4f} | "
                    f"SL: ${trade.stop_loss:.4f} (PM_H+$0.10) | TP: ${trade.target_price:.4f} (-15%) | "
                    f"VWAP+{new_premium:.1f}% | Strength: {trade.signal_strength}"
                )

                await self.discord.send_entry_notification(candidate, trade)
                await self.webhook.send_entry_signal(candidate, trade)
                await self._dispatch_entry_signal(candidate, trade)

                signal_id = f"{ticker}:ENTRY"
                await self._save_signal_notified(signal_id)
                await self._create_alert(ticker, trade)

            except Exception as e:
                logger.error(f"[ENTRY] Error entering {ticker}: {e}", exc_info=True)

        await self._save_active_trades()
        logger.info(f"[ENTRY] Entered {entered} VWAP Premium SHORT trades at 9:31")

    async def _create_alert(self, ticker: str, trade: ShortTrade):
        try:
            risk = trade.stop_loss - trade.entry_price
            reward = trade.entry_price - trade.target_price
            rr = reward / risk if risk > 0 else 0
            message = (
                f"S015 VWAP Premium SHORT ENTRY: {ticker} at ${trade.entry_price:.4f}. "
                f"SL: ${trade.stop_loss:.4f} (PM_H+$0.10), TP: ${trade.target_price:.4f} (-15%), "
                f"R:R 1:{rr:.1f}, VWAP+{trade.vwap_premium_pct:.1f}%, {trade.signal_strength}"
            )
            await self.db.execute(
                "INSERT INTO alerts (ticker, alert_type, price, message, created_at) VALUES ($1, $2, $3, $4, NOW())",
                ticker, 'S015_ENTRY', trade.entry_price, message
            )
        except Exception as e:
            logger.error(f"[ALERT] Error creating alert for {ticker}: {e}")

    # ====================================================================
    # LOOP 3: TRADE MANAGEMENT
    # ====================================================================

    async def trade_management_loop(self):
        logger.info("[LOOP] trade_management_loop started")
        while True:
            try:
                if self._is_weekday() and self.active_trades:
                    now = self._now_et()
                    if self._is_market_open():
                        if now.hour == 15 and now.minute >= 55:
                            await self._force_close_all_trades()
                        else:
                            await self._manage_trades()
                    elif now.hour >= 16 and now.hour < 20:
                        await self._force_close_all_trades()
            except Exception as e:
                logger.error(f"[LOOP] trade_management error: {e}", exc_info=True)

            await asyncio.sleep(10)

    async def _manage_trades(self):
        for ticker in list(self.active_trades.keys()):
            trade = self.active_trades[ticker]
            if trade.status != TradeStatus.ACTIVE:
                continue
            try:
                snapshot = await self.get_ticker_snapshot(ticker)
                if not snapshot:
                    continue
                result = trade.update_price(snapshot['price'])
                if result:
                    r_mult = trade.r_multiple()
                    logger.info(
                        f"[TRADE] {ticker} {result.value}: "
                        f"entry=${trade.entry_price:.4f} exit=${trade.exit_price:.4f} "
                        f"P&L={trade.pnl_pct:+.1f}% ({r_mult:+.2f}R) ({trade.exit_reason})"
                    )
                    await self.discord.send_trade_result(trade)
                    self.completed_trades.append(trade)
                    await self._save_completed_trade(trade)
                    del self.active_trades[ticker]
                    await self._save_active_trades()
            except Exception as e:
                logger.error(f"[TRADE] Error managing {ticker}: {e}")

    async def _force_close_all_trades(self):
        if not self.active_trades:
            return
        logger.info(f"[EOD CLOSE] Forcing close of {len(self.active_trades)} trades")
        for ticker in list(self.active_trades.keys()):
            trade = self.active_trades[ticker]
            if trade.status != TradeStatus.ACTIVE:
                continue
            try:
                snapshot = await self.get_ticker_snapshot(ticker)
                current_price = snapshot['price'] if snapshot and snapshot.get('price', 0) > 0 else 0
                trade.expire(current_price)
                await self.discord.send_trade_result(trade)
                self.completed_trades.append(trade)
                await self._save_completed_trade(trade)
            except Exception as e:
                logger.error(f"[EOD CLOSE] Error closing {ticker}: {e}")
                trade.expire(0)
                self.completed_trades.append(trade)

        self.active_trades.clear()
        try:
            await self.redis.set(f"s015:active_trades:{self._today_str()}", {}, ttl=86400)
        except Exception:
            pass

    # ====================================================================
    # LOOP 4 & 5: CLEANUP + SUMMARY
    # ====================================================================

    async def cleanup_loop(self):
        logger.info("[LOOP] cleanup_loop started")
        while True:
            try:
                now = datetime.now(timezone.utc)
                for ticker in list(self.candidates.keys()):
                    c = self.candidates[ticker]
                    if c.last_update and (now - c.last_update) > timedelta(hours=4):
                        del self.candidates[ticker]
                if len(self.completed_trades) > 500:
                    self.completed_trades = self.completed_trades[-500:]
            except Exception as e:
                logger.error(f"[LOOP] cleanup error: {e}")
            await asyncio.sleep(300)

    async def summary_scheduler_loop(self):
        logger.info("[LOOP] summary_scheduler_loop started")
        while True:
            try:
                now = self._now_et()
                current_time = now.strftime("%H:%M")

                if not await self._is_trading_day(now):
                    await asyncio.sleep(30)
                    continue

                for summary_time in SUMMARY_TIMES_ET:
                    if current_time == summary_time and self.last_summary_time != summary_time:
                        self.last_summary_time = summary_time
                        all_trades = self.completed_trades + list(self.active_trades.values())
                        await self.discord.send_daily_summary(
                            trades=all_trades,
                            candidates_count=len(self.candidates),
                            summary_time=summary_time
                        )

                if current_time == "00:00":
                    for trade in self.active_trades.values():
                        trade.expire(0)
                        self.completed_trades.append(trade)
                    self.active_trades.clear()
                    self.candidates.clear()
                    self.watch_notified.clear()
                    self.signals_notified.clear()
                    self.completed_trades.clear()
                    self.last_summary_time = ""
                    self.open_entries_done = False
                    logger.info("[RESET] Midnight reset")

            except Exception as e:
                logger.error(f"[LOOP] summary error: {e}")
            await asyncio.sleep(30)

    # ====================================================================
    # SIGNAL DISPATCH
    # ====================================================================

    async def _dispatch_signal(self, candidate: S17Candidate, signal_type: str):
        try:
            strategy_signal = StrategySignal(
                strategy="S015",
                signal_type=signal_type,
                ticker=candidate.ticker,
                bias="SHORT",
                price=candidate.current_price,
                level_entry=candidate.current_price,
                level_tp=candidate.current_price * 0.85,
                level_sl=candidate.pm_high + 0.10,
                gap_percentage=candidate.gap_pct,
                vol_premarket=candidate.pm_volume,
                note=f"S17 VWAP Premium Fade | VWAP+{candidate.vwap_premium_pct:.1f}% | {candidate.signal_strength.value}"
            )
            result = await self.signal_dispatcher.dispatch_signal(strategy_signal)
            if result.webhooks_total > 0:
                logger.info(f"[DISPATCH] {candidate.ticker} {signal_type}: {result.webhooks_success}/{result.webhooks_total}")
        except Exception as e:
            logger.error(f"Error dispatching signal for {candidate.ticker}: {e}")

    async def _dispatch_entry_signal(self, candidate: S17Candidate, trade: ShortTrade):
        try:
            risk = trade.stop_loss - trade.entry_price
            reward = trade.entry_price - trade.target_price
            rr = reward / risk if risk > 0 else 0
            strategy_signal = StrategySignal(
                strategy="S015",
                signal_type="ENTRY",
                ticker=candidate.ticker,
                bias="SHORT",
                price=trade.entry_price,
                level_entry=trade.entry_price,
                level_tp=trade.target_price,
                level_sl=trade.stop_loss,
                gap_percentage=candidate.gap_pct,
                vol_premarket=candidate.pm_volume,
                note=f"S17 VWAP Premium Fade ENTRY | SL ${trade.stop_loss:.2f} (PM_H+$0.10) | TP ${trade.target_price:.2f} (-15%) | R:R 1:{rr:.1f} | {trade.signal_strength}"
            )
            result = await self.signal_dispatcher.dispatch_signal(strategy_signal)
            if result.webhooks_total > 0:
                logger.info(f"[DISPATCH] {candidate.ticker} ENTRY: {result.webhooks_success}/{result.webhooks_total}")
        except Exception as e:
            logger.error(f"Error dispatching ENTRY for {candidate.ticker}: {e}")


async def main():
    service = STRAT015Service()
    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
