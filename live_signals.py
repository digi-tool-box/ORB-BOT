import asyncio
import os
import sys
import signal
import time
import logging
import pandas as pd
import pytz
from datetime import datetime
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv
from keep_alive import keep_alive
from telegram_notifier import send_telegram

load_dotenv()

from config import (
    SYMBOL, INTERVAL, NY_TIMEZONE, NY_OPEN_HOUR, NY_OPEN_MINUTE,
    SLIPPAGE_PCT, RISK_PER_TRADE_PCT, INITIAL_CAPITAL, LEVERAGE,
    BREAKOUT_PCT, RETEST_ZONE_PCT, RISK_REWARD, SL_BUFFER_PCT,
    BREAKEVEN_TRIGGER, TRAIL_STEP_PCT, MAKER_FEE, TAKER_FEE,
    MAX_TRADES_PER_DAY, DEBUG_MODE, QUANTITY_PRECISION, PRICE_PRECISION,
    LOG_FILE, IS_TESTNET, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
)

API_KEY = os.environ.get('API_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

if not API_KEY or not SECRET_KEY:
    from config import API_KEY as CONFIG_API_KEY, SECRET_KEY as CONFIG_SECRET_KEY
    API_KEY = API_KEY or CONFIG_API_KEY
    SECRET_KEY = SECRET_KEY or CONFIG_SECRET_KEY

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class LiveORBSignals:
    def __init__(self):
        self.client = None
        self.bm = None
        self.ny_tz = pytz.timezone(NY_TIMEZONE)
        self.today = None
        self.or_high = None
        self.or_low = None
        self.or_set = False
        self.trades_taken_today = 0
        self.breakout_done = {'BUY': False, 'SELL': False}
        self.breakout_detected = {'BUY': False, 'SELL': False}
        self.candles_today = []
        self.active_position = None
        self.sl_order_id = None
        self.tp_order_id = None
        self.pending_order_id = None
        self.pending_order_side = None
        self.pending_entry_level = None
        self.pending_stop_level = None
        self.pending_qty = None
        self._shutdown_requested = False
        self.daily_pnl = 0.0
        self.max_daily_loss_pct = 20.0

    def notify(self, message):
        send_telegram(message, token=TELEGRAM_BOT_TOKEN, chat_id=TELEGRAM_CHAT_ID)

    async def shutdown(self):
        """Graceful shutdown: close any open position and clean up."""
        logger.info("Shutdown initiated")
        print("\n🛑 SHUTDOWN initiated – protecting position...")
        sys.stdout.flush()
        self.notify("🔴 Bot SHUTTING DOWN")
        if self.active_position:
            await self.market_close_position(self.active_position['side'], reason="Bot shutdown")
        if self.pending_order_id:
            await self.cancel_order(self.pending_order_id)
            self.pending_order_id = None
        if self.client:
            try:
                await self.client.close_connection()
                print("🔌 Connection closed")
                sys.stdout.flush()
            except Exception:
                pass
        print("✅ Bot shutdown complete.")
        self.notify("✅ Bot shutdown complete")
        sys.stdout.flush()

    async def connect_with_retry(self, max_retries=5):
        """Attempt to connect to Binance Testnet with exponential backoff."""
        for attempt in range(max_retries):
            try:
                print(f"🔌 Connection attempt {attempt + 1}/{max_retries}...")
                sys.stdout.flush()
                net_name = "Testnet" if IS_TESTNET else "Mainnet"
                client = await AsyncClient.create(API_KEY, SECRET_KEY, testnet=IS_TESTNET)
                await client.ping()
                print(f"✅ Connected to Binance {net_name} successfully!")
                sys.stdout.flush()
                return client
            except Exception as e:
                print(f"⚠️ Connection attempt {attempt + 1} failed: {e}")
                sys.stdout.flush()
                if attempt < max_retries - 1:
                    wait_time = 10 * (attempt + 1)
                    print(f"🔄 Retrying in {wait_time} seconds...")
                    sys.stdout.flush()
                    await asyncio.sleep(wait_time)
                else:
                    print("❌ Failed to connect after multiple attempts. Exiting.")
                    sys.stdout.flush()
                    raise

    async def get_usdt_balance(self):
        try:
            acc = await self.client.futures_account()
            balance = float(acc['availableBalance'])
            print(f"💰 Available USDT Balance: {balance}")
            sys.stdout.flush()
            return balance
        except Exception as e:
            print(f"⚠️ Balance fetch error: {e}")
            sys.stdout.flush()
        return INITIAL_CAPITAL

    def calculate_quantity(self, entry, stop, side, balance):
        max_margin_use = balance * 0.98
        max_qty_by_margin = (max_margin_use * LEVERAGE) / entry if entry > 0 else 0
        risk_amount = balance * (RISK_PER_TRADE_PCT / 100)
        risk_per_unit = abs(entry - stop)
        if risk_per_unit <= 0:
            print("⚠️ Risk per unit is zero, cannot calculate quantity")
            sys.stdout.flush()
            return 0
        qty_by_risk = risk_amount / risk_per_unit
        qty = min(qty_by_risk, max_qty_by_margin)
        if qty < qty_by_risk:
            print(f"⚠️ Risk-based qty ({qty_by_risk:.3f}) exceeds margin limit ({max_qty_by_margin:.3f}). Capping to {qty:.3f}")
            sys.stdout.flush()
        print(f"📊 Qty Calc: Balance={balance:.2f}, Leverage={LEVERAGE}x, "
              f"MarginUse={max_margin_use:.2f} (98%), "
              f"Risk={risk_amount:.2f}, Risk/Unit={risk_per_unit:.2f}, Qty={qty:.3f}")
        sys.stdout.flush()
        return round(qty, QUANTITY_PRECISION)

    def validate_stop_distance(self, side, entry_price, stop_price, current_price=None):
        """
        Ensure stop price is at least 0.1% away from current price to avoid
        'Order would immediately trigger' error.
        """
        min_distance_pct = 0.001  # 0.1%
        if current_price is None:
            current_price = entry_price
        if side == 'BUY':
            required_distance = entry_price * min_distance_pct
            if abs(entry_price - stop_price) < required_distance:
                new_stop = entry_price - required_distance
                print(f"⚠️ Stop too close to entry. Adjusting {stop_price:.2f} → {new_stop:.2f}")
                sys.stdout.flush()
                return new_stop
        else:  # SELL
            required_distance = entry_price * min_distance_pct
            if abs(stop_price - entry_price) < required_distance:
                new_stop = entry_price + required_distance
                print(f"⚠️ Stop too close to entry. Adjusting {stop_price:.2f} → {new_stop:.2f}")
                sys.stdout.flush()
                return new_stop
        return stop_price

    async def place_limit_entry_order(self, side, price, max_retries=5):
        for attempt in range(max_retries):
            try:
                balance = await self.get_usdt_balance()
                stop_level = self.pending_stop_level
                qty = self.calculate_quantity(price, stop_level, side, balance)
                if qty <= 0:
                    print("⚠️ Invalid quantity, cannot place LIMIT order")
                    sys.stdout.flush()
                    return None
                self.pending_qty = qty
                print(f"🚀 Placing {side} LIMIT entry for {qty} {SYMBOL} at {price:.2f} (GTC)...")
                sys.stdout.flush()
                order = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=side,
                    type='LIMIT',
                    price=round(price, PRICE_PRECISION),
                    quantity=qty,
                    timeInForce='GTC'
                )
                order_id = order.get('orderId')
                if order_id is not None:
                    self.pending_order_id = order_id
                    self.pending_order_side = side
                    self.pending_entry_level = price
                    print(f"✅ {side} LIMIT entry placed! OrderID: {order_id}")
                    sys.stdout.flush()
                    return order
                print("⚠️ LIMIT order placed but missing orderId in response")
                sys.stdout.flush()
                return None
            except Exception as e:
                error_str = str(e)
                if "502 Bad Gateway" in error_str or "504" in error_str or "502" in error_str or "Bad Gateway" in error_str or "Gateway Timeout" in error_str or "CloudFront" in error_str or "Invalid JSON" in error_str:
                    print(f"⚠️ LIMIT entry attempt {attempt + 1}/{max_retries} failed (server error): {e}")
                    sys.stdout.flush()
                    if attempt < max_retries - 1:
                        wait = 5 * (attempt + 1)
                        print(f"🔄 Retrying in {wait}s...")
                        sys.stdout.flush()
                        await asyncio.sleep(wait)
                    else:
                        print(f"❌ LIMIT entry failed after {max_retries} attempts")
                        sys.stdout.flush()
                        self.notify(f"❌ LIMIT entry failed | {side} {SYMBOL} @ {price:.2f}\n{error_str[:200]}")
                        return None
                else:
                    print(f"❌ LIMIT entry error: {e}")
                    sys.stdout.flush()
                    self.notify(f"❌ LIMIT entry failed | {side} {SYMBOL} @ {price:.2f}\n{str(e)[:200]}")
                    return None

    async def place_market_entry_order(self, side, quantity):
        try:
            print(f"🚀 Placing {side} MARKET entry for {quantity} {SYMBOL}...")
            sys.stdout.flush()
            order = await self.client.futures_create_order(
                symbol=SYMBOL,
                side=side,
                type='MARKET',
                quantity=quantity,
            )
            order_id = order.get('orderId')
            if order_id is not None:
                fill_price = float(order.get('avgPrice', 0))
                print(f"✅ {side} MARKET entry filled! OrderID: {order_id}, Fill: {fill_price:.2f}")
                sys.stdout.flush()
                return order
            print("⚠️ MARKET order placed but missing orderId in response")
            sys.stdout.flush()
            return None
        except Exception as e:
            print(f"❌ MARKET entry error: {e}")
            sys.stdout.flush()
            return None

    async def _cancel_all_close_orders(self, close_side):
        """Cancel ALL orders on the close side to avoid -4130 conflicts."""
        try:
            open_orders = await self.client.futures_get_open_orders(symbol=SYMBOL)
            for order in open_orders:
                o_side = order['side']
                o_type = order['type']
                if o_side == close_side and o_type in ('STOP_MARKET', 'TAKE_PROFIT_MARKET', 'STOP', 'TAKE_PROFIT'):
                    try:
                        await self.client.futures_cancel_order(symbol=SYMBOL, orderId=order['orderId'])
                        print(f"🗑️ Cancelled {o_type} order {order['orderId']}")
                        logger.info(f"Cancelled {o_type} order {order['orderId']}")
                    except Exception as cancel_err:
                        if "Unknown order sent" not in str(cancel_err):
                            print(f"⚠️ Could not cancel order {order['orderId']}: {cancel_err}")
        except Exception as e:
            print(f"⚠️ Error cancelling existing orders: {e}")
        await asyncio.sleep(1)

    async def _find_existing_exit_order(self, close_side, order_type, stop_price=None):
        """Find existing exit order on Binance. Returns (orderId, stopPrice) or (None, None)."""
        try:
            open_orders = await self.client.futures_get_open_orders(symbol=SYMBOL)
            for o in open_orders:
                if o['side'] == close_side and o['type'] == order_type:
                    if stop_price is None or abs(float(o.get('stopPrice', 0)) - stop_price) < 0.01:
                        return o['orderId'], float(o.get('stopPrice', 0))
        except Exception:
            pass
        return None, None

    async def _get_position_qty(self):
        try:
            pos_info = await self.client.futures_position_information(symbol=SYMBOL)
            for p in pos_info:
                amt = float(p['positionAmt'])
                if amt != 0:
                    return abs(amt)
        except Exception:
            pass
        return 0.0

    async def place_exit_orders(self, side, stop_price, tp_price, quantity, retries=5):
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        sl_success = False
        tp_success = False
        self.sl_order_id = None
        self.tp_order_id = None

        live_qty = await self._get_position_qty()
        if live_qty > 0:
            quantity = live_qty

        # Cancel ALL close-side orders to prevent -4130 conflict
        await self._cancel_all_close_orders(close_side)

        # Place SL with quantity (NOT closePosition — avoids -4130 conflict)
        for attempt in range(retries):
            try:
                print(f"🛑 Placing SL {close_side} STOP_MARKET at {stop_price:.2f} for {quantity} {SYMBOL} (attempt {attempt+1})...")
                sys.stdout.flush()
                sl = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='STOP_MARKET',
                    stopPrice=round(stop_price, PRICE_PRECISION),
                    quantity=quantity,
                    newOrderRespType='RESULT',
                )
                order_id = sl.get('orderId')
                if order_id is not None:
                    self.sl_order_id = order_id
                    print(f"✅ SL placed successfully! ID: {order_id}")
                    sys.stdout.flush()
                    sl_success = True
                    break
                else:
                    print(f"⚠️ SL response missing orderId, checking open orders...")
                    sys.stdout.flush()
                    await asyncio.sleep(1)
                    found_id, _ = await self._find_existing_exit_order(close_side, 'STOP_MARKET', stop_price)
                    if found_id is not None:
                        self.sl_order_id = found_id
                        print(f"✅ SL confirmed via open orders! ID: {self.sl_order_id}")
                        sys.stdout.flush()
                        sl_success = True
                        break
                    await self._cancel_all_close_orders(close_side)
            except Exception as e:
                error_str = str(e)
                if "-4130" in error_str:
                    print(f"❌ SL attempt {attempt+1} failed (-4130). Cancelling conflicting orders and retrying...")
                    sys.stdout.flush()
                    await self._cancel_all_close_orders(close_side)
                else:
                    print(f"❌ SL attempt {attempt+1} failed: {e}")
                    sys.stdout.flush()
                if not sl_success and attempt < retries - 1:
                    await asyncio.sleep(2)

        # Check current mark price before placing TP
        try:
            ticker = await self.client.futures_symbol_ticker(symbol=SYMBOL)
            mark_price = float(ticker['price'])
            print(f"📊 Current mark price for TP check: {mark_price:.2f}")
            sys.stdout.flush()

            tp_would_trigger = False
            if close_side == 'BUY' and mark_price <= tp_price:
                tp_would_trigger = True
            elif close_side == 'SELL' and mark_price >= tp_price:
                tp_would_trigger = True

            if tp_would_trigger:
                print(f"⚠️ TP {tp_price:.2f} would trigger immediately (mark {mark_price:.2f}). Using MARKET close.")
                sys.stdout.flush()
                if sl_success and self.sl_order_id:
                    await self.cancel_order(self.sl_order_id)
                    self.sl_order_id = None
                await self.market_close_position(side, reason="TP would trigger immediately")
                return False, False
        except Exception as e:
            print(f"⚠️ Could not fetch mark price: {e}")
            sys.stdout.flush()

        # Place TP with quantity (NOT closePosition — avoids -4130 conflict)
        for attempt in range(retries):
            try:
                print(f"🎯 Placing TP {close_side} TAKE_PROFIT_MARKET at {tp_price:.2f} for {quantity} {SYMBOL} (attempt {attempt+1})...")
                sys.stdout.flush()
                tp = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='TAKE_PROFIT_MARKET',
                    stopPrice=round(tp_price, PRICE_PRECISION),
                    quantity=quantity,
                    newOrderRespType='RESULT',
                )
                order_id = tp.get('orderId')
                if order_id is not None:
                    self.tp_order_id = order_id
                    print(f"✅ TP placed successfully! ID: {order_id}")
                    sys.stdout.flush()
                    tp_success = True
                    break
                else:
                    print(f"⚠️ TP response missing orderId, retrying...")
                    sys.stdout.flush()
            except Exception as e:
                print(f"❌ TP attempt {attempt+1} failed: {e}")
                sys.stdout.flush()
                if attempt < retries - 1:
                    await asyncio.sleep(2)

        # If both SL and TP placed, return success
        if sl_success and tp_success:
            logger.info(f"SL/TP placed for {side} @ entry: {stop_price=}, {tp_price=}")
            return sl_success, tp_success

        # Emergency MARKET exit if either SL or TP failed
        if not sl_success or not tp_success:
            msg = f"🚨 SL/TP failed | {side} {SYMBOL}\nSL={'✅' if sl_success else '❌'} TP={'✅' if tp_success else '❌'}"
            print(msg)
            sys.stdout.flush()
            self.notify(msg)
            try:
                pos_info = await self.client.futures_position_information(symbol=SYMBOL)
                pos_open = any(float(p['positionAmt']) != 0 for p in pos_info)
                if pos_open:
                    await self.market_close_position(side, reason="SL/TP placement incomplete")
                else:
                    print("ℹ️ Position already closed.")
                    sys.stdout.flush()
            except Exception as e:
                err_msg = f"🚨 Emergency exit failed: {e}"
                print(err_msg)
                logger.error(err_msg)
                sys.stdout.flush()
                self.notify(err_msg)
            return False, False

        return sl_success, tp_success

    async def market_close_position(self, side, reason="emergency"):
        """Close position using quantity (fetched from Binance)."""
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        try:
            qty = await self._get_position_qty()
            if qty <= 0:
                print(f"ℹ️ No position to close (reason: {reason})")
                sys.stdout.flush()
                self.active_position = None
                return True
            logger.info(f"Market close {qty} {side} position (reason: {reason})")
            await self.client.futures_create_order(
                symbol=SYMBOL,
                side=close_side,
                type='MARKET',
                quantity=qty,
                newOrderRespType='RESULT',
            )
            print(f"✅ Position {qty} {side} closed via MARKET (reason: {reason})")
            sys.stdout.flush()
            self.active_position = None
            return True
        except Exception as e:
            print(f"❌ Market close failed (reason: {reason}): {e}")
            logger.error(f"Market close failed: {e}")
            sys.stdout.flush()
            self.notify(f"❌ Market close failed | {reason}\n{str(e)[:200]}")
            return False

    async def cancel_order(self, order_id):
        if not order_id:
            return
        try:
            print(f"🗑️ Cancelling order {order_id}...")
            sys.stdout.flush()
            await self.client.futures_cancel_order(symbol=SYMBOL, orderId=order_id)
            print(f"✅ Order {order_id} cancelled successfully")
            sys.stdout.flush()
        except Exception as e:
            if "Unknown order sent" not in str(e):
                print(f"⚠️ Cancel order error (may be filled): {e}")
                sys.stdout.flush()

    async def check_pending_limit_fill(self):
        if not self.pending_order_id:
            return False
        now = time.time()
        if now - getattr(self, '_last_fill_check', 0) < 10:
            return False
        self._last_fill_check = now
        try:
            order = await self.client.futures_get_order(symbol=SYMBOL, orderId=self.pending_order_id)
            status = order['status']
            if status == 'FILLED':
                fill_price = float(order.get('avgPrice', 0))
                if fill_price == 0 and self.pending_entry_level is not None:
                    fill_price = self.pending_entry_level
                print(f"\n{'='*50}")
                print(f"✅ LIMIT order {self.pending_order_id} FILLED @ {fill_price:.2f}!")
                sys.stdout.flush()
                side = self.pending_order_side
                stop_level = self.pending_stop_level
                qty = self.pending_qty
                if side == 'BUY':
                    stop = stop_level * (1 - SL_BUFFER_PCT/100)
                    risk = fill_price - stop
                    target = fill_price + risk * RISK_REWARD
                else:
                    stop = stop_level * (1 + SL_BUFFER_PCT/100)
                    risk = stop - fill_price
                    target = fill_price - risk * RISK_REWARD
                stop = self.validate_stop_distance(side, fill_price, stop)
                print(f"📋 Trade Details:")
                print(f"   Side: {side}")
                print(f"   Entry (filled): {fill_price:.2f}")
                print(f"   Stop Loss: {stop:.2f} (Risk: {risk:.2f})")
                print(f"   Take Profit: {target:.2f} (RR: 1:{RISK_REWARD})")
                sys.stdout.flush()
                await asyncio.sleep(1.5)
                live_qty = await self._get_position_qty()
                if live_qty <= 0:
                    live_qty = qty
                sl_placed, tp_placed = await self.place_exit_orders(side, stop, target, live_qty)
                if not sl_placed and not tp_placed:
                    print("🚨 CRITICAL: Both SL and TP failed! Checking if position still open...")
                    sys.stdout.flush()
                    self.notify(f"🚨 Both SL/TP failed after fill | {side} {SYMBOL}")
                    try:
                        pos_info = await self.client.futures_position_information(symbol=SYMBOL)
                        pos_open = any(float(p['positionAmt']) != 0 for p in pos_info)
                        if pos_open:
                            await self.market_close_position(side, reason="Both SL/TP failed at fill")
                        else:
                            print("ℹ️ Position already closed.")
                            sys.stdout.flush()
                    except Exception as e:
                        print(f"🚨 Emergency exit check failed: {e}")
                        logger.error(f"Emergency exit failed after fill: {e}")
                    self.pending_order_id = None
                    return False
                try:
                    acc = await self.client.futures_account()
                    entry_wallet = float(acc['totalWalletBalance'])
                except Exception:
                    entry_wallet = None
                self.active_position = {
                    'side': side,
                    'entry': fill_price,
                    'sl': stop,
                    'tp': target,
                    'breakeven_triggered': False,
                    'entry_wallet': entry_wallet
                }
                order_id = self.pending_order_id
                self.pending_order_id = None
                self.trades_taken_today += 1
                print(f"\n✅ {side} POSITION ACTIVE @ {fill_price:.2f} - Monitoring...")
                print(f"{'='*50}\n")
                sys.stdout.flush()
                self.notify(f"📈 TRADE ENTERED | {side} {qty} {SYMBOL}\nEntry: ${fill_price:.2f}\nSL: ${stop:.2f} | TP: ${target:.2f}\nRisk: ${risk:.2f} | RR: 1:{RISK_REWARD}")
                return True
            elif status in ('CANCELED', 'EXPIRED', 'REJECTED'):
                print(f"❌ Pending LIMIT order {self.pending_order_id} {status}")
                self.notify(f"❌ LIMIT order {status} | {self.pending_order_side} {SYMBOL}")
                self.pending_order_id = None
                return False
            return False
        except Exception as e:
            print(f"⚠️ Error checking pending limit fill: {e}")
            sys.stdout.flush()
            return False

    async def update_trailing_stop(self, candle_high, candle_low, candle_close):
        if not self.active_position:
            return

        pos = self.active_position
        side = pos['side']
        breakeven_triggered = pos.get('breakeven_triggered', False)
        current_sl = pos['sl']

        breakeven_trigger_pct = BREAKEVEN_TRIGGER
        trailing_pct = TRAIL_STEP_PCT

        new_sl = current_sl
        update_needed = False

        if side not in ('highest_high', 'lowest_low'):
            pos['highest_high'] = pos.get('highest_high', pos['entry'])
            pos['lowest_low'] = pos.get('lowest_low', pos['entry'])

        if side == 'BUY':
            if candle_high > pos['highest_high']:
                pos['highest_high'] = candle_high
            profit_pct = (pos['highest_high'] - pos['entry']) / pos['entry']
            if profit_pct >= breakeven_trigger_pct and not breakeven_triggered:
                new_sl = pos['entry']
                breakeven_triggered = True
                update_needed = True
                print(f"🟢 Breakeven triggered! Moving SL to entry: {new_sl:.2f}")
                sys.stdout.flush()
            if breakeven_triggered:
                new_trail_sl = pos['highest_high'] * (1 - trailing_pct)
                if new_trail_sl > new_sl:
                    new_sl = new_trail_sl
                    update_needed = True
                    print(f"🔄 Trailing SL up to {new_sl:.2f} (highest: {pos['highest_high']:.2f})")
                    sys.stdout.flush()
        else:
            if candle_low < pos['lowest_low']:
                pos['lowest_low'] = candle_low
            profit_pct = (pos['entry'] - pos['lowest_low']) / pos['entry']
            if profit_pct >= breakeven_trigger_pct and not breakeven_triggered:
                new_sl = pos['entry']
                breakeven_triggered = True
                update_needed = True
                print(f"🟢 Breakeven triggered! Moving SL to entry: {new_sl:.2f}")
                sys.stdout.flush()
            if breakeven_triggered:
                new_trail_sl = pos['lowest_low'] * (1 + trailing_pct)
                if new_trail_sl < new_sl:
                    new_sl = new_trail_sl
                    update_needed = True
                    print(f"🔄 Trailing SL down to {new_sl:.2f} (lowest: {pos['lowest_low']:.2f})")
                    sys.stdout.flush()

        if update_needed:
            print(f"🔄 Updating SL: {current_sl:.2f} → {new_sl:.2f}")
            sys.stdout.flush()

            if self.sl_order_id:
                await self.cancel_order(self.sl_order_id)

            try:
                pos_info = await self.client.futures_position_information(symbol=SYMBOL)
                current_qty = 0.0
                for p in pos_info:
                    amt = float(p['positionAmt'])
                    if amt != 0:
                        current_qty = abs(amt)
                        break
                if current_qty <= 0:
                    print("⚠️ No position size found, cannot update breakeven SL.")
                    return
            except Exception as e:
                print(f"❌ Failed to get position size: {e}")
                return

            close_side = 'SELL' if side == 'BUY' else 'BUY'
            try:
                new_sl_order = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='STOP_MARKET',
                    stopPrice=round(new_sl, PRICE_PRECISION),
                    quantity=current_qty,
                    newOrderRespType='RESULT',
                )
                order_id = new_sl_order.get('orderId')
                if order_id is not None:
                    self.sl_order_id = order_id
                else:
                    print(f"⚠️ Breakeven SL response missing orderId")
                    sys.stdout.flush()
                print(f"✅ New SL at entry: {new_sl:.2f}")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ Breakeven SL order error: {e}")
                sys.stdout.flush()

        self.active_position['breakeven_triggered'] = breakeven_triggered
        self.active_position['sl'] = new_sl
        self.active_position['highest_high'] = pos.get('highest_high', pos['entry'])
        self.active_position['lowest_low'] = pos.get('lowest_low', pos['entry'])

    async def check_position_status(self):
        if not self.active_position:
            return False

        try:
            pos_info = await self.client.futures_position_information(symbol=SYMBOL)
            position_exists = False
            current_qty = 0.0
            for p in pos_info:
                amt = float(p['positionAmt'])
                if amt != 0:
                    position_exists = True
                    current_qty = abs(amt)
                    unrealized_pnl = float(p['unRealizedProfit'])
                    if DEBUG_MODE:
                        print(f"📊 Position open: {amt} {SYMBOL}, Unrealized PnL: {unrealized_pnl:.2f} USDT")
                        sys.stdout.flush()
                    break

            if not position_exists:
                print("📴 Position closed! (SL/TP hit)")
                sys.stdout.flush()
                if self.active_position:
                    entry_wallet = self.active_position.get('entry_wallet')
                    if entry_wallet is not None:
                        try:
                            acc = await self.client.futures_account()
                            current_wallet = float(acc['totalWalletBalance'])
                            exit_pnl = round(current_wallet - entry_wallet, 2)
                        except Exception:
                            exit_pnl = 0.0
                    else:
                        exit_pnl = 0.0
                    side = self.active_position['side']
                    icon = "🟢" if exit_pnl >= 0 else "🔴"
                    self.notify(f"{icon} TRADE CLOSED | {side} {SYMBOL}\nPnL: ${exit_pnl:.2f}")
                self.active_position = None
                if self.sl_order_id:
                    await self.cancel_order(self.sl_order_id)
                    self.sl_order_id = None
                if self.tp_order_id:
                    await self.cancel_order(self.tp_order_id)
                    self.tp_order_id = None
                return False

            # SAFETY: If no SL order exists for this position, close immediately
            if self.sl_order_id is None and current_qty > 0:
                side = self.active_position['side']
                print("🚨 CRITICAL: Active position has no Stop Loss order! Forcing market close.")
                sys.stdout.flush()
                self.notify(f"🚨 No SL order | {side} {SYMBOL}\nClosing position immediately")
                await self.market_close_position(side, reason="No SL order")
                return False

            return True

        except Exception as e:
            print(f"⚠️ Position check error: {e}")
            sys.stdout.flush()

        return False

    async def process_closed_candle(self, kline, stream=None):
        try:
            candle_open_ts = kline['t']
            utc_time = datetime.fromtimestamp(candle_open_ts / 1000, tz=pytz.utc)
            ny_time = utc_time.astimezone(self.ny_tz)
            ny_date = ny_time.date()
            ny_hour = ny_time.hour
            ny_minute = ny_time.minute

            close_price = float(kline['c'])
            high_price = float(kline['h'])
            low_price = float(kline['l'])
            print(f"🕯️ Candle Closed: {ny_time.strftime('%Y-%m-%d %H:%M')} NY | "
                  f"H:{high_price:.2f} L:{low_price:.2f} C:{close_price:.2f} | "
                  f"OR:{'SET' if self.or_set else 'WAITING'}")
            sys.stdout.flush()

            if self.today != ny_date:
                print(f"\n{'='*50}")
                print(f"🆕 New Trading Day: {ny_date}")
                print(f"{'='*50}")
                sys.stdout.flush()
                self.today = ny_date
                self.or_set = False
                self.or_high = None
                self.or_low = None
                self.trades_taken_today = 0
                self.daily_pnl = 0.0
                self.breakout_done = {'BUY': False, 'SELL': False}
                self.breakout_detected = {'BUY': False, 'SELL': False}
                self.candles_today = []
                if self.pending_order_id:
                    await self.cancel_order(self.pending_order_id)
                    self.pending_order_id = None

            # No trading after 4:00 PM NY (market close)
            if ny_hour >= 16:
                if self.pending_order_id:
                    await self.cancel_order(self.pending_order_id)
                    self.pending_order_id = None
                    print("🗑️ Pending LIMIT order cancelled at EOD")
                    sys.stdout.flush()
                if self.active_position:
                    print("🕟 End of NY session – closing any open position.")
                    sys.stdout.flush()
                    await self.market_close_position(self.active_position['side'], reason="End of NY session")
                return

            if self.active_position:
                await self.update_trailing_stop(high_price, low_price, close_price)
                await self.check_position_status()
                return

            if not self.or_set:
                if ny_hour == NY_OPEN_HOUR and ny_minute == NY_OPEN_MINUTE:
                    self.or_high = high_price
                    self.or_low = low_price
                    self.or_set = True
                    print(f"🎯 Opening Range Set!")
                    print(f"   📈 OR High: {self.or_high:.2f}")
                    print(f"   📉 OR Low:  {self.or_low:.2f}")
                    sys.stdout.flush()
                else:
                    print(f"   ⏳ Waiting for 09:30 NY candle... (current: {ny_hour:02d}:{ny_minute:02d})")
                    sys.stdout.flush()
                return

            # Check if pending LIMIT order got filled (retest confirmation)
            if self.pending_order_id:
                filled = await self.check_pending_limit_fill()
                if filled:
                    return

            self.candles_today.append({
                'timestamp': candle_open_ts,
                'ny_time': ny_time,
                'open': float(kline['o']),
                'high': float(kline['h']),
                'low': float(kline['l']),
                'close': float(kline['c'])
            })

            if self.trades_taken_today >= MAX_TRADES_PER_DAY:
                return

            max_daily_loss = INITIAL_CAPITAL * (self.max_daily_loss_pct / 100)
            if self.daily_pnl <= -max_daily_loss:
                print(f"🚫 Max daily loss ({self.max_daily_loss_pct}%) reached. Stopping trading for the day.")
                sys.stdout.flush()
                return

            last = self.candles_today[-1]
            close = last['close']
            high = last['high']
            low = last['low']
            candle_range_pct = ((high - low) / low) * 100

            # BUY BREAKOUT → place LIMIT order within retest zone above OR High
            if close > self.or_high and not self.breakout_done['BUY'] and not self.pending_order_id:
                if candle_range_pct >= BREAKOUT_PCT:
                    print(f"📈 Breakout BUY Detected! Candle closed above OR High ({self.or_high:.2f}). Placing LIMIT BUY within retest zone...")
                    sys.stdout.flush()
                    self.pending_stop_level = self.or_low
                    buy_limit_price = self.or_high * (1 + RETEST_ZONE_PCT/100)
                    await self.place_limit_entry_order('BUY', buy_limit_price)
                    if self.pending_order_id:
                        self.breakout_done['BUY'] = True
                        print(f"⏳ LIMIT BUY at {buy_limit_price:.2f} online — will fill when price retests OR High (zone ±{RETEST_ZONE_PCT}%)")
                    else:
                        print(f"⚠️ LIMIT BUY failed, will retry on next candle")
                    return

            # SELL BREAKOUT → place LIMIT order within retest zone below OR Low
            if close < self.or_low and not self.breakout_done['SELL'] and not self.pending_order_id:
                if candle_range_pct >= BREAKOUT_PCT:
                    print(f"📉 Breakout SELL Detected! Candle closed below OR Low ({self.or_low:.2f}). Placing LIMIT SELL within retest zone...")
                    sys.stdout.flush()
                    self.pending_stop_level = self.or_high
                    sell_limit_price = self.or_low * (1 - RETEST_ZONE_PCT/100)
                    await self.place_limit_entry_order('SELL', sell_limit_price)
                    if self.pending_order_id:
                        self.breakout_done['SELL'] = True
                        print(f"⏳ LIMIT SELL at {sell_limit_price:.2f} online — will fill when price retests OR Low (zone ±{RETEST_ZONE_PCT}%)")
                    else:
                        print(f"⚠️ LIMIT SELL failed, will retry on next candle")
                    return

        except Exception as e:
            print(f"❌ Error processing candle: {e}")
            sys.stdout.flush()
            self.notify(f"❌ Candle processing error\n{str(e)[:200]}")

    async def recover_opening_range(self):
        try:
            print("🔍 Attempting to recover Opening Range (OR) from historical candles...")
            sys.stdout.flush()

            klines = await self.client.futures_klines(symbol=SYMBOL, interval=INTERVAL, limit=100)

            now_ny = datetime.now(self.ny_tz)
            now_utc_ts_ms = datetime.now(pytz.utc).timestamp() * 1000
            today_date = now_ny.date()
            self.today = today_date

            today_candles = []
            or_candle = None

            for k in klines:
                open_time_ms = k[0]
                close_time_ms = k[6]
                utc_time = datetime.fromtimestamp(open_time_ms / 1000, tz=pytz.utc)
                ny_time = utc_time.astimezone(self.ny_tz)

                if ny_time.date() == today_date:
                    candle_data = {
                        'timestamp': k[0],
                        'ny_time': ny_time,
                        'open': float(k[1]),
                        'high': float(k[2]),
                        'low': float(k[3]),
                        'close': float(k[4])
                    }

                    if now_utc_ts_ms > close_time_ms:
                        today_candles.append(candle_data)
                        if ny_time.hour == NY_OPEN_HOUR and ny_time.minute == NY_OPEN_MINUTE:
                            or_candle = candle_data

            if or_candle:
                self.or_high = or_candle['high']
                self.or_low = or_candle['low']
                self.or_set = True
                print(f"🎯 Recovered OR from history:")
                print(f"   📈 OR High: {self.or_high:.2f}")
                print(f"   📉 OR Low:  {self.or_low:.2f}")
                sys.stdout.flush()

                or_idx = today_candles.index(or_candle)
                self.candles_today = today_candles[or_idx + 1:]
                print(f"📚 Loaded {len(self.candles_today)} post-OR candles from history")
                sys.stdout.flush()

                # Track highest breakout state for placing LIMIT order if retest hasn't happened yet
                breakout_candle_time = None
                for candle in self.candles_today:
                    close = candle['close']
                    high = candle['high']
                    low = candle['low']
                    candle_range_pct = ((high - low) / low) * 100

                    if close > self.or_high and not self.breakout_detected['BUY']:
                        if candle_range_pct >= BREAKOUT_PCT:
                            self.breakout_detected['BUY'] = True
                            breakout_candle_time = candle['ny_time']
                            print(f"📈 [Recovery] Detected BUY Breakout on candle at {candle['ny_time'].strftime('%H:%M')}")
                            sys.stdout.flush()

                    if close < self.or_low and not self.breakout_detected['SELL']:
                        if candle_range_pct >= BREAKOUT_PCT:
                            self.breakout_detected['SELL'] = True
                            breakout_candle_time = candle['ny_time']
                            print(f"📉 [Recovery] Detected SELL Breakout on candle at {candle['ny_time'].strftime('%H:%M')}")
                            sys.stdout.flush()

                # If a breakout was detected in history, check if retest already happened
                if self.breakout_detected['BUY'] and not self.breakout_done['BUY']:
                    retest_upper = self.or_high * (1 + RETEST_ZONE_PCT/100)
                    retest_lower = self.or_high * (1 - RETEST_ZONE_PCT/100)
                    retest_found = False
                    for candle in self.candles_today:
                        if candle['ny_time'] > breakout_candle_time:
                            if candle['low'] <= retest_upper and candle['high'] >= retest_lower:
                                retest_found = True
                                break
                    if retest_found:
                        print(f"ℹ️ [Recovery] BUY retest already happened. Marking breakout_done (missed trade).")
                        sys.stdout.flush()
                        self.breakout_done['BUY'] = True
                    else:
                        print(f"ℹ️ [Recovery] BUY breakout pending retest. Will place LIMIT BUY if no active position.")
                        sys.stdout.flush()

                if self.breakout_detected['SELL'] and not self.breakout_done['SELL']:
                    retest_upper = self.or_low * (1 + RETEST_ZONE_PCT/100)
                    retest_lower = self.or_low * (1 - RETEST_ZONE_PCT/100)
                    retest_found = False
                    for candle in self.candles_today:
                        if candle['ny_time'] > breakout_candle_time:
                            if candle['high'] >= retest_lower and candle['low'] <= retest_upper:
                                retest_found = True
                                break
                    if retest_found:
                        print(f"ℹ️ [Recovery] SELL retest already happened. Marking breakout_done (missed trade).")
                        sys.stdout.flush()
                        self.breakout_done['SELL'] = True
                    else:
                        print(f"ℹ️ [Recovery] SELL breakout pending retest. Will place LIMIT SELL if no active position.")
                        sys.stdout.flush()
            else:
                print("ℹ️ Opening Range candle not found in history (market not open yet or older than 100 candles)")
                sys.stdout.flush()

        except Exception as e:
            print(f"⚠️ Error recovering Opening Range: {e}")
            import traceback
            traceback.print_exc()
            sys.stdout.flush()

    async def recover_active_position(self):
        try:
            print("🔍 Checking for open positions on Binance Futures...")
            sys.stdout.flush()
            pos_info = await self.client.futures_position_information(symbol=SYMBOL)

            active_amt = 0.0
            entry_price = 0.0

            for p in pos_info:
                amt = float(p['positionAmt'])
                if amt != 0:
                    active_amt = amt
                    entry_price = float(p['entryPrice'])
                    break

            if active_amt != 0.0:
                side = 'BUY' if active_amt > 0 else 'SELL'
                print(f"📦 Active Position found: {side} {abs(active_amt)} {SYMBOL} @ {entry_price:.2f}")
                sys.stdout.flush()

                open_orders = await self.client.futures_get_open_orders(symbol=SYMBOL)
                sl_price = None
                tp_price = None
                sl_order_id = None
                tp_order_id = None

                for o in open_orders:
                    o_type = o['type']
                    o_side = o['side']
                    expected_exit_side = 'SELL' if side == 'BUY' else 'BUY'

                    if o_side == expected_exit_side:
                        if o_type == 'STOP_MARKET':
                            sl_order_id = o['orderId']
                            sl_price = float(o['stopPrice'])
                        elif o_type == 'TAKE_PROFIT_MARKET':
                            tp_order_id = o['orderId']
                            tp_price = float(o['stopPrice'])

                print(f"   🛑 Sync SL: OrderID={sl_order_id}, Price={sl_price if sl_price else 'N/A'}")
                print(f"   🎯 Sync TP: OrderID={tp_order_id}, Price={tp_price if tp_price else 'N/A'}")
                sys.stdout.flush()

                # If missing SL or TP, close the position immediately
                if sl_price is None or tp_price is None:
                    print("🚨 Recovered position has incomplete SL/TP! Closing immediately to prevent loss.")
                    sys.stdout.flush()
                    self.notify(f"🚨 Recovered {side} position with incomplete SL/TP\nClosing to prevent loss")
                    await self.market_close_position(side, reason="Recovered with incomplete SL/TP")
                    return

                # Only if both orders exist, restore active_position
                self.sl_order_id = sl_order_id
                self.tp_order_id = tp_order_id
                self.active_position = {
                    'side': side,
                    'entry': entry_price,
                    'sl': sl_price,
                    'tp': tp_price,
                    'highest_high': entry_price,
                    'lowest_low': entry_price,
                    'breakeven_triggered': False
                }
            else:
                print("ℹ️ No active positions found")
                sys.stdout.flush()

        except Exception as e:
            print(f"⚠️ Error recovering active position: {e}")
            sys.stdout.flush()

    async def recover_pending_orders(self):
        try:
            print("🔍 Checking for pending LIMIT orders...")
            sys.stdout.flush()
            open_orders = await self.client.futures_get_open_orders(symbol=SYMBOL)
            for order in open_orders:
                if order['type'] == 'LIMIT' and order['status'] == 'NEW':
                    self.pending_order_id = order['orderId']
                    self.pending_order_side = order['side']
                    self.pending_entry_level = float(order['price'])
                    self.pending_qty = float(order['origQty'])
                    if not self.or_set or self.or_high is None or self.or_low is None:
                        print(f"⚠️ OR not set yet. Cannot recover stop level for LIMIT order. Cancelling order {self.pending_order_id}.")
                        sys.stdout.flush()
                        await self.cancel_order(self.pending_order_id)
                        self.pending_order_id = None
                        return
                    if self.pending_order_side == 'BUY':
                        self.pending_stop_level = self.or_low
                        side_print = 'BUY'
                    else:
                        self.pending_stop_level = self.or_high
                        side_print = 'SELL'
                    print(f"📌 Recovered pending LIMIT {side_print} order {self.pending_order_id} @ {self.pending_entry_level:.2f}")
                    sys.stdout.flush()
                    return
            print("ℹ️ No pending LIMIT orders found")
            sys.stdout.flush()
        except Exception as e:
            print(f"⚠️ Error recovering pending orders: {e}")
            sys.stdout.flush()

    async def recover_trade_count(self):
        # Trade count recovery is disabled to avoid overcounting emergency exits.
        # The in-memory counter will be reset on each new day and increments correctly.
        # Any open position is handled by recover_active_position().
        print("🔍 Trade count recovery disabled – resetting to 0.")
        sys.stdout.flush()
        self.trades_taken_today = 0

    def validate_config(self):
        errors = []
        if BREAKOUT_PCT <= 0:
            errors.append("BREAKOUT_PCT must be > 0")
        if RETEST_ZONE_PCT < 0:
            errors.append("RETEST_ZONE_PCT must be >= 0")
        if RISK_REWARD <= 0:
            errors.append("RISK_REWARD must be > 0")
        if LEVERAGE < 1 or LEVERAGE > 125:
            errors.append("LEVERAGE must be between 1-125")
        if RISK_PER_TRADE_PCT <= 0 or RISK_PER_TRADE_PCT > 100:
            errors.append("RISK_PER_TRADE_PCT must be between 0-100")
        if INITIAL_CAPITAL <= 0:
            errors.append("INITIAL_CAPITAL must be > 0")
        if MAX_TRADES_PER_DAY < 1:
            errors.append("MAX_TRADES_PER_DAY must be >= 1")
        if BREAKEVEN_TRIGGER <= 0:
            errors.append("BREAKEVEN_TRIGGER must be > 0")
        return errors

    async def start(self):
        try:
            print("\n" + "="*50)
            print("🔍 Initializing ORB Trading Bot...")
            print("="*50)
            sys.stdout.flush()

            if not API_KEY or not SECRET_KEY:
                print("❌ ERROR: API_KEY or SECRET_KEY missing!")
                print("Please set these in Render Environment Variables")
                sys.stdout.flush()
                return

            config_errors = self.validate_config()
            if config_errors:
                for err in config_errors:
                    print(f"❌ Config error: {err}")
                    logger.error(f"Config validation failed: {err}")
                sys.stdout.flush()
                print("❌ Bot cannot start due to configuration errors.")
                sys.stdout.flush()
                return

            masked_api = API_KEY[:4] + "****" + API_KEY[-4:] if len(API_KEY) > 8 else "****"
            print(f"🔑 API Key: {masked_api}")
            sys.stdout.flush()

            self.client = await self.connect_with_retry(max_retries=5)

            try:
                print(f"⚙️ Setting leverage to {LEVERAGE}x for {SYMBOL}...")
                sys.stdout.flush()
                await self.client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)
                print(f"✅ Leverage set to {LEVERAGE}x")
                sys.stdout.flush()
            except Exception as e:
                print(f"⚠️ Leverage setting warning: {e}")
                sys.stdout.flush()

            try:
                await self.client.futures_change_margin_type(symbol=SYMBOL, marginType='ISOLATED')
                print(f"✅ Margin mode set to ISOLATED")
                sys.stdout.flush()
            except Exception as e:
                if "No need to change" in str(e):
                    print(f"ℹ️ Margin mode already ISOLATED")
                else:
                    print(f"⚠️ Margin mode warning: {e}")
                sys.stdout.flush()

            try:
                await self.client.futures_change_position_mode(dualSidePosition=False)
                print(f"✅ Position mode set to ONE-WAY")
                sys.stdout.flush()
            except Exception as e:
                if "No need to change" in str(e):
                    print(f"ℹ️ Position mode already ONE-WAY")
                else:
                    print(f"⚠️ Position mode warning: {e}")
                sys.stdout.flush()

            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    asyncio.get_event_loop().add_signal_handler(
                        sig, lambda: asyncio.create_task(self.shutdown())
                    )
                except NotImplementedError:
                    pass

            await self.recover_opening_range()
            await self.recover_active_position()
            await self.recover_trade_count()
            await self.recover_pending_orders()

            balance = await self.get_usdt_balance()
            print(f"💰 Account Balance: {balance:.2f} USDT")
            sys.stdout.flush()

            try:
                test_klines = await self.client.futures_klines(symbol=SYMBOL, interval=INTERVAL, limit=1)
                if test_klines:
                    test_open_ms = test_klines[0][0]
                    test_utc = datetime.fromtimestamp(test_open_ms / 1000, tz=pytz.utc)
                    test_ny = test_utc.astimezone(self.ny_tz)
                    print(f"✅ Connectivity Test PASSED! Latest candle: {test_ny.strftime('%Y-%m-%d %H:%M')} NY, Close: {test_klines[0][4]}")
                else:
                    print("⚠️ Connectivity Test: Got empty response from REST API!")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ Connectivity Test FAILED: {e}")
                sys.stdout.flush()

            print(f"📡 Starting WebSocket stream for {SYMBOL} {INTERVAL}...")
            sys.stdout.flush()

            print(f"⏰ NY Session: {NY_OPEN_HOUR:02d}:{NY_OPEN_MINUTE:02d} {NY_TIMEZONE}")
            ny_now = datetime.now(self.ny_tz)
            ist_now = datetime.now(pytz.timezone('Asia/Kolkata'))
            print(f"🕐 Current NY Time: {ny_now.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"🕐 Current IST Time: {ist_now.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*50 + "\n")
            sys.stdout.flush()

            net_name = "TESTNET" if IS_TESTNET else "MAINNET"
            self.notify(f"🟢 Bot STARTED | {net_name} | {SYMBOL} | Balance: ${balance:.2f} | NY: {ny_now.strftime('%H:%M')} IST: {ist_now.strftime('%H:%M')}")

            RECV_TIMEOUT = 600
            last_msg_count = 0
            candles_processed = 0

            while not self._shutdown_requested:
                try:
                    self.bm = BinanceSocketManager(self.client)
                    stream = self.bm.kline_futures_socket(SYMBOL, interval=INTERVAL)

                    print("✅ Stream connected! Waiting for candle data...")
                    sys.stdout.flush()

                    async with stream as s:
                        while not self._shutdown_requested:
                            try:
                                msg = await asyncio.wait_for(s.recv(), timeout=RECV_TIMEOUT)
                                last_msg_count += 1

                                if last_msg_count == 1:
                                    print(f"📨 First WebSocket message received! Type: {msg.get('e', 'unknown')}")
                                    sys.stdout.flush()

                                if msg['e'] in ['kline', 'continuous_kline']:
                                    kline = msg['k']

                                    if last_msg_count % 60 == 0:
                                        ny_now = datetime.now(self.ny_tz)
                                        print(f"💓 Heartbeat: {ny_now.strftime('%H:%M:%S')} NY | "
                                              f"msgs={last_msg_count} | candles={candles_processed} | "
                                              f"OR={'SET' if self.or_set else 'WAITING'} | "
                                              f"pos={'ACTIVE' if self.active_position else 'NONE'} | "
                                              f"pend={'YES' if self.pending_order_id else 'NO'}")
                                        sys.stdout.flush()

                                    if kline['x']:
                                        candles_processed += 1
                                        await self.process_closed_candle(kline, stream=s)
                                    elif self.pending_order_id and not self.active_position:
                                        filled = await self.check_pending_limit_fill()
                                        if filled:
                                            candles_processed += 1
                                elif msg:
                                    print(f"⚠️ Non-kline message: {str(msg)[:200]}")
                                    sys.stdout.flush()

                            except asyncio.TimeoutError:
                                print(f"⚠️ No WebSocket data for {RECV_TIMEOUT}s! Connection likely dead.")
                                print("🔄 Breaking out to reconnect...")
                                sys.stdout.flush()
                                break

                            except asyncio.CancelledError:
                                print("🛑 Bot shutdown requested")
                                sys.stdout.flush()
                                self._shutdown_requested = True
                                break

                except asyncio.CancelledError:
                    print("🛑 Bot shutdown requested")
                    sys.stdout.flush()
                    self._shutdown_requested = True
                    break

                except Exception as e:
                    print(f"⚠️ WebSocket/stream error: {e}")
                    logger.error(f"WebSocket error: {e}")
                    sys.stdout.flush()

                if self._shutdown_requested:
                    break

                print("🔄 Reconnecting in 5 seconds...")
                sys.stdout.flush()
                await asyncio.sleep(5)

                try:
                    await self.client.close_connection()
                except Exception:
                    pass

                try:
                    self.client = await self.connect_with_retry(max_retries=3)
                    print("✅ Reconnected to Binance successfully!")
                    sys.stdout.flush()
                    # Recover state after reconnect
                    if not self.or_set:
                        await self.recover_opening_range()
                    await self.recover_active_position()
                    await self.recover_pending_orders()
                except Exception as reconnect_error:
                    print(f"❌ Reconnection failed after retries: {reconnect_error}")
                    print("🔄 Retrying in 30 seconds...")
                    sys.stdout.flush()
                    await asyncio.sleep(30)
                    continue

        except Exception as e:
            print(f"\n❌ FATAL ERROR in bot: {e}")
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            self.notify(f"🔥 FATAL ERROR | Bot crashed\n{e}")
        finally:
            await self.shutdown()


if __name__ == "__main__":
    print("\n" + "="*50)
    print("🌐 Starting Keep Alive Web Server...")
    print("="*50)
    sys.stdout.flush()

    keep_alive()

    print("\n🚀 Initializing ORB Trading Bot...")
    sys.stdout.flush()

    while True:
        bot = LiveORBSignals()
        try:
            asyncio.run(bot.start())
        except KeyboardInterrupt:
            print("\n🛑 Bot stopped by user (KeyboardInterrupt)")
            sys.stdout.flush()
            break
        except Exception as e:
            print(f"\n❌ Bot crashed: {e}")
            import traceback
            traceback.print_exc()
            sys.stdout.flush()
            logger.error(f"Bot crashed: {e}", exc_info=True)
        finally:
            logger.info("Bot session ended")
        print("🔄 Restarting bot in 10 seconds...")
        sys.stdout.flush()
        time.sleep(10)
