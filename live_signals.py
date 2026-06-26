import asyncio
import os
import sys
import pandas as pd
import pytz
from datetime import datetime, time
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv
from keep_alive import keep_alive

load_dotenv()

from config import (
    SYMBOL, INTERVAL, NY_TIMEZONE, NY_OPEN_HOUR, NY_OPEN_MINUTE,
    SLIPPAGE_PCT, RISK_PER_TRADE_PCT, INITIAL_CAPITAL, LEVERAGE,
    BREAKOUT_PCT, RETEST_ZONE_PCT, RISK_REWARD, SL_BUFFER_PCT,
    BREAKEVEN_TRIGGER, MAKER_FEE, TAKER_FEE,
    MAX_TRADES_PER_DAY, DEBUG_MODE, QUANTITY_PRECISION, PRICE_PRECISION
)

API_KEY = os.environ.get('API_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

if not API_KEY or not SECRET_KEY:
    from config import API_KEY as CONFIG_API_KEY, SECRET_KEY as CONFIG_SECRET_KEY
    API_KEY = API_KEY or CONFIG_API_KEY
    SECRET_KEY = SECRET_KEY or CONFIG_SECRET_KEY

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

    async def connect_with_retry(self, max_retries=5):
        """Attempt to connect to Binance Testnet with exponential backoff."""
        for attempt in range(max_retries):
            try:
                print(f"🔌 Connection attempt {attempt + 1}/{max_retries}...")
                sys.stdout.flush()
                client = await AsyncClient.create(API_KEY, SECRET_KEY, testnet=True)
                await client.ping()
                print("✅ Connected to Binance Testnet successfully!")
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
        effective_balance = balance
        risk_amount = effective_balance * (RISK_PER_TRADE_PCT / 100)
        risk_per_unit = abs(entry - stop)
        if risk_per_unit <= 0:
            print("⚠️ Risk per unit is zero, cannot calculate quantity")
            sys.stdout.flush()
            return 0
        qty_risk = risk_amount / risk_per_unit
        max_position_value = balance * LEVERAGE
        qty_margin = max_position_value / entry if entry > 0 else float('inf')
        qty = min(qty_risk, qty_margin)
        if qty < qty_risk:
            print(f"⚠️ Risk-based qty ({qty_risk:.3f}) exceeds margin limit ({qty_margin:.3f}). Capping to {qty:.3f}")
            sys.stdout.flush()
        print(f"📊 Qty Calc: EffectiveBal={effective_balance:.2f}, Leverage={LEVERAGE}x, Risk={risk_amount:.2f}, "
              f"Risk/Unit={risk_per_unit:.2f}, MaxPosValue={max_position_value:.2f}, Qty={qty:.3f}")
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

    async def place_limit_entry_order(self, side, price):
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
            print(f"❌ LIMIT entry error: {e}")
            sys.stdout.flush()
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

    async def place_exit_orders(self, side, stop_price, tp_price, quantity, retries=5):
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        sl_success = False
        tp_success = False
        self.sl_order_id = None
        self.tp_order_id = None

        # Cancel all existing SL/TP orders on the close side to avoid -4130
        try:
            open_orders = await self.client.futures_get_open_orders(symbol=SYMBOL)
            for order in open_orders:
                if order['side'] == close_side and order['type'] in ('STOP_MARKET', 'TAKE_PROFIT_MARKET'):
                    try:
                        await self.client.futures_cancel_order(symbol=SYMBOL, orderId=order['orderId'])
                        print(f"🗑️ Cancelled existing {order['type']} order {order['orderId']}")
                    except Exception as cancel_err:
                        if "Unknown order sent" not in str(cancel_err):
                            print(f"⚠️ Could not cancel order {order['orderId']}: {cancel_err}")
            await asyncio.sleep(0.5)
        except Exception as e:
            print(f"⚠️ Error cancelling existing orders: {e}")

        await asyncio.sleep(1.5)

        # Place SL with closePosition=True (ignores quantity, closes full position)
        for attempt in range(retries):
            try:
                print(f"🛑 Placing SL {close_side} STOP_MARKET at {stop_price:.2f} (attempt {attempt+1})...")
                sys.stdout.flush()
                sl = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='STOP_MARKET',
                    stopPrice=round(stop_price, PRICE_PRECISION),
                    closePosition='true',
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
                    print(f"⚠️ SL response missing orderId, retrying...")
                    sys.stdout.flush()
            except Exception as e:
                print(f"❌ SL attempt {attempt+1} failed: {e}")
                sys.stdout.flush()
                if attempt < retries - 1:
                    await asyncio.sleep(2)

        # Check current mark price before placing TP (might have already hit TP)
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
                print(f"⚠️ TP {tp_price:.2f} would trigger immediately (mark {mark_price:.2f}). Using MARKET order instead.")
                sys.stdout.flush()
                if sl_success and self.sl_order_id:
                    await self.cancel_order(self.sl_order_id)
                    self.sl_order_id = None
                try:
                    await self.client.futures_create_order(
                        symbol=SYMBOL,
                        side=close_side,
                        type='MARKET',
                        quantity=quantity,
                    )
                    print(f"✅ TP executed via MARKET order at {mark_price:.2f}")
                    sys.stdout.flush()
                    return False, False
                except Exception as e:
                    print(f"❌ MARKET TP exit failed: {e}")
                    sys.stdout.flush()
        except Exception as e:
            print(f"⚠️ Could not fetch mark price: {e}")
            sys.stdout.flush()

        # Place TP with closePosition=True
        for attempt in range(retries):
            try:
                print(f"🎯 Placing TP {close_side} TAKE_PROFIT_MARKET at {tp_price:.2f} (attempt {attempt+1})...")
                sys.stdout.flush()
                tp = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='TAKE_PROFIT_MARKET',
                    stopPrice=round(tp_price, PRICE_PRECISION),
                    closePosition='true',
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
            return sl_success, tp_success

        # Emergency MARKET exit if either SL or TP failed after all retries
        if not sl_success or not tp_success:
            print("🚨 CRITICAL: SL/TP placement incomplete! Emergency MARKET exit...")
            sys.stdout.flush()
            try:
                await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='MARKET',
                    quantity=quantity,
                )
                print("✅ Emergency MARKET exit placed!")
                sys.stdout.flush()
            except Exception as e:
                print(f"🚨 Emergency exit failed: {e}")
                sys.stdout.flush()
            return False, False

        return sl_success, tp_success

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
                sl_placed, tp_placed = await self.place_exit_orders(side, stop, target, qty)
                if not sl_placed and not tp_placed:
                    print("🚨 CRITICAL: Both SL and TP failed! Emergency market exit...")
                    sys.stdout.flush()
                    close_side = 'SELL' if side == 'BUY' else 'BUY'
                    try:
                        await self.client.futures_create_order(
                            symbol=SYMBOL, side=close_side, type='MARKET', quantity=qty
                        )
                    except Exception as e:
                        print(f"🚨 Emergency exit failed: {e}")
                    self.pending_order_id = None
                    return False
                self.active_position = {
                    'side': side,
                    'entry': fill_price,
                    'sl': stop,
                    'tp': target,
                    'breakeven_triggered': False
                }
                order_id = self.pending_order_id
                self.pending_order_id = None
                self.trades_taken_today += 1
                print(f"\n✅ {side} POSITION ACTIVE @ {fill_price:.2f} - Monitoring...")
                print(f"{'='*50}\n")
                sys.stdout.flush()
                return True
            elif status in ('CANCELED', 'EXPIRED', 'REJECTED'):
                print(f"❌ Pending LIMIT order {self.pending_order_id} {status}")
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

        new_sl = current_sl
        update_needed = False

        if side == 'BUY':
            profit_pct = (candle_high - pos['entry']) / pos['entry']
            if profit_pct >= breakeven_trigger_pct and not breakeven_triggered:
                new_sl = pos['entry']
                breakeven_triggered = True
                update_needed = True
                print(f"🟢 Breakeven triggered! Moving SL to entry: {new_sl:.2f}")
                sys.stdout.flush()
        else:
            profit_pct = (pos['entry'] - candle_low) / pos['entry']
            if profit_pct >= breakeven_trigger_pct and not breakeven_triggered:
                new_sl = pos['entry']
                breakeven_triggered = True
                update_needed = True
                print(f"🟢 Breakeven triggered! Moving SL to entry: {new_sl:.2f}")
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
                    closePosition='true',
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
                print("🚨 CRITICAL: Active position has no Stop Loss order! Forcing market close.")
                sys.stdout.flush()
                close_side = 'SELL' if self.active_position['side'] == 'BUY' else 'BUY'
                try:
                    await self.client.futures_create_order(
                        symbol=SYMBOL,
                        side=close_side,
                        type='MARKET',
                        quantity=current_qty
                    )
                    print("✅ Position closed via emergency market order.")
                    self.active_position = None
                    sys.stdout.flush()
                except Exception as e:
                    print(f"❌ Failed to close position: {e}")
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
                    side = self.active_position['side']
                    close_side = 'SELL' if side == 'BUY' else 'BUY'
                    try:
                        pos_info = await self.client.futures_position_information(symbol=SYMBOL)
                        qty = 0.0
                        for p in pos_info:
                            amt = float(p['positionAmt'])
                            if amt != 0:
                                qty = abs(amt)
                                break
                        if qty > 0:
                            await self.client.futures_create_order(
                                symbol=SYMBOL,
                                side=close_side,
                                type='MARKET',
                                quantity=qty
                            )
                            print("✅ Position closed at end of session.")
                        self.active_position = None
                        sys.stdout.flush()
                    except Exception as e:
                        print(f"❌ Failed to close position at EOD: {e}")
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

            last = self.candles_today[-1]
            close = last['close']
            high = last['high']
            low = last['low']
            candle_range_pct = ((high - low) / low) * 100

            # BUY BREAKOUT → place LIMIT order at OR High (will fill on retest)
            if close > self.or_high and not self.breakout_done['BUY'] and not self.pending_order_id:
                if candle_range_pct >= BREAKOUT_PCT:
                    self.breakout_done['BUY'] = True
                    print(f"📈 Breakout BUY Detected! Candle closed above OR High ({self.or_high:.2f}). Placing LIMIT BUY at OR High...")
                    sys.stdout.flush()
                    self.pending_stop_level = self.or_low
                    await self.place_limit_entry_order('BUY', self.or_high)
                    if self.pending_order_id:
                        print(f"⏳ LIMIT BUY at {self.or_high:.2f} online — will fill when price retests OR High")
                    return

            # SELL BREAKOUT → place LIMIT order at OR Low (will fill on retest)
            if close < self.or_low and not self.breakout_done['SELL'] and not self.pending_order_id:
                if candle_range_pct >= BREAKOUT_PCT:
                    self.breakout_done['SELL'] = True
                    print(f"📉 Breakout SELL Detected! Candle closed below OR Low ({self.or_low:.2f}). Placing LIMIT SELL at OR Low...")
                    sys.stdout.flush()
                    self.pending_stop_level = self.or_high
                    await self.place_limit_entry_order('SELL', self.or_low)
                    if self.pending_order_id:
                        print(f"⏳ LIMIT SELL at {self.or_low:.2f} online — will fill when price retests OR Low")
                    return

        except Exception as e:
            print(f"❌ Error processing candle: {e}")
            sys.stdout.flush()

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

                for candle in self.candles_today:
                    close = candle['close']
                    high = candle['high']
                    low = candle['low']
                    candle_range_pct = ((high - low) / low) * 100

                    if close > self.or_high and not self.breakout_detected['BUY']:
                        if candle_range_pct >= BREAKOUT_PCT:
                            self.breakout_detected['BUY'] = True
                            print(f"📈 [Recovery] Detected BUY Breakout on candle at {candle['ny_time'].strftime('%H:%M')}")
                            sys.stdout.flush()

                    if close < self.or_low and not self.breakout_detected['SELL']:
                        if candle_range_pct >= BREAKOUT_PCT:
                            self.breakout_detected['SELL'] = True
                            print(f"📉 [Recovery] Detected SELL Breakout on candle at {candle['ny_time'].strftime('%H:%M')}")
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
                    close_side = 'SELL' if side == 'BUY' else 'BUY'
                    try:
                        await self.client.futures_create_order(
                            symbol=SYMBOL,
                            side=close_side,
                            type='MARKET',
                            quantity=abs(active_amt)
                        )
                        print("✅ Position closed successfully. No active position tracked.")
                        sys.stdout.flush()
                        self.active_position = None
                        return
                    except Exception as e:
                        print(f"❌ Failed to close position: {e}")
                        sys.stdout.flush()
                        self.active_position = None
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

            RECV_TIMEOUT = 600
            last_msg_count = 0
            candles_processed = 0

            while True:
                try:
                    self.bm = BinanceSocketManager(self.client)
                    stream = self.bm.kline_futures_socket(SYMBOL, interval=INTERVAL)

                    print("✅ Stream connected! Waiting for candle data...")
                    sys.stdout.flush()

                    async with stream as s:
                        while True:
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
                                return

                except asyncio.CancelledError:
                    print("🛑 Bot shutdown requested")
                    sys.stdout.flush()
                    return

                except Exception as e:
                    print(f"⚠️ WebSocket/stream error: {e}")
                    sys.stdout.flush()

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
        finally:
            if self.client:
                await self.client.close_connection()
                print("🔌 Connection closed")
                sys.stdout.flush()


if __name__ == "__main__":
    print("\n" + "="*50)
    print("🌐 Starting Keep Alive Web Server...")
    print("="*50)
    sys.stdout.flush()

    keep_alive()

    print("\n🚀 Initializing ORB Trading Bot...")
    sys.stdout.flush()

    try:
        bot = LiveORBSignals()
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
        sys.stdout.flush()
    except Exception as e:
        print(f"\n❌ Bot crashed: {e}")
        import traceback
        traceback.print_exc()
        sys.stdout.flush()
        import time
        while True:
            time.sleep(60)
