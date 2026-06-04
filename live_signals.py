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
            acc = await self.client.futures_account_balance()
            for b in acc:
                if b['asset'] == 'USDT':
                    balance = float(b['balance'])
                    print(f"💰 Available USDT Balance: {balance}")
                    sys.stdout.flush()
                    return balance
        except Exception as e:
            print(f"⚠️ Balance fetch error: {e}")
            sys.stdout.flush()
        return INITIAL_CAPITAL

    def calculate_quantity(self, entry, stop, side, balance):
        risk_amount = balance * (RISK_PER_TRADE_PCT / 100)
        effective_entry = entry * (1 + SLIPPAGE_PCT/100) if side == 'BUY' else entry * (1 - SLIPPAGE_PCT/100)
        risk_per_unit = abs(effective_entry - stop)
        if risk_per_unit <= 0:
            print("⚠️ Risk per unit is zero, cannot calculate quantity")
            sys.stdout.flush()
            return 0
        qty = risk_amount / risk_per_unit
        print(f"📊 Qty Calc: Risk={risk_amount:.2f}, Risk/Unit={risk_per_unit:.2f}, Qty={qty:.3f}")
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

    async def place_market_order(self, side, quantity):
        try:
            print(f"🚀 Placing {side} MARKET order for {quantity} {SYMBOL}...")
            sys.stdout.flush()
            order = await self.client.futures_create_order(
                symbol=SYMBOL,
                side=side,
                type='MARKET',
                quantity=quantity
            )
            print(f"✅ {side} order placed successfully! OrderID: {order['orderId']}")
            sys.stdout.flush()
            return order
        except Exception as e:
            print(f"❌ Entry order error: {e}")
            sys.stdout.flush()
            return None

    async def place_exit_orders(self, side, stop_price, tp_price, retries=3):
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        sl_success = False

        # Small delay to ensure position is registered on exchange
        await asyncio.sleep(0.5)

        for attempt in range(retries):
            try:
                print(f"🛑 Placing SL {close_side} order at {stop_price:.2f} (attempt {attempt+1})...")
                sys.stdout.flush()
                sl = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='STOP_MARKET',
                    stopPrice=round(stop_price, PRICE_PRECISION),
                    closePosition=True,
                )
                self.sl_order_id = sl['orderId']
                print(f"✅ SL placed successfully! ID: {sl['orderId']}")
                sys.stdout.flush()
                sl_success = True
                break
            except Exception as e:
                print(f"❌ SL attempt {attempt+1} failed: {e}")
                sys.stdout.flush()
                if attempt < retries - 1:
                    await asyncio.sleep(1)

        for attempt in range(retries):
            try:
                print(f"🎯 Placing TP {close_side} order at {tp_price:.2f} (attempt {attempt+1})...")
                sys.stdout.flush()
                tp = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='TAKE_PROFIT_MARKET',
                    stopPrice=round(tp_price, PRICE_PRECISION),
                    closePosition=True,
                )
                self.tp_order_id = tp['orderId']
                print(f"✅ TP placed successfully! ID: {tp['orderId']}")
                sys.stdout.flush()
                break
            except Exception as e:
                print(f"❌ TP attempt {attempt+1} failed: {e}")
                sys.stdout.flush()
                if attempt < retries - 1:
                    await asyncio.sleep(1)

        return sl_success

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

    async def update_trailing_stop(self, candle_high, candle_low, candle_close):
        if not self.active_position:
            return

        pos = self.active_position
        side = pos['side']
        highest_high = pos.get('highest_high', candle_high)
        lowest_low = pos.get('lowest_low', candle_low)
        breakeven_triggered = pos.get('breakeven_triggered', False)
        current_sl = pos['sl']

        breakeven_trigger_pct = 0.2 / 100
        trailing_pct = 0.1 / 100

        new_sl = current_sl
        update_needed = False

        if side == 'BUY':
            if candle_high > highest_high:
                highest_high = candle_high
                if DEBUG_MODE:
                    print(f"📈 New High: {highest_high:.2f}")
                    sys.stdout.flush()

            profit_pct = (highest_high - pos['entry']) / pos['entry']
            if profit_pct >= breakeven_trigger_pct and not breakeven_triggered:
                new_sl = pos['entry']
                breakeven_triggered = True
                update_needed = True
                print(f"🟢 Breakeven triggered! Moving SL to entry: {new_sl:.2f}")
                sys.stdout.flush()

            if breakeven_triggered:
                trail_sl = highest_high * (1 - trailing_pct)
                if trail_sl > new_sl:
                    new_sl = trail_sl
                    update_needed = True
                    print(f"📈 Trailing SL Up: {new_sl:.2f}")
                    sys.stdout.flush()
        else:
            if candle_low < lowest_low:
                lowest_low = candle_low
                if DEBUG_MODE:
                    print(f"📉 New Low: {lowest_low:.2f}")
                    sys.stdout.flush()

            profit_pct = (pos['entry'] - lowest_low) / pos['entry']
            if profit_pct >= breakeven_trigger_pct and not breakeven_triggered:
                new_sl = pos['entry']
                breakeven_triggered = True
                update_needed = True
                print(f"🟢 Breakeven triggered! Moving SL to entry: {new_sl:.2f}")
                sys.stdout.flush()

            if breakeven_triggered:
                trail_sl = lowest_low * (1 + trailing_pct)
                if trail_sl < new_sl:
                    new_sl = trail_sl
                    update_needed = True
                    print(f"📉 Trailing SL Down: {new_sl:.2f}")
                    sys.stdout.flush()

        if update_needed:
            print(f"🔄 Updating SL: {current_sl:.2f} → {new_sl:.2f}")
            sys.stdout.flush()

            if self.sl_order_id:
                await self.cancel_order(self.sl_order_id)

            close_side = 'SELL' if side == 'BUY' else 'BUY'
            try:
                new_sl_order = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='STOP_MARKET',
                    stopPrice=round(new_sl, PRICE_PRECISION),
                    closePosition=True,
                )
                self.sl_order_id = new_sl_order['orderId']
                print(f"✅ New SL placed at {new_sl:.2f}")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ Trailing SL order error: {e}")
                sys.stdout.flush()

        self.active_position['highest_high'] = highest_high
        self.active_position['lowest_low'] = lowest_low
        self.active_position['breakeven_triggered'] = breakeven_triggered
        self.active_position['sl'] = new_sl

    async def check_position_status(self):
        if not self.active_position:
            return False

        try:
            pos_info = await self.client.futures_position_information(symbol=SYMBOL)
            position_exists = False
            for p in pos_info:
                amt = float(p['positionAmt'])
                if amt != 0:
                    position_exists = True
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
            if self.sl_order_id is None:
                print("🚨 CRITICAL: Active position has no Stop Loss order! Forcing market close.")
                sys.stdout.flush()
                close_side = 'SELL' if self.active_position['side'] == 'BUY' else 'BUY'
                try:
                    await self.client.futures_create_order(
                        symbol=SYMBOL,
                        side=close_side,
                        type='MARKET',
                        quantity=abs(float(p['positionAmt']))
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

    async def process_closed_candle(self, kline):
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

            # Force close any position at 4:00 PM NY (market close)
            if self.active_position and ny_hour == 16 and ny_minute == 0:
                print("🕟 End of NY session – closing any open position.")
                sys.stdout.flush()
                side = self.active_position['side']
                close_side = 'SELL' if side == 'BUY' else 'BUY'
                qty = abs((await self.client.futures_position_information(symbol=SYMBOL))[0]['positionAmt'])
                try:
                    await self.client.futures_create_order(
                        symbol=SYMBOL,
                        side=close_side,
                        type='MARKET',
                        quantity=float(qty)
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

            if close > self.or_high and not self.breakout_detected['BUY'] and not self.breakout_done['BUY']:
                if candle_range_pct >= BREAKOUT_PCT:
                    self.breakout_detected['BUY'] = True
                    print(f"📈 Breakout BUY Detected! Candle closed above OR High ({self.or_high:.2f}). Waiting for retest...")
                    sys.stdout.flush()

            if close < self.or_low and not self.breakout_detected['SELL'] and not self.breakout_done['SELL']:
                if candle_range_pct >= BREAKOUT_PCT:
                    self.breakout_detected['SELL'] = True
                    print(f"📉 Breakout SELL Detected! Candle closed below OR Low ({self.or_low:.2f}). Waiting for retest...")
                    sys.stdout.flush()

            if self.breakout_detected['BUY'] and not self.breakout_done['BUY']:
                retest_upper = self.or_high * (1 + RETEST_ZONE_PCT/100)
                retest_lower = self.or_high * (1 - RETEST_ZONE_PCT/100)
                if low <= retest_upper and high >= retest_lower:
                    print(f"\n{'!'*50}")
                    print(f"🚀 BUY RETEST SIGNAL DETECTED!")
                    print(f"   Low: {low:.2f} <= Retest Upper: {retest_upper:.2f}")
                    print(f"   High: {high:.2f} >= Retest Lower: {retest_lower:.2f}")
                    print(f"{'!'*50}")
                    sys.stdout.flush()

                    await self.execute_trade('BUY', self.or_high, self.or_low)
                    self.breakout_done['BUY'] = True
                    self.trades_taken_today += 1
                    return

            if self.breakout_detected['SELL'] and not self.breakout_done['SELL']:
                retest_upper = self.or_low * (1 + RETEST_ZONE_PCT/100)
                retest_lower = self.or_low * (1 - RETEST_ZONE_PCT/100)
                if high >= retest_lower and low <= retest_upper:
                    print(f"\n{'!'*50}")
                    print(f"🔻 SELL RETEST SIGNAL DETECTED!")
                    print(f"   High: {high:.2f} >= Retest Lower: {retest_lower:.2f}")
                    print(f"   Low: {low:.2f} <= Retest Upper: {retest_upper:.2f}")
                    print(f"{'!'*50}")
                    sys.stdout.flush()

                    await self.execute_trade('SELL', self.or_low, self.or_high)
                    self.breakout_done['SELL'] = True
                    self.trades_taken_today += 1
                    return

        except Exception as e:
            print(f"❌ Error processing candle: {e}")
            sys.stdout.flush()

    async def execute_trade(self, side, entry_level, stop_level):
        balance = await self.get_usdt_balance()
        qty = self.calculate_quantity(entry_level, stop_level, side, balance)

        if qty <= 0:
            print("⚠️ Invalid quantity, trade aborted")
            sys.stdout.flush()
            return

        order = await self.place_market_order(side, qty)
        if not order:
            print("❌ Entry order failed, trade aborted")
            sys.stdout.flush()
            return

        if side == 'BUY':
            stop = stop_level * (1 - SL_BUFFER_PCT/100)
            risk = entry_level - stop
            target = entry_level + risk * RISK_REWARD
        else:
            stop = stop_level * (1 + SL_BUFFER_PCT/100)
            risk = stop - entry_level
            target = entry_level - risk * RISK_REWARD

        stop = self.validate_stop_distance(side, entry_level, stop)

        print(f"\n📋 Trade Details:")
        print(f"   Side: {side}")
        print(f"   Entry: {entry_level:.2f}")
        print(f"   Stop Loss: {stop:.2f} (Risk: {risk:.2f})")
        print(f"   Take Profit: {target:.2f} (RR: 1:{RISK_REWARD})")
        sys.stdout.flush()

        sl_placed = await self.place_exit_orders(side, stop, target)

        if not sl_placed:
            print("🚨 CRITICAL: Stop Loss order failed to place! Initiating emergency market exit...")
            sys.stdout.flush()
            exit_side = 'SELL' if side == 'BUY' else 'BUY'
            try:
                await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=exit_side,
                    type='MARKET',
                    quantity=qty
                )
                print("🚨 Emergency market exit order placed successfully!")
            except Exception as e:
                print(f"🚨 CRITICAL ERROR: Emergency exit failed! Manual intervention required! Error: {e}")
            sys.stdout.flush()

            if self.tp_order_id:
                await self.cancel_order(self.tp_order_id)
                self.tp_order_id = None

            self.active_position = None
            return

        self.active_position = {
            'side': side,
            'entry': entry_level,
            'sl': stop,
            'tp': target,
            'highest_high': entry_level,
            'lowest_low': entry_level,
            'breakeven_triggered': False
        }

        print(f"\n✅ {side} POSITION ACTIVE - Monitoring...")
        print(f"{'='*50}\n")
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
                        # Still do not track it – it's unsafe
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

    async def recover_trade_count(self):
        try:
            print("🔍 Recovering trades taken today...")
            sys.stdout.flush()

            now_utc = datetime.now(pytz.utc)
            today_start_utc = datetime(now_utc.year, now_utc.month, now_utc.day, 0, 0, 0, tzinfo=pytz.utc)
            start_time_ms = int(today_start_utc.timestamp() * 1000)

            all_orders = await self.client.futures_get_all_orders(symbol=SYMBOL, startTime=start_time_ms)

            entry_trades = 0
            for o in all_orders:
                if o['status'] == 'FILLED' and o['type'] == 'MARKET':
                    entry_trades += 1

            self.trades_taken_today = entry_trades
            print(f"📊 Recovered Trade Count: {self.trades_taken_today} entry trades today")
            sys.stdout.flush()

        except Exception as e:
            print(f"⚠️ Error recovering trade count: {e}")
            sys.stdout.flush()

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

            await self.recover_opening_range()
            await self.recover_active_position()
            await self.recover_trade_count()

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

            RECV_TIMEOUT = 360
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
                                              f"pos={'ACTIVE' if self.active_position else 'NONE'}")
                                        sys.stdout.flush()

                                    if kline['x']:
                                        candles_processed += 1
                                        await self.process_closed_candle(kline)
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
