import asyncio
import os
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
    MAX_TRADES_PER_DAY
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
        self.candles_today = []
        self.active_position = None
        self.sl_order_id = None
        self.tp_order_id = None

    async def get_usdt_balance(self):
        try:
            acc = await self.client.futures_account_balance()
            for b in acc:
                if b['asset'] == 'USDT':
                    return float(b['balance'])
        except Exception as e:
            print(f"Balance fetch error: {e}")
        return INITIAL_CAPITAL

    def calculate_quantity(self, entry, stop, side, balance):
        risk_amount = balance * (RISK_PER_TRADE_PCT / 100)
        effective_entry = entry * (1 + SLIPPAGE_PCT/100) if side == 'BUY' else entry * (1 - SLIPPAGE_PCT/100)
        risk_per_unit = abs(effective_entry - stop)
        if risk_per_unit <= 0:
            return 0
        qty = risk_amount / risk_per_unit
        return round(qty, 3)

    async def place_market_order(self, side, quantity):
        try:
            order = await self.client.futures_create_order(
                symbol=SYMBOL,
                side=side,
                type='MARKET',
                quantity=quantity
            )
            print(f"✅ {side} order placed: {order['orderId']}")
            return order
        except Exception as e:
            print(f"❌ Entry order error: {e}")
            return None

    async def place_exit_orders(self, side, stop_price, tp_price):
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        try:
            sl = await self.client.futures_create_order(
                symbol=SYMBOL,
                side=close_side,
                type='STOP_MARKET',
                stopPrice=round(stop_price, 2),
                closePosition=True,
                timeInForce='GTC'
            )
            self.sl_order_id = sl['orderId']
            print(f"🛑 SL placed (ID: {sl['orderId']}) at {stop_price}")
        except Exception as e:
            print(f"SL order error: {e}")

        try:
            tp = await self.client.futures_create_order(
                symbol=SYMBOL,
                side=close_side,
                type='TAKE_PROFIT_MARKET',
                stopPrice=round(tp_price, 2),
                closePosition=True,
                timeInForce='GTC'
            )
            self.tp_order_id = tp['orderId']
            print(f"🎯 TP placed (ID: {tp['orderId']}) at {tp_price}")
        except Exception as e:
            print(f"TP order error: {e}")

    async def cancel_order(self, order_id):
        try:
            await self.client.futures_cancel_order(symbol=SYMBOL, orderId=order_id)
            print(f"❌ Order cancelled: {order_id}")
        except Exception as e:
            print(f"Cancel order error: {e}")

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
            profit_pct = (highest_high - pos['entry']) / pos['entry']
            if profit_pct >= breakeven_trigger_pct and not breakeven_triggered:
                new_sl = pos['entry']
                breakeven_triggered = True
                update_needed = True
            if breakeven_triggered:
                trail_sl = highest_high * (1 - trailing_pct)
                if trail_sl > new_sl:
                    new_sl = trail_sl
                    update_needed = True
        else:
            if candle_low < lowest_low:
                lowest_low = candle_low
            profit_pct = (pos['entry'] - lowest_low) / pos['entry']
            if profit_pct >= breakeven_trigger_pct and not breakeven_triggered:
                new_sl = pos['entry']
                breakeven_triggered = True
                update_needed = True
            if breakeven_triggered:
                trail_sl = lowest_low * (1 + trailing_pct)
                if trail_sl < new_sl:
                    new_sl = trail_sl
                    update_needed = True

        if update_needed:
            print(f"🔄 Updating SL to {new_sl:.2f}")
            if self.sl_order_id:
                await self.cancel_order(self.sl_order_id)
            close_side = 'SELL' if side == 'BUY' else 'BUY'
            try:
                new_sl_order = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='STOP_MARKET',
                    stopPrice=round(new_sl, 2),
                    closePosition=True,
                    timeInForce='GTC'
                )
                self.sl_order_id = new_sl_order['orderId']
                print(f"✅ New SL placed at {new_sl:.2f}")
            except Exception as e:
                print(f"Trailing SL order error: {e}")

        self.active_position['highest_high'] = highest_high
        self.active_position['lowest_low'] = lowest_low
        self.active_position['breakeven_triggered'] = breakeven_triggered
        self.active_position['sl'] = new_sl

    async def check_position_status(self):
        if not self.active_position:
            return
        try:
            pos_info = await self.client.futures_position_information(symbol=SYMBOL)
            for p in pos_info:
                amt = float(p['positionAmt'])
                if amt != 0:
                    return
            print("📴 Position closed (SL/TP hit).")
            self.active_position = None
            if self.sl_order_id:
                await self.cancel_order(self.sl_order_id)
            if self.tp_order_id:
                await self.cancel_order(self.tp_order_id)
            self.sl_order_id = None
            self.tp_order_id = None
        except Exception as e:
            print(f"Position check error: {e}")

    async def process_closed_candle(self, kline):
        candle_ts = kline['t']
        candle_time = datetime.utcfromtimestamp(candle_ts / 1000)
        ny_time = self.ny_tz.localize(candle_time)
        ny_date = ny_time.date()
        ny_hour = ny_time.hour
        ny_minute = ny_time.minute

        if self.today != ny_date:
            print(f"🆕 New trading day: {ny_date}")
            self.today = ny_date
            self.or_set = False
            self.or_high = None
            self.or_low = None
            self.trades_taken_today = 0
            self.breakout_done = {'BUY': False, 'SELL': False}
            self.candles_today = []

        if self.active_position:
            await self.update_trailing_stop(
                float(kline['h']), float(kline['l']), float(kline['c'])
            )
            await self.check_position_status()
            return

        if not self.or_set:
            if ny_hour == NY_OPEN_HOUR and ny_minute == NY_OPEN_MINUTE:
                self.or_high = float(kline['h'])
                self.or_low = float(kline['l'])
                self.or_set = True
                print(f"🎯 OR Set: High={self.or_high}, Low={self.or_low}")
            return

        self.candles_today.append({
            'timestamp': candle_ts,
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

        if close > self.or_high and not self.breakout_done['BUY']:
            candle_range_pct = ((high - low) / low) * 100
            if candle_range_pct >= BREAKOUT_PCT:
                retest_upper = self.or_high * (1 + RETEST_ZONE_PCT/100)
                retest_lower = self.or_high * (1 - RETEST_ZONE_PCT/100)
                if low <= retest_upper and high >= retest_lower:
                    await self.execute_trade('BUY', self.or_high, self.or_low)
                    self.breakout_done['BUY'] = True
                    self.trades_taken_today += 1
                    return

        if close < self.or_low and not self.breakout_done['SELL']:
            candle_range_pct = ((high - low) / low) * 100
            if candle_range_pct >= BREAKOUT_PCT:
                retest_upper = self.or_low * (1 + RETEST_ZONE_PCT/100)
                retest_lower = self.or_low * (1 - RETEST_ZONE_PCT/100)
                if high >= retest_lower and low <= retest_upper:
                    await self.execute_trade('SELL', self.or_low, self.or_high)
                    self.breakout_done['SELL'] = True
                    self.trades_taken_today += 1
                    return

    async def execute_trade(self, side, entry_level, stop_level):
        balance = await self.get_usdt_balance()
        qty = self.calculate_quantity(entry_level, stop_level, side, balance)
        if qty <= 0:
            print("⚠️ Quantity zero, trade skipped.")
            return

        order = await self.place_market_order(side, qty)
        if not order:
            return

        if side == 'BUY':
            stop = stop_level * (1 - SL_BUFFER_PCT/100)
            risk = entry_level - stop
            target = entry_level + risk * RISK_REWARD
        else:
            stop = stop_level * (1 + SL_BUFFER_PCT/100)
            risk = stop - entry_level
            target = entry_level - risk * RISK_REWARD

        await self.place_exit_orders(side, stop, target)

        self.active_position = {
            'side': side,
            'entry': entry_level,
            'sl': stop,
            'tp': target,
            'highest_high': entry_level,
            'lowest_low': entry_level,
            'breakeven_triggered': False
        }

        print(f"📈 {side} POSITION ACTIVE | Entry: {entry_level} | SL: {stop} | TP: {target}")

    async def start(self):
        if not API_KEY or not SECRET_KEY:
            print("❌ API keys missing!")
            return

        print("🔌 Connecting to Binance Testnet...")
        self.client = await AsyncClient.create(API_KEY, SECRET_KEY, testnet=True)

        try:
            await self.client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)
            print(f"⚙️ Leverage set to {LEVERAGE}x")
        except Exception as e:
            print(f"Leverage warning: {e}")

        self.bm = BinanceSocketManager(self.client)
        stream = self.bm.kline_futures_socket(SYMBOL, interval=INTERVAL)
        print(f"✅ Streaming {SYMBOL} {INTERVAL}...")

        async with stream as s:
            while True:
                try:
                    msg = await s.recv()
                    if msg and msg.get('e') == 'kline':
                        kline = msg['k']
                        if kline['x']:
                            await self.process_closed_candle(kline)
                except asyncio.CancelledError:
                    print("🛑 Task cancelled.")
                    break
                except Exception as e:
                    print(f"WebSocket error: {e}. Reconnecting in 5s...")
                    await asyncio.sleep(5)
                    await self.client.close_connection()
                    self.client = await AsyncClient.create(API_KEY, SECRET_KEY, testnet=True)
                    self.bm = BinanceSocketManager(self.client)
                    stream = self.bm.kline_futures_socket(SYMBOL, interval=INTERVAL)
                    continue

        await self.client.close_connection()

if __name__ == "__main__":
    print("🌐 Starting Keep Alive Web Server...")
    keep_alive()
    bot = LiveORBSignals()
    asyncio.run(bot.start())
