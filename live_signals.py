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

    async def place_exit_orders(self, side, stop_price, tp_price):
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        sl_success = False
        
        # Place Stop Loss
        try:
            print(f"🛑 Placing SL {close_side} order at {stop_price:.2f}...")
            sys.stdout.flush()
            sl = await self.client.futures_create_order(
                symbol=SYMBOL,
                side=close_side,
                type='STOP_MARKET',
                stopPrice=round(stop_price, PRICE_PRECISION),
                closePosition=True,
                timeInForce='GTC'
            )
            self.sl_order_id = sl['orderId']
            print(f"✅ SL placed successfully! ID: {sl['orderId']}, Price: {stop_price:.2f}")
            sys.stdout.flush()
            sl_success = True
        except Exception as e:
            print(f"❌ SL order error: {e}")
            sys.stdout.flush()

        # Place Take Profit
        try:
            print(f"🎯 Placing TP {close_side} order at {tp_price:.2f}...")
            sys.stdout.flush()
            tp = await self.client.futures_create_order(
                symbol=SYMBOL,
                side=close_side,
                type='TAKE_PROFIT_MARKET',
                stopPrice=round(tp_price, PRICE_PRECISION),
                closePosition=True,
                timeInForce='GTC'
            )
            self.tp_order_id = tp['orderId']
            print(f"✅ TP placed successfully! ID: {tp['orderId']}, Price: {tp_price:.2f}")
            sys.stdout.flush()
        except Exception as e:
            print(f"❌ TP order error: {e}")
            sys.stdout.flush()
            
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
            # Order might already be filled, that's okay
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
            
            # Cancel old SL order
            if self.sl_order_id:
                await self.cancel_order(self.sl_order_id)
            
            # Place new SL order
            close_side = 'SELL' if side == 'BUY' else 'BUY'
            try:
                new_sl_order = await self.client.futures_create_order(
                    symbol=SYMBOL,
                    side=close_side,
                    type='STOP_MARKET',
                    stopPrice=round(new_sl, PRICE_PRECISION),
                    closePosition=True,
                    timeInForce='GTC'
                )
                self.sl_order_id = new_sl_order['orderId']
                print(f"✅ New SL placed at {new_sl:.2f}")
                sys.stdout.flush()
            except Exception as e:
                print(f"❌ Trailing SL order error: {e}")
                sys.stdout.flush()

        # Update position state
        self.active_position['highest_high'] = highest_high
        self.active_position['lowest_low'] = lowest_low
        self.active_position['breakeven_triggered'] = breakeven_triggered
        self.active_position['sl'] = new_sl

    async def check_position_status(self):
        if not self.active_position:
            return False
        
        try:
            pos_info = await self.client.futures_position_information(symbol=SYMBOL)
            for p in pos_info:
                amt = float(p['positionAmt'])
                if amt != 0:
                    # Position still open
                    unrealized_pnl = float(p['unRealizedProfit'])
                    if DEBUG_MODE:
                        print(f"📊 Position open: {amt} {SYMBOL}, Unrealized PnL: {unrealized_pnl:.2f} USDT")
                        sys.stdout.flush()
                    return True
            
            # Position is closed
            print("📴 Position closed! (SL/TP hit)")
            sys.stdout.flush()
            self.active_position = None
            
            # Clean up remaining orders
            if self.sl_order_id:
                await self.cancel_order(self.sl_order_id)
                self.sl_order_id = None
            if self.tp_order_id:
                await self.cancel_order(self.tp_order_id)
                self.tp_order_id = None
                
        except Exception as e:
            print(f"⚠️ Position check error: {e}")
            sys.stdout.flush()
        
        return False

    async def process_closed_candle(self, kline):
        try:
            candle_close_ts = kline['T']
            # Convert millisecond timestamp to localized UTC, then convert to NY time
            utc_time = datetime.fromtimestamp(candle_close_ts / 1000, tz=pytz.utc)
            ny_time = utc_time.astimezone(self.ny_tz)
            ny_date = ny_time.date()
            ny_hour = ny_time.hour
            ny_minute = ny_time.minute

            # Daily reset at new NY session
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

            # If we have an active position, manage it
            if self.active_position:
                await self.update_trailing_stop(
                    float(kline['h']), float(kline['l']), float(kline['c'])
                )
                await self.check_position_status()
                return

            # Detect OR candle (09:30 NY time)
            if not self.or_set:
                if ny_hour == NY_OPEN_HOUR and ny_minute == NY_OPEN_MINUTE:
                    self.or_high = float(kline['h'])
                    self.or_low = float(kline['l'])
                    self.or_set = True
                    print(f"🎯 Opening Range Set!")
                    print(f"   📈 OR High: {self.or_high:.2f}")
                    print(f"   📉 OR Low:  {self.or_low:.2f}")
                    sys.stdout.flush()
                return

            # Store candle for potential signal detection
            self.candles_today.append({
                'timestamp': candle_close_ts,
                'ny_time': ny_time,
                'open': float(kline['o']),
                'high': float(kline['h']),
                'low': float(kline['l']),
                'close': float(kline['c'])
            })

            # Check max trades per day
            if self.trades_taken_today >= MAX_TRADES_PER_DAY:
                return

            # Signal Detection
            last = self.candles_today[-1]
            close = last['close']
            high = last['high']
            low = last['low']
            candle_range_pct = ((high - low) / low) * 100

            # 1. Detect Breakouts
            # BUY Breakout check
            if close > self.or_high and not self.breakout_detected['BUY'] and not self.breakout_done['BUY']:
                if candle_range_pct >= BREAKOUT_PCT:
                    self.breakout_detected['BUY'] = True
                    print(f"📈 Breakout BUY Detected! Candle closed above OR High ({self.or_high:.2f}). Waiting for retest...")
                    sys.stdout.flush()

            # SELL Breakout check
            if close < self.or_low and not self.breakout_detected['SELL'] and not self.breakout_done['SELL']:
                if candle_range_pct >= BREAKOUT_PCT:
                    self.breakout_detected['SELL'] = True
                    print(f"📉 Breakout SELL Detected! Candle closed below OR Low ({self.or_low:.2f}). Waiting for retest...")
                    sys.stdout.flush()

            # 2. Detect Retests
            # BUY Retest check
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

            # SELL Retest check
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

        # Place market entry order
        order = await self.place_market_order(side, qty)
        if not order:
            print("❌ Entry order failed, trade aborted")
            sys.stdout.flush()
            return

        # Calculate SL and TP levels
        if side == 'BUY':
            stop = stop_level * (1 - SL_BUFFER_PCT/100)
            risk = entry_level - stop
            target = entry_level + risk * RISK_REWARD
        else:
            stop = stop_level * (1 + SL_BUFFER_PCT/100)
            risk = stop - entry_level
            target = entry_level - risk * RISK_REWARD

        print(f"\n📋 Trade Details:")
        print(f"   Side: {side}")
        print(f"   Entry: {entry_level:.2f}")
        print(f"   Stop Loss: {stop:.2f} (Risk: {risk:.2f})")
        print(f"   Take Profit: {target:.2f} (RR: 1:{RISK_REWARD})")
        sys.stdout.flush()

        # Place exit orders
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
            return

        # Track active position
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
            today_date = now_ny.date()
            self.today = today_date
            
            today_candles = []
            or_candle = None
            
            for k in klines:
                close_time_ms = k[6]
                utc_time = datetime.fromtimestamp(close_time_ms / 1000, tz=pytz.utc)
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
                
                for o in open_orders:
                    o_type = o['type']
                    o_side = o['side']
                    expected_exit_side = 'SELL' if side == 'BUY' else 'BUY'
                    
                    if o_side == expected_exit_side:
                        if o_type == 'STOP_MARKET':
                            self.sl_order_id = o['orderId']
                            sl_price = float(o['stopPrice'])
                        elif o_type == 'TAKE_PROFIT_MARKET':
                            self.tp_order_id = o['orderId']
                            tp_price = float(o['stopPrice'])
                
                print(f"   🛑 Sync SL: OrderID={self.sl_order_id}, Price={sl_price if sl_price else 'N/A'}")
                print(f"   🎯 Sync TP: OrderID={self.tp_order_id}, Price={tp_price if tp_price else 'N/A'}")
                sys.stdout.flush()
                
                self.active_position = {
                    'side': side,
                    'entry': entry_price,
                    'sl': sl_price if sl_price else entry_price,
                    'tp': tp_price if tp_price else entry_price,
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
            
            now_utc = datetime.utcnow()
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

            # Mask keys for security
            masked_api = API_KEY[:4] + "****" + API_KEY[-4:] if len(API_KEY) > 8 else "****"
            print(f"🔑 API Key: {masked_api}")
            sys.stdout.flush()

            print("🔌 Connecting to Binance Futures Testnet...")
            sys.stdout.flush()
            
            self.client = await AsyncClient.create(API_KEY, SECRET_KEY, testnet=True)
            print("✅ Connected to Binance Testnet successfully!")
            sys.stdout.flush()

            # Set leverage
            try:
                print(f"⚙️ Setting leverage to {LEVERAGE}x for {SYMBOL}...")
                sys.stdout.flush()
                response = await self.client.futures_change_leverage(symbol=SYMBOL, leverage=LEVERAGE)
                print(f"✅ Leverage set to {LEVERAGE}x")
                sys.stdout.flush()
            except Exception as e:
                print(f"⚠️ Leverage setting warning: {e}")
                sys.stdout.flush()

            # Run startup recoveries
            await self.recover_opening_range()
            await self.recover_active_position()
            await self.recover_trade_count()

            # Get account info
            balance = await self.get_usdt_balance()
            print(f"💰 Account Balance: {balance:.2f} USDT")
            sys.stdout.flush()

            print(f"📡 Starting WebSocket stream for {SYMBOL} {INTERVAL}...")
            sys.stdout.flush()
            
            self.bm = BinanceSocketManager(self.client)
            stream = self.bm.kline_futures_socket(SYMBOL, interval=INTERVAL)
            
            print("✅ Stream connected! Waiting for NY Session OR candle...")
            print(f"⏰ NY Session: {NY_OPEN_HOUR:02d}:{NY_OPEN_MINUTE:02d} {NY_TIMEZONE}")
            print("="*50 + "\n")
            sys.stdout.flush()

            async with stream as s:
                while True:
                    try:
                        msg = await s.recv()
                        if msg and msg.get('e') == 'kline':
                            kline = msg['k']
                            if kline['x']:  # Candle closed
                                await self.process_closed_candle(kline)
                    except asyncio.CancelledError:
                        print("🛑 Bot shutdown requested")
                        sys.stdout.flush()
                        break
                    except Exception as e:
                        print(f"⚠️ WebSocket error: {e}")
                        print("🔄 Reconnecting in 5 seconds...")
                        sys.stdout.flush()
                        await asyncio.sleep(5)
                        
                        # Reconnect
                        try:
                            await self.client.close_connection()
                            self.client = await AsyncClient.create(API_KEY, SECRET_KEY, testnet=True)
                            self.bm = BinanceSocketManager(self.client)
                            stream = self.bm.kline_futures_socket(SYMBOL, interval=INTERVAL)
                            print("✅ Reconnected successfully!")
                            sys.stdout.flush()
                        except Exception as reconnect_error:
                            print(f"❌ Reconnection failed: {reconnect_error}")
                            sys.stdout.flush()
                            await asyncio.sleep(10)
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
        # Keep alive for debugging
        import time
        while True:
            time.sleep(60)
