import asyncio
import os
import pandas as pd
import pytz
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv
from keep_alive import keep_alive
import asyncio

# Local development ke liye .env file load karega (Render par ye line safe rahegi)
load_dotenv()

# --- IMPORT CONFIG ---
# Humne config se API_KEY aur SECRET_KEY ka direct import hata diya hai 
# taaki Render ke variables override na hon.
from config import (
    SYMBOL, 
    INTERVAL, 
    NY_TIMEZONE, 
    NY_OPEN_HOUR, 
    NY_OPEN_MINUTE, 
    SLIPPAGE_PCT, 
    RISK_PER_TRADE_PCT, 
    INITIAL_CAPITAL
)

# ─── RENDER ENVIRONMENT VARIABLES FETCH ──────────────────────────────────
# Sabse pehle Render/OS ke environment variables check honge.
API_KEY = os.environ.get('API_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

# Agar Render par nahi hain aur local testing kar rahe hain, to config file se backup uthayega.
if not API_KEY or not SECRET_KEY:
    try:
        from config import API_KEY as CONFIG_API_KEY, SECRET_KEY as CONFIG_SECRET_KEY
        API_KEY = API_KEY or CONFIG_API_KEY
        SECRET_KEY = SECRET_KEY or CONFIG_SECRET_KEY
    except ImportError:
        pass
# ────────────────────────────────────────────────────────────────────────

class LiveORBSignals:
    def __init__(self):
        self.client = None
        self.bm = None
        self.or_high = None
        self.or_low = None
        self.or_set = False
        self.ny_tz = pytz.timezone(NY_TIMEZONE)
        self.today = None
        self.breakout_done = {'BUY': False, 'SELL': False}
        self.retest_done = {'BUY': False, 'SELL': False}
        self.trade_taken = False
        self.position = None

    def calculate_quantity(self, entry, stop, side):
        slippage_amount = entry * (SLIPPAGE_PCT / 100)
        if side == 'BUY':
            effective_entry = entry + slippage_amount
        else:
            effective_entry = entry - slippage_amount
            
        equity = INITIAL_CAPITAL 
        risk_amount = equity * (RISK_PER_TRADE_PCT / 100)
        risk_per_unit = abs(effective_entry - stop)
        
        if risk_per_unit == 0:
            return 0, effective_entry
            
        quantity = risk_amount / risk_per_unit
        return round(quantity, 3), effective_entry

    async def place_order(self, side, quantity):
        try:
            print(f"🚀 Sending {side} order for {quantity} units to Binance...")
            order = await self.client.futures_create_order(
                symbol=SYMBOL,
                side=side,
                type='MARKET',
                quantity=quantity
            )
            print(f"✅ Order Placed Successfully. OrderID: {order.get('orderId')}")
            return order
        except Exception as e:
            print(f"❌ Order Placement Error: {e}")
            return None

    async def start(self):
        # API keys validation log
        if not API_KEY or not SECRET_KEY:
            print("❌ Error: API_KEY or SECRET_KEY missing! Render ke Environment Variables check karein.")
            return

        print("🔌 Connecting to Binance WebSocket...")
        # Note: Agar live trading karni hai to testnet=False ya remove kar dena.
        self.client = await AsyncClient.create(API_KEY, SECRET_KEY, testnet=True)
        self.bm = BinanceSocketManager(self.client)
        stream = self.bm.kline_futures_socket(SYMBOL, interval=INTERVAL)
        print(f"✅ Connected successfully using Render Keys. Monitoring {SYMBOL}...")
        
        try:
            async with stream as s:
                while True:
                    msg = await s.recv()
                    if msg and msg.get('e') == 'kline':
                        kline = msg['k']
                        self.monitor_live_position(kline)
                        if kline.get('x'):
                            await self.process_closed_candle(kline)
        except asyncio.CancelledError:
            print("🛑 Task cancelled, closing connections...")
        finally:
            await self.client.close_connection()
            print("🔌 Connection Closed Cleanly.")

    def monitor_live_position(self, kline):
        if self.position is None: return
        current_price = float(kline['c'])

    async def process_closed_candle(self, kline):
        signal_buy = False
        signal_sell = False
        current_close = float(kline['c'])
        
        # --- AUTOMATED BUY EXECUTION ---
        if signal_buy and not self.trade_taken:
            if self.or_low is None:
                print("⚠️ Warning: self.or_low is not set yet.")
                return
            
            entry_level = current_close
            stop_level = self.or_low  
            qty, effective_price = self.calculate_quantity(entry_level, stop_level, 'BUY')
            
            if qty > 0:
                order_status = await self.place_order('BUY', qty)
                if order_status:
                    self.trade_taken = True
                    self.position = 'BUY'
                
        # --- AUTOMATED SELL EXECUTION ---
        elif signal_sell and not self.trade_taken:
            if self.or_high is None:
                print("⚠️ Warning: self.or_high is not set yet.")
                return
                
            entry_level = current_close
            stop_level = self.or_high  
            qty, effective_price = self.calculate_quantity(entry_level, stop_level, 'SELL')
            
            if qty > 0:
                order_status = await self.place_order('SELL', qty)
                if order_status:
                    self.trade_taken = True
                    self.position = 'SELL'

if __name__ == "__main__":
    try:
        bot = LiveORBSignals()
        asyncio.run(bot.start())
    except KeyboardInterrupt:
        print("\n👋 Bot stopped manually.")
        keep_alive()
