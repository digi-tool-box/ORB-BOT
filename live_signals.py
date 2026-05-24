import asyncio
import os
import pandas as pd
import pytz
from binance import AsyncClient, BinanceSocketManager
from dotenv import load_dotenv

# Load environmental variables from .env file
load_dotenv()

# --- IMPORT CONFIG ---
# Hum config se sirf strategy variables mangwa rahe hain. 
# API keys ko .env se handle karna best practice hai.
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

# API Keys fallback mechanism (Pehle .env check karega, nahi mila to config se)
try:
    from config import API_KEY as CONFIG_API_KEY, SECRET_KEY as CONFIG_SECRET_KEY
except ImportError:
    CONFIG_API_KEY, CONFIG_SECRET_KEY = None, None

API_KEY = os.environ.get('API_KEY') or CONFIG_API_KEY
SECRET_KEY = os.environ.get('SECRET_KEY') or CONFIG_SECRET_KEY

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
        # Slippage adjustment
        slippage_amount = entry * (SLIPPAGE_PCT / 100)
        
        if side == 'BUY':
            effective_entry = entry + slippage_amount
        else: # SELL
            effective_entry = entry - slippage_amount
            
        equity = INITIAL_CAPITAL 
        risk_amount = equity * (RISK_PER_TRADE_PCT / 100)
        risk_per_unit = abs(effective_entry - stop)
        
        # Zero division error safety check
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
        if not API_KEY or not SECRET_KEY:
            print("❌ Error: API_KEY or SECRET_KEY missing! Check .env or config.py")
            return

        print("🔌 Connecting to Binance WebSocket...")
        # Future trading ke liye testnet=True valid hai futures endpoints par
        self.client = await AsyncClient.create(API_KEY, SECRET_KEY, testnet=True)
        self.bm = BinanceSocketManager(self.client)
        stream = self.bm.kline_futures_socket(SYMBOL, interval=INTERVAL)
        print(f"✅ Connected. Waiting for NY session for {SYMBOL}...")
        
        try:
            async with stream as s:
                while True:
                    msg = await s.recv()
                    # Safe dictionary checking
                    if msg and msg.get('e') == 'kline':
                        kline = msg['k']
                        self.monitor_live_position(kline)
                        if kline.get('x'): # Agar candle close ho gayi hai
                            await self.process_closed_candle(kline)
        except asyncio.CancelledError:
            print("🛑 Task cancelled, closing connections...")
        finally:
            # Gracefully closing client session
            await self.client.close_connection()
            print("🔌 Connection Closed Cleanly.")

    def monitor_live_position(self, kline):
        if self.position is None: 
            return
        current_price = float(kline['c'])
        # Aapka existing trailing ya SL/TP logic yahan aayega...

    async def process_closed_candle(self, kline):
        signal_buy = False
        signal_sell = False
        current_close = float(kline['c'])
        
        # --- AAPKA BREAKOUT LOGIC YAHA AAYEGA ---
        # Note: self.or_low aur self.or_high check kar lena ki unme value hai ya nahi breakout logic chalne se pehle.
        
        # --- AUTOMATED BUY EXECUTION ---
        if signal_buy and not self.trade_taken:
            if self.or_low is None:
                print("⚠️ Warning: Cannot buy, self.or_low is not set yet.")
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
                print("⚠️ Warning: Cannot sell, self.or_high is not set yet.")
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
        print("\n👋 Bot stopped manually by user.")
