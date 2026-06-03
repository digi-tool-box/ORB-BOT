import os
from dotenv import load_dotenv

load_dotenv()

# === API credentials ===
API_KEY = os.getenv("BINANCE_API_KEY")
SECRET_KEY = os.getenv("BINANCE_SECRET_KEY")

# === Trading pair & Leverage ===
SYMBOL = "BTCUSDT"
INTERVAL = "5m"
LEVERAGE = 5                # <--- Yahan set kiya hai (Testing ke liye 5x safe aur best hai)

# === Backtest period (UTC) ===
START_DATE = "2025-05-01"
END_DATE   = "2026-05-21"

# === Strategy parameters ===
BREAKOUT_PCT = 0.5          # 0.5% candle range for valid breakout
RETEST_ZONE_PCT = 0.1       # ±0.1% around OR level
RISK_REWARD = 2.0           # 1:2
SL_BUFFER_PCT = 0.0         # extra buffer below/above OR for SL
SLIPPAGE_PCT = 0.05         # 0.05% slippage

# === NY session ===
NY_TIMEZONE = "America/New_York"
NY_OPEN_HOUR = 9
NY_OPEN_MINUTE = 30

# === Capital & Risk ===
INITIAL_CAPITAL = 100       # USD
RISK_PER_TRADE_PCT = 1.0    # risk 1% of capital per trade

# === LIVE TRADING INFRASTRUCTURE ===
IS_TESTNET = True           # True for Testnet (Safe Mode), False for Real Money
QUANTITY_PRECISION = 3      # Precision for BTC (e.g., 0.001)
PRICE_PRECISION = 1         # Precision for BTC price (e.g., 50000.00)

# === POSITION MANAGEMENT ===
# Partial Exit Strategy
TP1_PCT = 0.5               # 0.5% profit pe 50% position exit (1:1)
TP2_PCT = 1.0               # 1.0% profit pe remaining position exit (1:2)

# Risk Protection
BREAKEVEN_TRIGGER = 0.002   # Profit 0.2% reach hote hi SL ko entry pe shift karo
TRAIL_STEP_PCT = 0.001      # Trailing step 0.1%

# === DEBUG & LOGGING ===
DEBUG_MODE = True           # set False to silence debug prints
MAX_TRADES_PER_DAY = 2      # maximum number of trades allowed per day
LOG_FILE = "trading_bot.log" # File path for logs
