import pandas as pd
import numpy as np
import os
import pytz
from data_loader import fetch_klines
from strategy import add_ny_session_column, find_or_candle, detect_signals
from performance import calculate_metrics, plot_equity, export_trades, export_metrics

# Sabhi config variables yahan ek hi baar mein import kiye gaye hain
from config import (
    START_DATE, END_DATE, SYMBOL, INTERVAL, 
    INITIAL_CAPITAL, RISK_PER_TRADE_PCT, LEVERAGE,
    SLIPPAGE_PCT, MAKER_FEE, TAKER_FEE
)

def prepare_binance_data(df):
    """
    Binance ke raw data ko strategy ke liye sahi format mein clean aur convert karta hai.
    """
    if df is None or df.empty:
        return pd.DataFrame()
        
    df = df.copy()
    
    # 1. Column names ko lowercase (small letters) mein badlein (e.g., 'Close' -> 'close')
    df.columns = [col.lower() for col in df.columns]
    
    # 2. Agar 'open_time' ya 'timestamp' column hai aur wo index nahi hai, toh use index banayein
    for time_col in ['open_time', 'timestamp', 'time']:
        if time_col in df.columns:
            df.set_index(time_col, inplace=True)
            break

    # 3. Index ko proper Datetime format mein convert karein
    if not isinstance(df.index, pd.DatetimeIndex):
        # Binance milliseconds timestamp use karta hai (int64/float64)
        if df.index.dtype in [np.int64, np.float64, 'int64', 'float64']:
            df.index = pd.to_datetime(df.index, unit='ms')
        else:
            df.index = pd.to_datetime(df.index)

    # 4. Data columns ko strings se numbers (floats) mein badlein
    numeric_cols = ['open', 'high', 'low', 'close', 'volume']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    # Na/Null values wali rows ko saaf karein
    df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
    
    return df

def simulate_trades(df, signals, capital, risk_pct):
    """Candle-by-candle simulation with Trailing Stop Loss and Breakeven logic."""
    trades = []
    df = add_ny_session_column(df)
    equity = capital

    from config import BREAKEVEN_TRIGGER, TRAIL_STEP_PCT

    breakeven_trigger_pct = BREAKEVEN_TRIGGER
    trailing_pct = TRAIL_STEP_PCT

    for _, sig in signals.iterrows():
            entry = sig['entry']
            entry_time = sig['entry_time']
            base_stop = sig['stop']
            target = sig['target']

            side = sig['type']
            # Entry LIMIT price hi level hai; kho jao toh pura move karo
            if side == 'BUY':
                slippage_factor = (1 + SLIPPAGE_PCT / 100)
            else:
                slippage_factor = (1 - SLIPPAGE_PCT / 100)

            risk_per_unit = abs(entry - base_stop)
            risk_amount = equity * (risk_pct / 100)
            max_position_value = equity * LEVERAGE
            qty_risk = risk_amount / risk_per_unit if risk_per_unit > 0 else 0
            qty_margin = max_position_value / entry if entry > 0 else float('inf')
            qty = min(qty_risk, qty_margin)
            if qty <= 0:
                continue

            current_sl = base_stop
            filled = False
            highest_high = entry
            lowest_low = entry
            is_breakeven_hit = False
            entry_price = None
            exit_price = None
            outcome = None
            exit_time = None

            post_entry = df[df.index >= entry_time]

            for idx, candle in post_entry.iterrows():
                # REALISTIC ENTRY: limit sirf tab fill hoti hai jab price entry level touch kare
                if not filled:
                    if side == 'BUY':
                        fills_here = (candle['low'] <= entry * slippage_factor)
                    else:
                        fills_here = (candle['high'] >= entry * slippage_factor)
                    if fills_here:
                        filled = True
                        entry_price = entry * slippage_factor
                        highest_high = max(highest_high, candle['high'])
                        lowest_low = min(lowest_low, candle['low'])
                    else:
                        continue

                if side == 'BUY':
                    if candle['high'] > highest_high:
                        highest_high = candle['high']
                    profit_pct = (highest_high - entry_price) / entry_price
                    if profit_pct >= breakeven_trigger_pct and not is_breakeven_hit:
                        current_sl = entry_price
                        is_breakeven_hit = True
                    if is_breakeven_hit:
                        new_trail_sl = highest_high * (1 - trailing_pct)
                        if new_trail_sl > current_sl:
                            current_sl = new_trail_sl
                    # SL ko pehle check karte hain (conservative intrabar ordering)
                    if candle['low'] <= current_sl:
                        exit_price = current_sl
                        outcome = 'Breakeven/TSL' if is_breakeven_hit else 'SL'
                    elif candle['high'] >= target:
                        exit_price = target
                        outcome = 'TP'
                else:  # SELL
                    if candle['low'] < lowest_low:
                        lowest_low = candle['low']
                    profit_pct = (entry_price - lowest_low) / entry_price
                    if profit_pct >= breakeven_trigger_pct and not is_breakeven_hit:
                        current_sl = entry_price
                        is_breakeven_hit = True
                    if is_breakeven_hit:
                        new_trail_sl = lowest_low * (1 + trailing_pct)
                        if new_trail_sl < current_sl:
                            current_sl = new_trail_sl
                    if candle['high'] >= current_sl:
                        exit_price = current_sl
                        outcome = 'Breakeven/TSL' if is_breakeven_hit else 'SL'
                    elif candle['low'] <= target:
                        exit_price = target
                        outcome = 'TP'

                if outcome:
                    exit_time = idx
                    if side == 'BUY':
                        pnl = (exit_price - entry_price) * qty
                    else:
                        pnl = (entry_price - exit_price) * qty
                    maker_fee = entry_price * qty * (MAKER_FEE / 100)
                    taker_fee = exit_price * qty * (TAKER_FEE / 100)
                    total_fees = maker_fee + taker_fee
                    pnl -= total_fees
                    trades.append({
                        'entry_time': entry_time,
                        'exit_time': exit_time,
                        'type': side,
                        'entry': entry_price,
                        'exit': exit_price,
                        'pnl': pnl,
                        'fees': round(total_fees, 2),
                        'outcome': outcome,
                        'qty': qty
                    })
                    equity += pnl
                    break

            if not filled:
                # Limit order kabhi fill nahi hua (price retest par level touch nahi kiya)
                pass

    return pd.DataFrame(trades)

if __name__ == "__main__":
    print("Downloading data...")
    raw_df = fetch_klines(START_DATE, END_DATE)
    print(f"Raw data shape: {raw_df.shape}")

    # Processing Binance data format
    df = prepare_binance_data(raw_df)
    print(f"Formatted data shape: {df.shape}")

    if df.empty:
        print("[ERROR] Data format clean karne ke baad DataFrame empty ho gaya. Apne data columns check karein!")
    else:
        print("Detecting Opening Ranges...")
        or_candles = find_or_candle(df)
        print(f"Found {len(or_candles)} OR candles")

        print("Generating signals (Max 2 trades per day)...")
        signals = detect_signals(df, or_candles)
        print(f"Signals generated: {len(signals)}")

        if not signals.empty:
            print("Simulating trades with capital management...")
            trades = simulate_trades(df, signals,
                                     capital=INITIAL_CAPITAL,
                                     risk_pct=RISK_PER_TRADE_PCT)
            print(f"Trades executed: {len(trades)}")

            metrics = calculate_metrics(trades, initial_capital=INITIAL_CAPITAL)
            for k, v in metrics.items():
                if k != 'equity_curve':
                    print(f"{k}: {v}")

            os.makedirs("exports", exist_ok=True)
            export_trades(trades)
            export_metrics(metrics)
            if 'equity_curve' in metrics:
                plot_equity(metrics['equity_curve'], trades)
                print("Equity curve chart saved to exports/equity_curve.png")
        else:
            print("No signals found.")