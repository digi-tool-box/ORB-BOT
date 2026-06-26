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
    INITIAL_CAPITAL, RISK_PER_TRADE_PCT, 
    SLIPPAGE_PCT
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

    # Parameters for Trailing and Breakeven
    breakeven_trigger_pct = 0.2 / 100  # 0.2% profit par breakeven activate hoga
    trailing_pct = 0.1 / 100           # 0.1% peeche trail karega

    for _, sig in signals.iterrows():
        entry = sig['entry']
        slippage = SLIPPAGE_PCT / 100
        
        # --- FIXED: Indentation theek kar di gayi hai aur 'entry' ko hi update kiya gaya hai ---
        if sig['type'] == 'BUY':
            entry = entry * (1 + slippage)   # buy mein thoda upar
        else:
            entry = entry * (1 - slippage)   # sell mein thoda neeche
            
        current_sl = sig['stop']  # Dynamic SL
        target = sig['target']
        
        risk_per_unit = abs(entry - current_sl)
        risk_amount = equity * (risk_pct / 100)
        qty = risk_amount / risk_per_unit if risk_per_unit > 0 else 0
        entry_time = sig['entry_time']
        
        post_entry = df[df.index >= entry_time]
        
        # Trailing tracking variables
        is_breakeven_hit = False
        highest_high = entry
        lowest_low = entry

        for idx, candle in post_entry.iterrows():
            if sig['type'] == 'BUY':
                # --- TRAILING & BREAKEVEN LOGIC FOR BUY ---
                if candle['high'] > highest_high:
                    highest_high = candle['high']

                # Check for Breakeven Trigger
                profit_pct = (highest_high - entry) / entry
                if profit_pct >= breakeven_trigger_pct and not is_breakeven_hit:
                    current_sl = entry
                    is_breakeven_hit = True

                # If Breakeven is hit, trail SL 0.1% behind highest high
                if is_breakeven_hit:
                    new_trail_sl = highest_high * (1 - trailing_pct)
                    if new_trail_sl > current_sl:
                        current_sl = new_trail_sl

                # --- EXIT CONDITIONS ---
                if candle['low'] <= current_sl:
                    pnl = (current_sl - entry) * qty
                    outcome = 'Breakeven/TSL' if is_breakeven_hit else 'SL'
                    trades.append({
                        'entry_time': entry_time,
                        'exit_time': idx,
                        'type': 'BUY',
                        'entry': entry,
                        'exit': current_sl,
                        'pnl': pnl,
                        'outcome': outcome,
                        'qty': qty
                    })
                    equity += pnl
                    break
                    
                if candle['high'] >= target:
                    pnl = (target - entry) * qty
                    trades.append({
                        'entry_time': entry_time,
                        'exit_time': idx,
                        'type': 'BUY',
                        'entry': entry,
                        'exit': target,
                        'pnl': pnl,
                        'outcome': 'TP',
                        'qty': qty
                    })
                    equity += pnl
                    break

            else:  # SELL SIGNALS
                # --- TRAILING & BREAKEVEN LOGIC FOR SELL ---
                if candle['low'] < lowest_low:
                    lowest_low = candle['low']

                # Check for Breakeven Trigger
                profit_pct = (entry - lowest_low) / entry
                if profit_pct >= breakeven_trigger_pct and not is_breakeven_hit:
                    current_sl = entry
                    is_breakeven_hit = True

                # If Breakeven is hit, trail SL 0.1% above lowest low
                if is_breakeven_hit:
                    new_trail_sl = lowest_low * (1 + trailing_pct)
                    if new_trail_sl < current_sl:
                        current_sl = new_trail_sl

                # --- EXIT CONDITIONS ---
                if candle['high'] >= current_sl:
                    pnl = (entry - current_sl) * qty
                    outcome = 'Breakeven/TSL' if is_breakeven_hit else 'SL'
                    trades.append({
                        'entry_time': entry_time,
                        'exit_time': idx,
                        'type': 'SELL',
                        'entry': entry,
                        'exit': current_sl,
                        'pnl': pnl,
                        'outcome': outcome,
                        'qty': qty
                    })
                    equity += pnl
                    break
                    
                if candle['low'] <= target:
                    pnl = (entry - target) * qty
                    trades.append({
                        'entry_time': entry_time,
                        'exit_time': idx,
                        'type': 'SELL',
                        'entry': entry,
                        'exit': target,
                        'pnl': pnl,
                        'outcome': 'TP',
                        'qty': qty
                    })
                    equity += pnl
                    break
                    
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