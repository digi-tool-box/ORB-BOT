import pandas as pd
import numpy as np
import pytz
from config import (NY_TIMEZONE, NY_OPEN_HOUR, NY_OPEN_MINUTE,
                    BREAKOUT_PCT, RETEST_ZONE_PCT, RISK_REWARD, SL_BUFFER_PCT, DEBUG_MODE)

def add_ny_session_column(df):
    ny_tz = pytz.timezone(NY_TIMEZONE)
    df = df.copy()
    
    # Index ko datetime mein badlein agar Binance se wo string/int form mein aaya hai
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, unit='ms' if df.index.dtype in [np.int64, np.float64] else None)
        
    # Binance data pehle se UTC hota hai. Agar pehle se localized hai toh convert karein, nahi toh localize karein.
    if df.index.tz is None:
        df['dt_ny'] = df.index.tz_localize('UTC').tz_convert(ny_tz)
    else:
        df['dt_ny'] = df.index.tz_convert(ny_tz)
        
    df['ny_date'] = df['dt_ny'].dt.date
    return df

def find_or_candle(df):
    df = add_ny_session_column(df)
    or_candles = []
    for date, group in df.groupby('ny_date'):
        mask = (group['dt_ny'].dt.hour == NY_OPEN_HOUR) & \
               (group['dt_ny'].dt.minute == NY_OPEN_MINUTE)
        if mask.any():
            # FIXED: .iloc[0] ko sahi kiya taaki poori row sample mil sake
            row = group[mask].iloc[0].copy()
            row['or_time_index'] = group[mask].index[0] # Original Index capture karne ke liye
            or_candles.append(row)
            
    if not or_candles:
        if DEBUG_MODE:
            print("[DEBUG] Koi bhi Opening Range (OR) candle nahi mili! Apni NY_OPEN_HOUR settings check karein.")
        return pd.DataFrame()
        
    return pd.DataFrame(or_candles)

def detect_signals(df, or_candles):
    """Maximum two trades per day – first two valid retests win."""
    if or_candles.empty:
        return pd.DataFrame()
        
    df = add_ny_session_column(df)
    signals = []

    for _, or_row in or_candles.iterrows():
        or_high = or_row['high']
        or_low = or_row['low']
        or_time = or_row['or_time_index']
        or_date = or_row['ny_date']

        post_or = df[(df['ny_date'] == or_date) & (df.index > or_time)]
        if post_or.empty:
            continue

        trades_today = 0
        i = 0
        # Post_or dataframe ki bars ko loop karenge
        while i < len(post_or) and trades_today < 2:
            candle = post_or.iloc[i]
            trade_taken_this_candle = False

            # BUY SETUP
            if candle['close'] > or_high:
                candle_range_pct = ((candle['high'] - candle['low']) / candle['low']) * 100
                if candle_range_pct >= BREAKOUT_PCT:
                    retest_zone_upper = or_high * (1 + RETEST_ZONE_PCT/100)
                    retest_zone_lower = or_high * (1 - RETEST_ZONE_PCT/100)
                    
                    retest_candles = post_or.iloc[i+1:]
                    for r_idx, r_candle in retest_candles.iterrows():
                        if r_candle['low'] <= retest_zone_upper and r_candle['high'] >= retest_zone_lower:
                            entry = or_high
                            stop = or_low * (1 - SL_BUFFER_PCT/100)
                            risk = entry - stop
                            target = entry + risk * RISK_REWARD
                            
                            if DEBUG_MODE:
                                print(f"[DEBUG] BUY retest candle: {r_idx}, O:{r_candle['open']}, H:{r_candle['high']}, L:{r_candle['low']}, C:{r_candle['close']}")
                            
                            signals.append({
                                'entry_time': r_idx,
                                'type': 'BUY',
                                'entry': entry,
                                'stop': stop,
                                'target': target,
                                'or_date': or_date,
                                'or_high': or_high,
                                'or_low': or_low
                            })
                            trades_today += 1
                            trade_taken_this_candle = True
                            
                            # Agla candle index wahan set karein jahan trade execute hua hai
                            i = post_or.index.get_loc(r_idx)
                            break
                    
                    if trade_taken_this_candle:
                        continue 

            # SELL SETUP
            if not trade_taken_this_candle and candle['close'] < or_low:
                candle_range_pct = ((candle['high'] - candle['low']) / candle['low']) * 100
                if candle_range_pct >= BREAKOUT_PCT:
                    retest_zone_upper = or_low * (1 + RETEST_ZONE_PCT/100)
                    retest_zone_lower = or_low * (1 - RETEST_ZONE_PCT/100)
                    
                    retest_candles = post_or.iloc[i+1:]
                    for r_idx, r_candle in retest_candles.iterrows():
                        if r_candle['high'] >= retest_zone_lower and r_candle['low'] <= retest_zone_upper:
                            entry = or_low
                            stop = or_high * (1 + SL_BUFFER_PCT/100)
                            risk = stop - entry
                            target = entry - risk * RISK_REWARD
                            
                            if DEBUG_MODE:
                                print(f"[DEBUG] SELL retest candle: {r_idx}, O:{r_candle['open']}, H:{r_candle['high']}, L:{r_candle['low']}, C:{r_candle['close']}")
                            
                            signals.append({
                                'entry_time': r_idx,
                                'type': 'SELL',
                                'entry': entry,
                                'stop': stop,
                                'target': target,
                                'or_date': or_date,
                                'or_high': or_high,
                                'or_low': or_low
                            })
                            trades_today += 1
                            trade_taken_this_candle = True
                            
                            i = post_or.index.get_loc(r_idx)
                            break
                    
                    if trade_taken_this_candle:
                        continue

            i += 1

    return pd.DataFrame(signals)
