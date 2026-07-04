import pandas as pd
import time
from binance.client import Client
from config import API_KEY, SECRET_KEY, SYMBOL, INTERVAL, START_DATE, END_DATE

_client = None

def _get_client():
    global _client
    if _client is None:
        _client = Client(API_KEY, SECRET_KEY, tld='com')
    return _client

def fetch_klines(start_str=None, end_str=None):
    """Download Binance Futures klines with pagination, return DataFrame."""
    client = _get_client()
    all_klines = []
    current_start = start_str

    while True:
        klines = client.futures_historical_klines(
            symbol=SYMBOL,
            interval=INTERVAL,
            start_str=current_start,
            end_str=end_str,
            limit=1500
        )
        if not klines:
            break
        all_klines.extend(klines)
        if len(klines) < 1500:
            break
        # Move start to the last candle's open time + 1ms
        current_start = klines[-1][0] + 1
        time.sleep(0.2)

    if not all_klines:
        return pd.DataFrame()

    df = pd.DataFrame(all_klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    df = df.drop_duplicates(subset=['open_time'])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)
    df.set_index('open_time', inplace=True)
    return df

if __name__ == "__main__":
    df = fetch_klines(START_DATE, END_DATE)
    print(f"Downloaded {len(df)} candles")
    print(df.head())