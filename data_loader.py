import pandas as pd
from binance.client import Client
from config import API_KEY, SECRET_KEY, SYMBOL, INTERVAL, START_DATE, END_DATE

client = Client(API_KEY, SECRET_KEY, tld='com')

def fetch_klines(start_str=None, end_str=None):
    """Download Binance Futures klines, return DataFrame."""
    klines = client.futures_historical_klines(
        symbol=SYMBOL,
        interval=INTERVAL,
        start_str=start_str,
        end_str=end_str
    )
    df = pd.DataFrame(klines, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'number_of_trades',
        'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
    ])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    for col in ['open', 'high', 'low', 'close', 'volume']:
        df[col] = df[col].astype(float)
    df.set_index('open_time', inplace=True)
    return df

if __name__ == "__main__":
    df = fetch_klines(START_DATE, END_DATE)
    print(f"Downloaded {len(df)} candles")
    print(df.head())