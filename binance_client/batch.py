import os
import sys
from pathlib import Path

# Add parent directory to path to allow imports when running directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from binance.client import Client
from time_client.last_timestamp import get_current_utc_time,get_last_timestamp

load_dotenv()


client = Client(os.getenv("API_KEY"), os.getenv("SECRET_KEY"), tld='us')

def get_cryptocurrency_data(symbol, interval, start_str):
    klines = client.get_historical_klines(symbol, interval, start_str)
    return klines

def transform_data(klines):
    transformed_data = []
    for kline in klines:
        transformed_data.append({
            "open_time": kline[0],
            "open": kline[1],
            "high": kline[2],
            "low": kline[3],
            "close": kline[4],
            "volume": kline[5],
            "close_time": kline[6],
            "quote_asset_volume": kline[7],
            "number_of_trades": kline[8],
        })
    return transformed_data

def ingest_bitcoin_data(interval="1m"):
    current_time = get_current_utc_time()
    last_timestamp = get_last_timestamp("binance-bitcoin")
    print(f"Fetching binance data at {current_time}...")
    klines =  get_cryptocurrency_data("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, last_timestamp)
    transformed_data = transform_data(klines)
    metadata={
      "source": "binance api",
      "type": "batch",
      "symbol": "BTCUSDT",
      "granularity": interval,
      "timestamp": current_time}
    path=f"binance/batch/bitcoin/{current_time}.json"
    return {
        "data": transformed_data,
        "metadata": metadata,
        "path": path,
        "timestamp": current_time,
        "length": len(transformed_data)
    }

# get_bitcoin_data()