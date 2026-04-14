import os
from dotenv import load_dotenv
from binance.client import Client
from minio_client.minio import save_to_minio
from last_timestamp.last_timestamp import get_current_utc_time,update_state,get_last_timestamp

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


def save_bitcoin_data(bucket_name,interval="1m"):
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
    save_to_minio(bucket_name,path , transformed_data,metadata=metadata)
    update_state("binance-bitcoin", current_time)