import yfinance as yf
from last_timestamp.last_timestamp import get_current_utc_time, update_state,get_last_timestamp
from minio_client.minio import save_to_minio

def get_commodity_data(symbol,start_str, interval="1m", end_str=None):
    
    ticker= yf.Ticker(symbol)
    df = ticker.history(interval=interval, start=start_str, end=end_str)
    df = df.reset_index()
    df["Datetime"] = df["Datetime"].astype(str)
    return df.reset_index().to_dict(orient="records")



def save_gold_data(bucket_name,interval="1m"):
    current_time = get_current_utc_time()
    last_timestamp = get_last_timestamp("yfinance-gold").split("T")[0]
    print(f"Fetching yfinance data at {current_time}...")
    data= get_commodity_data("GC=F", start_str=last_timestamp, interval=interval)
    metadata= {
    "source": "yfinance api",
    "type": "batch",
    "symbol": "GC=F",
    "granularity":interval,
    "timestamp": current_time}
    path=f"yfinance/batch/gold/{current_time}.json"
    save_to_minio(bucket_name, path, data, metadata=metadata)
    update_state("yfinance-gold", current_time)
    



def save_oil_data(bucket_name,interval="1m"):
    current_time = get_current_utc_time()
    last_timestamp = get_last_timestamp("yfinance-oil").split("T")[0]
    print(f"Fetching yfinance data at {current_time}...")
    data= get_commodity_data("CL=F", start_str=last_timestamp, interval=interval)
    metadata= {
    "source": "yfinance api",
    "type": "batch",
    "symbol": "CL=F",
    "granularity":interval,
    "timestamp": current_time}
    path=f"yfinance/batch/oil/{current_time}.json"
    save_to_minio(bucket_name, path, data, metadata=metadata)
    update_state("yfinance-oil", current_time)
