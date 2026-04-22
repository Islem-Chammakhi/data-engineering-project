import sys
from pathlib import Path

# Add parent directory to path to allow imports when running directly
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.time_client.last_timestamp import get_current_utc_time,get_last_timestamp
import yfinance as yf


def get_commodity_data(symbol,start_str, interval="1m", end_str=None):
    
    ticker= yf.Ticker(symbol)
    df = ticker.history(interval=interval, start=start_str, end=end_str)
    df = df.reset_index()
    df["Datetime"] = df["Datetime"].astype(str)
    return df.reset_index().to_dict(orient="records")



def ingest_gold_data(interval="1m"):
    current_time = get_current_utc_time().replace(":","-")
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
    return {
    "data": data,
    "metadata": metadata,
    "path": path,
    "timestamp": current_time,
    "length": len(data)
    }
    


def ingest_oil_data(interval="1m"):
    current_time = get_current_utc_time().replace(":","-")
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
    return {
    "data": data,
    "metadata": metadata,
    "path": path,
    "timestamp": current_time,
    "length": len(data)
    }

