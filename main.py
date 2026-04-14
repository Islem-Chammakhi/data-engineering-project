from minio_client.minio import add_bucket
from binance_client.batch import save_bitcoin_data
from yfinance_client.batch import save_gold_data,save_oil_data
from news_client.batch import save_newsapi_data
from concurrent.futures import ThreadPoolExecutor, as_completed
from last_timestamp.last_timestamp import get_current_utc_time

def run_task(task_name, func):
    try:
        print(f"Starting {task_name}")
        func()
        print(f"{task_name} SUCCESS at {get_current_utc_time()}")
    except Exception as e:
        print(f"{task_name} FAILED at {get_current_utc_time()}:", e)


def get_batch_data(bucket_name):
    add_bucket(bucket_name)
    tasks = [
    ("news", lambda: save_newsapi_data(bucket_name)),
    ("gold", lambda: save_gold_data(bucket_name)),
    ("oil", lambda: save_oil_data(bucket_name)),
    ("btc", lambda: save_bitcoin_data(bucket_name)),
    ]

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_task, name, func) for name, func in tasks]
        for future in as_completed(futures):
            future.result() 

get_batch_data("raw-data-staging")
