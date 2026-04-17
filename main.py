from minio_client.minio import add_bucket, save_to_minio
from binance_client.batch import get_bitcoin_data
from yfinance_client.batch import get_gold_data,get_oil_data
from news_client.batch import get_newsapi_data
from concurrent.futures import ThreadPoolExecutor, as_completed
from time_client.last_timestamp import get_current_utc_time, update_state
from datetime import datetime
from minio_client.minio import save_to_minio
from db.session_db import get_session
from db.schemas.ingestion_task import IngestionTask
from db.schemas.ingestion_run import IngestionRun


def run_task(task_name, func,run_id,bucket_name):
    session = get_session()
    start_time = datetime.utcnow()
    error_message = None
    try:
        print(f"Starting {task_name}")
        result = func()
        records_count = len(result.get("data", [])) if result else 0

        
        print(f"{task_name} SUCCESS at {get_current_utc_time()}")
        status="SUCCESS"
    except Exception as e:
        print(f"{task_name} FAILED at {get_current_utc_time()}:", e)

        status = "FAILED"
        records_count = 0
        error_message = str(e)
    finally:
        end_time = datetime.utcnow()

        task = IngestionTask(
            run_id=run_id,
            source=task_name,
            status=status,
            start_time=start_time,
            end_time=end_time,
            records_count=records_count,
            last_timestamp=result.get("timestamp") if result else None,
            error_message=error_message if status == "FAILED" else None
        )
        if (status=="SUCCESS"):
            save_to_minio(bucket_name, result.get("path"), result.get("data"), metadata=result.get("metadata"))
            update_state(task_name, result.get("timestamp"))
        session.add(task)
        session.commit()
        session.close()


def finalize_run(run_id):
    session = get_session()

    tasks = session.query(IngestionTask).filter_by(run_id=run_id).all()

    total_tasks = len(tasks)
    success_tasks = sum(1 for t in tasks if t.status == "SUCCESS")
    failed_tasks = sum(1 for t in tasks if t.status == "FAILED")

    if failed_tasks == 0:
        status = "SUCCESS"
    elif success_tasks == 0:
        status = "FAILED"
    else:
        status = "PARTIAL"

    run = session.query(IngestionRun).filter_by(run_id=run_id).first()

    run.end_time = datetime.utcnow()
    run.status = status
    run.total_tasks = total_tasks
    run.success_tasks = success_tasks
    run.failed_tasks = failed_tasks

    session.commit()
    session.close()
    print(f"Run {run_id} finished with status {status}")

def get_batch_data(bucket_name):
    add_bucket(bucket_name)
    session = get_session()

    run = IngestionRun(
        start_time=datetime.utcnow(),
        status="RUNNING"
    )

    session.add(run)
    session.commit()

    run_id = run.run_id
    session.close()
    tasks = [
    ("newsapi-arabic", lambda: get_newsapi_data(), run_id, bucket_name),
    ("yfinance-gold", lambda: get_gold_data(), run_id, bucket_name),
    ("yfinance-oil", lambda: get_oil_data(), run_id, bucket_name),
    ("binance-bitcoin", lambda: get_bitcoin_data(), run_id, bucket_name),
    ]
    

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_task, name, func, run_id, bucket_name) for name, func, run_id, bucket_name in tasks]
        for future in as_completed(futures):
            future.result()
     
    finalize_run(run_id)

get_batch_data("raw-data-staging")
