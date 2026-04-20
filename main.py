from minio_client.minio import add_bucket, save_to_minio
from binance_client.batch import ingest_bitcoin_data
from yfinance_client.batch import ingest_gold_data,ingest_oil_data
from news_client.batch import ingest_newsapi_data
from time_client.last_timestamp import get_current_utc_time, update_state
from datetime import datetime
from minio_client.minio import save_to_minio,migrate_to_historical
from db.session_db import get_session
from db.schemas.ingestion_task import IngestionTask
from db.schemas.ingestion_run import IngestionRun
from prefect import task,flow


@task(retries=3, retry_delay_seconds=10)
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

    return {
    "status": status,
    "path": result.get("path") if result else None
    }


@task(retries=3, retry_delay_seconds=10)
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


@flow(name="market-news-ingestion")
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

    #  Prefect parallel execution
    futures = [
        run_task.submit("newsapi-arabic", ingest_newsapi_data, run_id, bucket_name),
        run_task.submit("yfinance-gold", ingest_gold_data, run_id, bucket_name),
        run_task.submit("yfinance-oil", ingest_oil_data, run_id, bucket_name),
        run_task.submit("binance-bitcoin", ingest_bitcoin_data, run_id, bucket_name),
    ]

    # attendre toutes les tasks
    runs = [f.result() for f in futures]
    # results = [f.result() for f in runs]
    # for r in results:
    #     if r["status"] == "SUCCESS" and r["path"]:
    #         migrate_to_historical.submit(
    #             "raw-data-staging",
    #             "raw-data-historical",
    #             r["path"]
    #         )

    #  task finale
    finalize_run.submit(run_id)

get_batch_data("raw-data-staging")
