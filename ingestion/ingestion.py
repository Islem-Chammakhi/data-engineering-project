from utils.time_client.last_timestamp import get_current_utc_time, update_state
from datetime import datetime
from utils.minio_client.minio import save_to_minio
from db.session_db import get_session
from db.schemas.ingestion_task import IngestionTask
from db.schemas.ingestion_run import IngestionRun
from prefect import task

# @task(retries=3, retry_delay_seconds=10)
# def run_ingestion(source_name, func,staging_bucket):
#     # session = get_session()
#     # start_time = datetime.utcnow()
#     # error_message = None
#     try:
#         print(f"Starting {source_name}")
#         result = func()
#         records_count = result.get("length", 0)

        
#         print(f"{source_name} SUCCESS at {get_current_utc_time()}")
#         status="SUCCESS"
#     except Exception as e:
#         print(f"{source_name} FAILED at {get_current_utc_time()}:", e)

#         status = "FAILED"
#         records_count = 0
#         error_message = str(e)
#     finally:
#         end_time = datetime.utcnow()

#         # task = IngestionTask(
#         #     run_id=run_id,
#         #     source=source_name,
#         #     status=status,
#         #     start_time=start_time,
#         #     end_time=end_time,
#         #     records_count=records_count,
#         #     last_timestamp=result.get("timestamp") if result else None,
#         #     error_message=error_message if status == "FAILED" else None
#         # )
#         if (status=="SUCCESS"):
#             save_to_minio(staging_bucket, result.get("path"), result.get("data"), metadata=result.get("metadata"))
#             update_state(source_name, result.get("timestamp"))
#         else : 
#             print("error happened :",error_message)
#         # session.add(task)
#         # session.commit()
#         # session.close()

#     return {
#     "status": status,
#     "path": result.get("path") if result else None
#     }

@task(retries=3, retry_delay_seconds=10)
def run_ingestion(source_name, func, staging_bucket):

    result = None
    error_message = None
    status = None
    records_count = 0

    try:
        print(f"Starting {source_name}")

        result = func()
        records_count = result.get("length", 0)

        print(f"{source_name} SUCCESS at {get_current_utc_time()}")
        status = "SUCCESS"

    except Exception as e:
        print(f"{source_name} FAILED at {get_current_utc_time()}:", e)

        status = "FAILED"
        error_message = str(e)

    finally:
        end_time = datetime.utcnow()

        if status == "SUCCESS" and result:
            save_to_minio(
                staging_bucket,
                result.get("path"),
                result.get("data"),
                metadata=result.get("metadata")
            )
            update_state(source_name, result.get("timestamp"))

        else:
            print("error happened :", error_message)

    return {
        "status": status,
        "path": result.get("path") if result else None
    }

# @task(retries=3, retry_delay_seconds=10)
# def finalize_run(run_id):
#     session = get_session()

#     tasks = session.query(IngestionTask).filter_by(run_id=run_id).all()

#     total_tasks = len(tasks)
#     success_tasks = sum(1 for t in tasks if t.status == "SUCCESS")
#     failed_tasks = sum(1 for t in tasks if t.status == "FAILED")

#     if failed_tasks == 0:
#         status = "SUCCESS"
#     elif success_tasks == 0:
#         status = "FAILED"
#     else:
#         status = "PARTIAL"

#     run = session.query(IngestionRun).filter_by(run_id=run_id).first()

#     run.end_time = datetime.utcnow()
#     run.status = status
#     run.total_tasks = total_tasks
#     run.success_tasks = success_tasks
#     run.failed_tasks = failed_tasks

#     session.commit()
#     session.close()
#     print(f"Run {run_id} finisheds with status {status}")


# @flow(name="market-news-ingestion")
# def get_batch_data(staging_bucket):
#     add_bucket(staging_bucket)

#     session = get_session()

#     run = IngestionRun(
#         start_time=datetime.utcnow(),
#         status="RUNNING"
#     )

#     session.add(run)
#     session.commit()

#     run_id = run.run_id
#     session.close()

#     #  Prefect parallel execution
#     futures = [
#         run_task.submit("newsapi-arabic", ingest_newsapi_data, run_id, staging_bucket),
#         run_task.submit("yfinance-gold", ingest_gold_data, run_id, staging_bucket),
#         run_task.submit("yfinance-oil", ingest_oil_data, run_id, staging_bucket),
#         run_task.submit("binance-bitcoin", ingest_bitcoin_data, run_id, staging_bucket),
#     ]

#     # attendre toutes les tasks
#     ingestion_results  = [f.result() for f in futures]
    
#     # 2. vérifier succès global
#     success_paths = [
#         r["path"] for r in ingestion_results
#         if r["status"] == "SUCCESS" and r["path"]
#     ]

#     if not success_paths:
#         raise Exception("No successful ingestion → skip transformation")
    
#     # transformation_results=[for path in  success_paths : run_transformation(path)]
#     #  task finale
#     finalize_run.submit(run_id)

# get_batch_data("raw-data-staging")
