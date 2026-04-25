from utils.time_client.last_timestamp import  update_state
from utils.minio_client.minio import save_to_minio
from db.services.pipeline_service import log_task
from prefect import task
from datetime import datetime

@task(retries=3, retry_delay_seconds=10)
def run_ingestion(source_name, func, staging_bucket, run_id):
    start_time = datetime.utcnow()
    try:
        print(f"Starting {source_name}")

        result = func()
        records_count = result.get("length", 0)

        if records_count == 0:
            raise ValueError(f"No records ingested for {source_name}")

        print(f"{source_name} SUCCESS with {records_count} records")

        # ! seulement ici on sauvegarde
        save_to_minio(
            staging_bucket,
            result.get("path"),
            result.get("data"),
            metadata=result.get("metadata")
        )
        time= result.get("timestamp").split("T")[1].replace("-", ":")
        date= result.get("timestamp").split("T")[0]
        full_date= date+"T"+time
        update_state(source_name, full_date)
        log_task(run_id, "ingestion", "SUCCESS", start_time, records_count)
        return {
            "status": "SUCCESS",
            "path": result.get("path")
        }

    except Exception as e:
        log_task(run_id, "ingestion", "FAILED", start_time, 0, str(e))
        # ! important : Prefect doit voir l’erreur
        raise Exception(f"Ingestion failed for {source_name}: {str(e)}")

