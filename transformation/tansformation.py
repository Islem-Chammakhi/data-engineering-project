from datetime import datetime
from prefect import task 
from db.services.pipeline_service import log_task

files_path={
    "newsapi-arabic":"news.py",
    "binance-bitcoin":"bitcoin.py",
    "yfinance-gold":"gold.py",
    "yfinance-oil":"oil.py"
}

@task(retries=3, retry_delay_seconds=10)
def run_transformation(source_name,run_id):
    import subprocess
    file_path = files_path.get(source_name)
    start_time = datetime.utcnow()
    try:
        records_count = 100  # exemple
        subprocess.run([
            "docker",
            "exec",
            "spark-client",
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "./jobs/"+file_path 
        ], check=True)
        log_task(run_id, "transformation", "SUCCESS", start_time, records_count)
        return {"status": "SUCCESS"}

    except Exception as e:
        log_task(run_id, "transformation", "FAILED", start_time, 0, str(e))
        raise Exception(f"transformation failed for {source_name}: {str(e)}")