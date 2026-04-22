from prefect import task 

files_path={
    "newsapi-arabic":"news.py",
    "binance-bitcoin":"bitcoin.py",
    "yfinance-gold":"gold.py",
    "yfinance-oil":"oil.py"
}

@task(retries=3, retry_delay_seconds=10)
def run_transformation(source_name):
    import subprocess
    file_path = files_path.get(source_name)
    try:
        subprocess.run([
            "docker",
            "exec",
            "spark-client",
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "./jobs/"+file_path 
        ], check=True)

        return {"status": "SUCCESS"}

    except Exception as e:
        return {"status": "FAILED", "error": str(e)}