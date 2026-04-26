from prefect import task 

@task(retries=3, retry_delay_seconds=10)
def load_data():
    import subprocess
    try:
        
        subprocess.run([
            "docker",
            "exec",
            "spark-client",
            "spark-submit",
            "--master", "spark://spark-master:7077",
            "--packages","org.postgresql:postgresql:42.7.3",
            "./jobs/silver_to_gold/aggregate.py"
        ], check=True)
        return {"status": "SUCCESS"}

    except Exception as e:
        raise Exception(f"load failed : {str(e)}")