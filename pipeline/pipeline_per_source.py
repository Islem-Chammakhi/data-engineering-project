
from prefect import task,flow
from ingestion.ingestion import run_ingestion
from utils.minio_client.minio import migrate_to_historical
from transformation.tansformation import run_transformation

@flow
def pipeline_per_source(source_name, ingest_fn ,staging_bucket):

    # 1. ingestion
    result = run_ingestion(
        source_name,
        ingest_fn,
        staging_bucket
    )

    if result["status"] != "SUCCESS":
        print("pipeline "+source_name+" teblouka fil transformation")
        raise Exception(f"Ingestion failed for {source_name}")


    # 2. transformation
    transform_result = run_transformation(source_name)

    if transform_result["status"] != "SUCCESS":
        print("pipeline "+source_name+" teblouka fil ingestion")
        raise Exception(f"Transformation failed for {source_name}")

    # 3. move raw → history
    staging_path=result["path"]
    result = migrate_to_historical(
        "raw-data-staging",
        "raw-data",
        staging_path
    )
    if(not result):
        print("pipeline "+source_name+" teblouka fil migration lil bucket te3 historique")
        raise Exception(f"moving failed for {source_name}")
    return "pipeline " + source_name +" 5dem jawou gazouz"