
from prefect import flow,task
from ingestion.ingestion import run_ingestion
from utils.minio_client.minio import migrate_to_historical
from transformation.tansformation import run_transformation
from db.services.pipeline_service import create_pipeline_run, update_pipeline_run

@flow()
def pipeline_per_source(source_name, ingest_fn ,staging_bucket):
    run_id = create_pipeline_run(source_name)

    try :
            
        # 1. ingestion
        result = run_ingestion(
            source_name,
            ingest_fn,
            staging_bucket,
            run_id
        )

        if result["status"] != "SUCCESS":
            print(source_name+" cannot proced to the transformation ingestion fail")
            raise Exception(f"Ingestion failed for {source_name}")

        # 2. transformation
        transform_result = run_transformation(source_name, run_id)

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
        
        update_pipeline_run(run_id, "SUCCESS")
        return "pipeline " + source_name +" 5dem jawou gazouz"
    except : 
        update_pipeline_run(run_id, "FAILED")
        return "pipeline "+source_name+" teblouka"
    
@task
def run_pipeline(source_name, ingest_fn, staging_bucket):
    return pipeline_per_source(
        source_name,
        ingest_fn,
        staging_bucket
    )