import json
import os
from dotenv import load_dotenv
from minio import Minio
from minio.commonconfig import CopySource
from minio.deleteobjects import DeleteObject
import io
load_dotenv()
from prefect import task

client = Minio(
    os.getenv("MINIO_URL"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False
)

def add_bucket(bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def save_to_minio(bucket_name, object_name:str, data,content_type='application/json', metadata=None):
    json_bytes = json.dumps(data,ensure_ascii=False).encode("utf-8")
    client.put_object(bucket_name, object_name, io.BytesIO(json_bytes), length=len(json_bytes), content_type=content_type, metadata=metadata)
    print(f"Data saved to bucket '{bucket_name}' with object name '{object_name}'.")
    

@task(retries=3, retry_delay_seconds=10)
def migrate_to_historical(bucket_staging, bucket_historical, object_path):

    result = client.copy_object(
        bucket_name=bucket_historical,
        object_name=object_path,
        source=CopySource(
            bucket_name=bucket_staging,
            object_name=object_path,
        )
    )

    if result.object_name:
        # ✔ supprimer depuis staging (correct)
        client.remove_object(bucket_staging, object_path)
        return True

    return False