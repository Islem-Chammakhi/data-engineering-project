import json
import os
from dotenv import load_dotenv
from minio import Minio
import io
from minio_client.minio import copy_object, delete_object
from prefect import task
load_dotenv()

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

def save_to_minio(bucket_name, object_name, data,content_type='application/json', metadata=None):
    json_bytes = json.dumps(data,ensure_ascii=False).encode("utf-8")
    client.put_object(bucket_name, object_name, io.BytesIO(json_bytes), length=len(json_bytes), content_type=content_type, metadata=metadata)
    print(f"Data saved to bucket '{bucket_name}' with object name '{object_name}'.")
    


@task
def migrate_to_historical(bucket_staging, bucket_historical, object_path):

    # copier vers historique
    copy_object(
        source_bucket=bucket_staging,
        dest_bucket=bucket_historical,
        object_name=object_path
    )

    # supprimer staging 
    delete_object(bucket_staging, object_path)