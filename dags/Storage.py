from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import os

def download_from_blob():
    blob_conn_str = <azure-blob-storage-connection-string>
    container_name = <container-name>
    prefix = "advanced_bank_data"  

    blob_service = BlobServiceClient.from_connection_string(blob_conn_str)
    container_client = blob_service.get_container_client(container_name)

    local_dir = "/tmp/advanced_bank_data"
    os.makedirs(local_dir, exist_ok=True)

    blobs = container_client.list_blobs(name_starts_with=prefix)
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob.name)
        local_path = os.path.join(local_dir, os.path.basename(blob.name))
        with open(local_path, "wb") as f:
            f.write(blob_client.download_blob().readall())

    return local_dir

# Define DAG
with DAG(
    dag_id='download_blob_to_local',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None, 
    catchup=False,
    tags=['finance', 'ingestion']
) as dag:

    download_task = PythonOperator(
        task_id='download_from_blob',
        python_callable=download_from_blob
    )

    download_task
