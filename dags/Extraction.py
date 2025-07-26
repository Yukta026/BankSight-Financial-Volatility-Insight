from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime, timedelta
import yfinance as yf

default_args = {
    'owner': 'admin',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def download_stock_data(**kwargs):
    import yfinance as yf
    import pandas as pd

    symbol = kwargs['symbol']
    start_date = "2024-01-01"
    end_date = datetime.today().strftime('%Y-%m-%d')  # today's date in YYYY-MM-DD format
    data = yf.download(symbol, start=start_date, end=end_date)
    

    data.columns = [col if isinstance(col, str) else col[0] for col in data.columns]
    
    path = f"/tmp/{symbol}_stock_data.parquet"
    data.to_parquet(path, engine='pyarrow', coerce_timestamps='ms')
    print(f"Downloaded {symbol} data to {path}")


def upload_to_azure(**kwargs):
    symbol = kwargs['symbol']
    local_path = "<file-path>"
    container_name = "<container-name>"
    blob_name = f"{symbol}_stock_data.parquet"

    hook = WasbHook(wasb_conn_id='azure_blob_storage')
    hook.load_file(
        file_path=local_path,
        container_name=container_name,
        blob_name=blob_name,
        overwrite=True
    )
    print(f"Uploaded {blob_name} to Azure Blob Storage container {container_name}")

with DAG(
    dag_id='canadian_banks_stock_ingestion',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['finance', 'ingestion'],
) as dag:

    banks = ['RY', 'TD', 'BNS', 'BMO', 'CM']

    for bank in banks:
        download = PythonOperator(
            task_id=f'download_{bank}_stock',
            python_callable=download_stock_data,
            op_kwargs={'symbol': bank},
        )
        upload = PythonOperator(
            task_id=f'upload_{bank}_stock',
            python_callable=upload_to_azure,
            op_kwargs={'symbol': bank},
        )
        download >> upload
