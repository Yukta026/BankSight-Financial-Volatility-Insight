from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import snowflake.connector
import os

# ❄️ Snowflake connection info
SNOWFLAKE_USER = <user-name>
SNOWFLAKE_PASSWORD = <password>
SNOWFLAKE_ACCOUNT = <snowflake-account>
SNOWFLAKE_WAREHOUSE = <warehouse-name>
SNOWFLAKE_DATABASE = <snowflake-database>
SNOWFLAKE_SCHEMA = <snowflake-schema>
SNOWFLAKE_TABLE = <table-name>

LOCAL_DIR = '/tmp/advanced_bank_data'

def upload_to_snowflake():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cursor = conn.cursor()

    stage_name = f'@%{SNOWFLAKE_TABLE}'

    for filename in os.listdir(LOCAL_DIR):
        if filename.endswith('.parquet'):
            filepath = os.path.join(LOCAL_DIR, filename)
            cursor.execute(f"PUT file://{filepath} {stage_name} OVERWRITE = TRUE")

    cursor.execute(f"""
        COPY INTO ADVANCE_BNK
FROM @%ADVANCE_BNK
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'CONTINUE';

    """)

    cursor.close()
    conn.close()

with DAG(
    dag_id='upload_local_to_snowflake',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  
    catchup=False,
    tags=["snowflake", "parquet"]
) as dag:

    upload_task = PythonOperator(
        task_id='upload_to_snowflake',
        python_callable=upload_to_snowflake
    )

    upload_task
