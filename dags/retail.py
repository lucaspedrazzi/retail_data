from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

@dag(
    start_date=datetime(2024, 7, 7),
    schedule_interval=None,
    catchup=False,
    tags=['retail']
)

def retail_dag():
    bucket_name = ' lucaspedrazzi-retail-data'
    @task.external_python(python='/usr/local/airflow/pandas_venv/bin/python')
    
    def correct_csv_format():
        import pandas as pd
        
        file_path = '/usr/local/airflow/include/datasets/online_retail.csv'
        new_file_path = '/usr/local/airflow/include/datasets/online_retail_corrected.csv'
        df = pd.read_csv(file_path, encoding='ISO-8859-1')
        df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m/%d/%y %H:%M', errors='coerce')
        df.to_csv(new_file_path, index=False)
        
    upload_retail_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id= 'upload_retail_csv_to_gcs',
        src = '/usr/local/airflow/include/datasets/online_retail_corrected.csv',
        dst = 'raw/online_retail.csv',
        bucket = bucket_name,
        gcp_conn_id = 'gcp',
        mime_type = 'text/csv',
    )
    
    upload_country_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id= 'upload_retail_csv_to_gcs',
        src = '/usr/local/airflow/include/datasets/country.csv',
        dst = 'raw/country.csv',
        bucket = bucket_name,
        gcp_conn_id = 'gcp',
        mime_type = 'text/csv',
    )
    
    
    
    retail_dag()
    
    
