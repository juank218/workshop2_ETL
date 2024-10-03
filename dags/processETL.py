from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from grammy_etl import fetch_grammy_data, validate_grammy_data
from spotify_etl import fetch_spotify_data, validate_spotify_data
from merge_process import combine_csv_files, store_merged_data_to_db, upload_to_google_drive

# Configuración por defecto del DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 23),  
    'email': ['data_team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

# Definición del DAG
with DAG(
    'data_pipeline_etl',
    default_args=default_args,
    description='ETL pipeline for combining Grammy and Spotify data',
    schedule_interval='@daily',
) as dag:
    
    extract_grammy_task = PythonOperator(
        task_id='extract_grammy_data',
        python_callable=fetch_grammy_data,
        provide_context=True,
    )
    
    validate_grammy_task = PythonOperator(
        task_id='validate_grammy_data',
        python_callable=validate_grammy_data,
        provide_context=True,
    )
    
    extract_spotify_task = PythonOperator(
        task_id='extract_spotify_data',
        python_callable=fetch_spotify_data,
        provide_context=True,
    )
    
    validate_spotify_task = PythonOperator(
        task_id='validate_spotify_data',
        python_callable=validate_spotify_data,
        provide_context=True,
    )
    
    merge_datasets_task = PythonOperator(
        task_id='merge_datasets',
        python_callable=combine_csv_files,
        provide_context=True,
    )
    
    store_merged_data_task = PythonOperator(
        task_id='store_data_in_db',
        python_callable=store_merged_data_to_db,
        provide_context=True,
    )
    
    upload_drive_task = PythonOperator(
        task_id='upload_to_google_drive',
        python_callable=upload_to_google_drive,
        provide_context=True,
    )
    

# Definir el flujo de tareas
extract_grammy_task >> validate_grammy_task >> merge_datasets_task >> store_merged_data_task >> upload_drive_task
extract_spotify_task >> validate_spotify_task
