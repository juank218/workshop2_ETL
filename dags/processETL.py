from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from datetime import datetime
from grammyETL import load_grammy, grammy_check
from spotifyETL import load_spotify, spotify_check
from merge import merge_all_csv, save_merge_DB, save_drive


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 23),  
    'email': ['juan_carlos.quintero@uao.edu.co'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'api_workshop_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',
) as dag:
    
    extract_DB = PythonOperator(
        task_id='extract_DB',
        python_callable=load_grammy,
        provide_context=True,
    )
    
    transform_DB = PythonOperator(
        task_id='transform_DB',
        python_callable=grammy_check,
        provide_context=True,
    )
    
    extract_csv = PythonOperator(
        task_id='extract_csv',
        python_callable=load_spotify,
        provide_context=True,
    )
    
    transform_csv = PythonOperator(
        task_id='transform_csv',
        python_callable=spotify_check,
        provide_context=True,
    )
    
    merge_data = PythonOperator(
        task_id='merge',
        python_callable=merge_all_csv,
        provide_context=True,
    )
    
    save_merge_DB = PythonOperator(
        task_id='save_merge_DB',
        python_callable=save_merge_DB,
        provide_context=True,
    )
    
    save_merge_drive = PythonOperator(
        task_id='save_merge_drive',
        python_callable=save_drive,
        provide_context=True,
    )
    


extract_DB >> transform_DB >> merge_data >> save_merge_DB >> save_merge_drive
extract_csv >> transform_csv