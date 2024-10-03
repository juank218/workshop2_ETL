from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 13),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


# Función para leer los datos desde el CSV
def read_spotify_csv(**kwargs):
    # Leer el archivo CSV (ajusta la ruta según la ubicación de tu archivo)
    spotify_df = pd.read_csv('..\workshop2_ETL\raw_data\spotify_dataset.csv')
    
    # Guardar el dataframe para usarlo en la siguiente tarea
    kwargs['ti'].xcom_push(key='spotify_data', value=spotify_df.to_dict())  # Se usa XCom para pasar los datos entre tareas

# Función para transformar los datos (limpieza)
def transform_spotify_csv(**kwargs):
    # Obtener los datos de la tarea anterior con XCom
    spotify_data = kwargs['ti'].xcom_pull(key='spotify_data', task_ids='read_csv')
    spotify_df = pd.DataFrame(spotify_data)
    
    # Aplicar transformaciones: limpiar nulos y eliminar duplicados
    spotify_df_cleaned = spotify_df.dropna().drop_duplicates()
    
    # Imprimir una muestra del dataframe limpio (opcional)
    print(spotify_df_cleaned.head())

# Crear el DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 10),
    'retries': 1,
}

dag = DAG(
    'spotify_data_pipeline',
    default_args=default_args,
    description='A simple DAG to process Spotify CSV data',
    schedule_interval='@daily',  # Ajusta esto según tu necesidad
)

# Definir las tareas
read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_spotify_csv,
    provide_context=True,
    dag=dag,
)

transform_csv_task = PythonOperator(
    task_id='transform_csv',
    python_callable=transform_spotify_csv,
    provide_context=True,
    dag=dag,
)

# Definir dependencias
read_csv_task >> transform_csv_task
