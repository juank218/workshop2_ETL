from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Función para extraer datos desde la base de datos
def extract_data_from_db(**kwargs):
    # Construir la conexión a la base de datos
    db_connection_url = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    engine = create_engine(db_connection_url)

    # Consultar los datos desde la base de datos (puedes ajustar la consulta SQL)
    query = "SELECT * FROM grammy_awards"
    with engine.connect() as connection:
        result = connection.execute(text(query))
        grammy_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    # Almacenar el DataFrame para la próxima tarea
    kwargs['ti'].xcom_push(key='extracted_data', value=grammy_df.to_dict())

# Función para transformar los datos
def transform_data(**kwargs):
    # Recuperar los datos extraídos con XCom
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract')
    grammy_df = pd.DataFrame(extracted_data)

    # Aplicar transformaciones: eliminar nulos y duplicados
    grammy_df_cleaned = grammy_df.dropna().drop_duplicates()

    # Mostrar una muestra de los datos limpios (opcional)
    print(grammy_df_cleaned.head())

# Crear el DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'spotify_data_pipeline',
    default_args=default_args,
    description='A simple DAG to extract and transform Grammy awards data from PostgreSQL',
    schedule_interval='@daily',
)

# Tarea para extraer datos desde la base de datos
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data_from_db,
    provide_context=True,
    dag=dag,
)

# Tarea para transformar los datos
transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Definir las dependencias
extract_task >> transform_task
