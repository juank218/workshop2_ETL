import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

logging.basicConfig(level=logging.INFO)

def fetch_grammy_data():
    try:
        # Cargar las variables de entorno
        load_dotenv()

        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        database_name = os.getenv('DATABASE')
        db_user = os.getenv('USER_DB')
        db_password = os.getenv('PASSWORD_DB')

        # Establecer conexión a la base de datos
        db_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{database_name}')
        db_inspector = inspect(db_engine)

        connection = db_engine.connect()
        logging.info("Connection to the database established successfully.")

        # Cargar datos de la tabla Grammy Awards
        table_name = 'grammy_awards'
        grammy_data = pd.read_sql_table(table_name, db_engine)

        logging.info("Data from the Grammy Awards table loaded successfully.")

        connection.close()

        return grammy_data.to_json(orient='records')

    except Exception as error:
        logging.error(f"Failed to load data: {str(error)}")


def validate_grammy_data(**context):
    try:
        ti = context['ti']
        raw_data = ti.xcom_pull(task_ids='extract_db_task')
        json_records = json.loads(raw_data)
        dataframe = pd.json_normalize(json_records)

        logging.info("Data loaded and normalized.")

        # Verificar valores nulos
        null_check = dataframe.isnull().sum()
        logging.info(f"Null values in dataset: {null_check}")

        # Verificar datos duplicados
        duplicate_records = dataframe[dataframe.duplicated()]
        logging.info(f"Duplicated records: {duplicate_records}")

        logging.info("Data validation completed, ready for further processing.")

        return dataframe.to_json(orient='records')

    except Exception as error:
        logging.error(f"Error during data validation: {str(error)}")
