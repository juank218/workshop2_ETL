import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

logging.basicConfig(level=logging.INFO)

def combine_csv_files(**context):
    try:
        # Cargar los datos desde XCom
        ti = context["ti"]
        
        json_data_db = json.loads(ti.xcom_pull(task_ids="transform_db_task"))
        df_database = pd.json_normalize(data=json_data_db)

        json_data_csv = json.loads(ti.xcom_pull(task_ids="transform_csv_task"))
        df_csv = pd.json_normalize(data=json_data_csv)
        
        logging.info("CSV data loaded successfully.")

        # Combinar los DataFrames
        combined_df = pd.merge(df_database, df_csv, left_on='nominee', right_on='track_name', how='inner')
        logging.info(f"DataFrames merged successfully. Final shape: {combined_df.shape}")

        # Eliminar columnas innecesarias
        combined_df.drop(columns=['artist_name', 'live_performance', 'signature_time'], inplace=True)
        logging.info("Columns removed successfully.")

        # Verificar duplicados
        initial_duplicates = combined_df['track_id'].duplicated().sum()
        logging.info(f"Initial duplicates in 'track_id': {initial_duplicates}")

        combined_df = combined_df.drop_duplicates(subset=['track_id'], keep='first')

        final_duplicates = combined_df['track_id'].duplicated().sum()
        logging.info(f"Remaining duplicates in 'track_id': {final_duplicates}")

        logging.info("Data cleaned and ready for next step.")

        return combined_df.to_json(orient='records')

    except Exception as error:
        logging.error(f"Error during CSV merge process: {str(error)}")


def store_merged_data_to_db(**context):
    try:
        ti = context["ti"]
        merged_data = ti.xcom_pull(task_ids='combine_csv')
        
        json_records = json.loads(merged_data)
        df_merged = pd.json_normalize(data=json_records)
        
        # Cargar variables de entorno
        load_dotenv()

        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_name = os.getenv('DATABASE_NAME')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        
        # Conectar a la base de datos
        db_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        db_inspector = inspect(db_engine)
        
        connection = db_engine.connect()
        logging.info("Database connection established.")

        # Guardar el DataFrame en la base de datos
        df_merged.to_sql('combined_data', db_engine, if_exists='replace', index=False)
        
        logging.info("Merged data saved to the database.")

        connection.close()

    except Exception as error:
        logging.error(f"Error saving merged data to the database: {str(error)}")


def authenticate_google_drive():
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile("./client_secrets.json")
    gauth.LocalWebserverAuth()
    return gauth


def upload_to_google_drive(file_name='combined_data.csv', folder_id=None, **context):
    try:
        ti = context["ti"]
        merged_data = ti.xcom_pull(task_ids='combine_csv')
        
        json_records = json.loads(merged_data)
        df = pd.json_normalize(data=json_records)
        
        # Guardar el DataFrame como un archivo CSV
        temp_csv_path = f"./{file_name}"
        df.to_csv(temp_csv_path, index=False)
        logging.info("CSV file saved successfully.")

        # Autenticarse en Google Drive
        gauth = authenticate_google_drive()
        drive = GoogleDrive(gauth)

        # Crear y subir el archivo a Google Drive
        drive_file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}] if folder_id else []})
        drive_file.SetContentFile(temp_csv_path)
        drive_file.Upload()

        logging.info(f"File '{file_name}' uploaded successfully to Google Drive.")

        # Eliminar el archivo CSV local
        os.remove(temp_csv_path)

    except Exception as error:
        logging.error(f"Error uploading file to Google Drive: {str(error)}")
