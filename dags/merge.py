import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive


logging.basicConfig(level=logging.INFO)


def merge_all_csv(**kwargs):
    try:
        ti = kwargs["ti"]
        
        json_data = json.loads(ti.xcom_pull(task_ids="transform_DB"))
        df1 = pd.json_normalize(data=json_data)

        json_data = json.loads(ti.xcom_pull(task_ids="transform_csv"))
        df2 = pd.json_normalize(data=json_data)
        
        logging.info("CSV files loaded successfully.")

        df_merge = pd.merge(df1, df2, left_on='nominee', right_on='track_name', how='inner')
        logging.info(f"DataFrames merged successfully. Shape: {df_merge.shape}")

        df_merge.drop(columns=['artists', 'liveness', 'time_signature'], inplace=True)
        logging.info("Unnecessary columns dropped successfully.")

        count_duplicated = df_merge['track_id'].duplicated().sum()
        logging.info(f"Number of duplicates in the 'track_id' column before dropping: {count_duplicated}")

        df_merge = df_merge.drop_duplicates(subset=['track_id'], keep='first')

        count_duplicated = df_merge['track_id'].duplicated().sum()
        logging.info(f"Number of duplicates in the 'track_id' column after dropping: {count_duplicated}")

        logging.info("Dataset ready and cleaned.")

        logging.info(f"Final DataFrame: \n{df_merge.head()}")
        return df_merge.to_json(orient='records')

    except Exception as e:
        logging.error(f"Error during dataframe merge: {e}")
        
        
def save_merge_DB(**kwargs):
    try: 
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids='merge')
        
        json_data = json.loads(data)
        df = pd.json_normalize(data=json_data)
        
        load_dotenv()

        localhost = os.getenv('LOCALHOST')
        port = os.getenv('PORT')
        nameDB = os.getenv('DB_NAME')
        userDB = os.getenv('DB_USER')
        passDB = os.getenv('DB_PASS')
        
        engine = create_engine(f'postgresql+psycopg2://{userDB}:{passDB}@{localhost}:{port}/{nameDB}')
        inspector = inspect(engine)
        
        connection = engine.connect()
        logging.info("Successfully connected to the database.")
        
        
        df.to_sql('merge_data', engine, if_exists='replace', index=False)
        
        logging.info("Data saved successfully.")
        
        connection.close()
        
        
    except Exception as e:
        logging.error(f"Error saving the merge: {str(e)}")
        

def authenticate():
    gauth = GoogleAuth()

    gauth.LoadClientConfigFile("./client_secret.json")
    
    gauth.LocalWebserverAuth()
    
    return gauth      
      
  
def save_drive(file_name='df_merge.csv', folder_id=None,**kwargs):
    try:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids='merge')
        
        json_data = json.loads(data)
        df = pd.json_normalize(data=json_data)
        
        logging.info("DataFrame saved as CSV successfully.")

        # Guardar el DataFrame como CSV temporalmente
        temp_file_path = f"./{file_name}"
        df.to_csv(temp_file_path, index=False)
        logging.info("DataFrame saved as CSV successfully.")

        # Autenticarse en Google Drive
        gauth = authenticate()
        drive = GoogleDrive(gauth)

        # Crear el archivo en Google Drive
        file = drive.CreateFile({'title': file_name, 'parents': [{'id': folder_id}] if folder_id else []})
        file.SetContentFile(temp_file_path)
        file.Upload()

        logging.info(f"Archivo '{file_name}' subido correctamente a Google Drive.")

        # Eliminar el archivo CSV local después de la subida
        os.remove(temp_file_path)

    except Exception as e:
        logging.error(f"Error saving the merge to Google Drive: {str(e)}")