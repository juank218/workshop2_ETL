import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

logging.basicConfig(level=logging.INFO)

def fetch_grammy_data():
    try:
        load_dotenv()

        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        database_name = os.getenv('DB_NAME')
        db_user = os.getenv('DB_USERNAME')
        db_password = os.getenv('DB_PASSWORD')

        engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{database_name}')
        inspector = inspect(engine)
                
        connection = engine.connect()
        logging.info("Successfully connected to the database.")
                
        dataframe = 'grammy_awards'  
        df_grammy = pd.read_sql_table(dataframe, engine)
                
        logging.info("Successfully loaded the data.")
                
        connection.close()
                
        return df_grammy.to_json(orient='records')

    except Exception as e:
            logging.error(f"Error loading the data: {str(e)}")

def validate_grammy_data(**kwargs):
    try:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids='extract_grammy_data')
        
        # Verificar si los datos son None
        if data is None:
            logging.error("No data returned from extract_grammy_data task.")
            raise ValueError("No data to validate")
        
        # Si los datos son válidos, procesarlos
        json_data = json.loads(data)
        data = pd.json_normalize(json_data)
        logging.info("Data successful load")
        
        is_null = pd.isnull(data)
        logging.info(f"The null in dataset are: {is_null}")
        logging.info(data.info())
        
        duplicated = data[data.duplicated()]
        logging.info(f"The duplicated data are: {duplicated}")
        
        logging.info("The dataset is ready to merge")
        return data.to_json(orient='records')
    
    except Exception as e:
        logging.info(f"Grammy check has found the next error: {str(e)}")