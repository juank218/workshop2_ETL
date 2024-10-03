import pandas as pd
import logging
import os
import json
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

logging.basicConfig(level=logging.INFO)

def load_grammy():
    try:
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
        
        dataframe = 'grammy_awards'  
        df_grammy = pd.read_sql_table(dataframe, engine)
        
        logging.info("Successfully loaded the data.")
        
        connection.close()
        
        return df_grammy.to_json(orient='records')
    
    
    except Exception as e:
        logging.error(f"Error loading the data: {str(e)}")
        
        
def grammy_check(**kwargs):
    try:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids='extract_DB')
        json_data = json.loads(data)
        data = pd.json_normalize(data=json_data)
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
