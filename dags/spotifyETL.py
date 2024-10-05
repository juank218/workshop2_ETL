import pandas as pd
import logging
import json
import os

logging.basicConfig(level=logging.INFO)

def fetch_spotify_data():
    try:
        data = pd.read_csv("/home/juank/Escritorio/workshop2_ETL/raw_data/spotify_dataset.csv")
        logging.info("Successfully loaded the data.")
        return data.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error loading spotify : {str(e)}")
        

def validate_spotify_data(**kwargs):
    try:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids='extract_spotify_data')

        # Verificar si los datos son None
        if data is None:
            logging.error("No data returned from fetch_spotify_data task.")
            raise ValueError("No data to validate")
        
        # Si los datos son válidos, procesarlos
        json_data = json.loads(data)
        data = pd.json_normalize(json_data)
        logging.info("Data successful load")

        is_null = pd.isnull(data)
        logging.info(f"The null in dataset are: {is_null}")
        logging.info(data.info())

        logging.info("Now we will drop the column Unnamed: 0")
        data = data.drop(columns=['Unnamed: 0'])
        logging.info(data.info())

        duplicated = data[data.duplicated()]
        logging.info(f"The duplicated data are: {duplicated}")

        logging.info("The dataset is ready to merge")
        return data.to_json(orient='records')
    
    except Exception as e:
        logging.info(f"Spotify check has found the next error: {str(e)}")