import pandas as pd
import logging
import json
import os

logging.basicConfig(level=logging.INFO)

def load_spotify():
    try:
        data = pd.read_csv("./cleanData/spotify-clean.csv")
        logging.info("Successfully loaded the data.")
        return data.to_json(orient="records")
    except Exception as e:
        logging.error(f"Error loading spotify : {str(e)}")
        

def spotify_check(**kwargs):
    try:
        ti = kwargs["ti"]
        data = ti.xcom_pull(task_ids='extract_csv')
        json_data = json.loads(data)
        data = pd.json_normalize(data=json_data)
        logging.info("Data successful load")
        is_null = pd.isnull(data)
        logging.info(f"The null in dataset are: {is_null}")
        logging.info("Now we will drop the column Unnamed: 0")
        data = data.drop(columns=['Unnamed: 0'])
        logging.info(data.info())
        duplicated = data[data.duplicated()]
        logging.info(f"The duplicated data are: {duplicated}")
        logging.info("The dataset is ready to merge")
        return data.to_json(orient='records')
    except Exception as e:
        logging.info(f"Spotify check has found the next error: {str(e)}")

