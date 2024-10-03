import pandas as pd
import logging
import json
import os

logging.basicConfig(level=logging.INFO)

def fetch_spotify_data():
    try:
        spotify_data = pd.read_csv("./raw_data/spotify_dataset.csv")
        logging.info("Spotify data loaded successfully.")
        return spotify_data.to_json(orient="records")
    except Exception as error:
        logging.error(f"Error occurred while loading Spotify data: {str(error)}")
        

def validate_spotify_data(**context):
    try:
        ti = context["ti"]
        raw_data = ti.xcom_pull(task_ids='fetch_spotify')
        json_records = json.loads(raw_data)
        dataframe = pd.json_normalize(data=json_records)

        logging.info("Spotify data successfully pulled and normalized.")

        null_values = dataframe.isnull().sum()
        logging.info(f"Null values in the dataset: {null_values}")

        logging.info("Dropping the column 'Unnamed: 0'")
        dataframe = dataframe.drop(columns=['Unnamed: 0'])
        logging.info(dataframe.info())

        duplicate_rows = dataframe[dataframe.duplicated()]
        logging.info(f"Duplicate entries in the dataset: {duplicate_rows}")

        logging.info("Data is ready for the merge process.")
        return dataframe.to_json(orient='records')
    except Exception as error:
        logging.error(f"Error in Spotify data validation: {str(error)}")