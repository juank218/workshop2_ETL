import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Ruta del archivo del dataset mergeado
merged_csv_path = 'data/merged_data.csv'

# Leer el dataset mergeado
merged_data = pd.read_csv(merged_csv_path)

# Mostrar los primeros registros del dataset para verificar
print(merged_data.head())

# Opcional: Limpiar datos nulos o realizar cualquier procesamiento adicional si es necesario
merged_data.fillna("Desconocido", inplace=True)

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Crear la conexión con la base de datos PostgreSQL
db_connection_url = f"postgresql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_connection_url)

# Migrar el dataset mergeado a PostgreSQL
merged_data.to_sql('merged_data', engine, if_exists='replace', index=False)

print("Migración completada exitosamente.")
