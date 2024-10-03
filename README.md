# workshop2_ETL

```markdown
# Grammy & Spotify Data ETL Pipeline

This project is an ETL (Extract, Transform, Load) pipeline that processes data related to Grammy Awards and Spotify tracks. The project is developed using Python and `pandas` for data manipulation, with data being stored in a PostgreSQL database. The project also includes DAGs managed by Apache Airflow to automate the ETL process.

## Project Structure

```bash
WORKSHOP2_ETL/
│
├── code/
│   ├── EDAs/
│   │   ├── EDA_grammy.ipynb        # Exploratory Data Analysis for Grammy dataset
│   │   └── EDA_spotify.ipynb       # Exploratory Data Analysis for Spotify dataset
│   ├── merge/
│   │   └── Datasets_Merge.py       # Script to merge the Grammy and Spotify datasets
│   └── csv_to_db_migrations/       # Folder containing scripts to migrate CSV to DB
│
├── dags/
│   ├── grammy.py                   # Airflow DAG for Grammy dataset ETL process
│   └── spotify.py                  # Airflow DAG for Spotify dataset ETL process
│
├── data/                           # Folder to store datasets
│   ├── grammy_dataset_cleaned.csv  # Cleaned Grammy dataset
│   ├── merged_data.csv             # Merged dataset (Spotify + Grammy)
│   └── spotify_dataset_cleaned.csv # Cleaned Spotify dataset
│
├── raw_data/                       # Folder to store raw input data
│   ├── spotify_dataset.csv         # Raw Spotify dataset
│   └── the_grammy_awards.csv       # Raw Grammy dataset
│
├── venv/                           # Virtual environment
├── .env                            # Environment variables file
├── README.md                       # Project documentation
└── requirements.txt                # Python dependencies

```

## Installation

### 1. Clone the repository

```bash
bash
Copiar código
git clone https://github.com/yourusername/WORKSHOP2_ETL.git
cd WORKSHOP2_ETL

```

### 2. Set up a virtual environment using `venv`

If you don't have `venv` installed, follow the instructions [here](https://docs.python.org/3/library/venv.html).

1. Create the virtual environment:
    
    ```bash
    bash
    Copiar código
    python3 -m venv venv
    
    ```
    
2. Activate the virtual environment:
    - On macOS/Linux:
        
        ```bash
        bash
        Copiar código
        source venv/bin/activate
        
        ```
        
    - On Windows:
        
        ```bash
        bash
        Copiar código
        venv\Scripts\activate
        
        ```
        

### 3. Install the required dependencies

Make sure you have the virtual environment activated before running this command:

```bash
bash
Copiar código
pip install -r requirements.txt

```

### 4. Set up PostgreSQL database

1. Install PostgreSQL if you haven't done so already.
2. Create a PostgreSQL database:

```bash
bash
Copiar código
psql -U postgres
CREATE DATABASE workshop2;

```

1. Update your `.env` file with your PostgreSQL connection details:

```makefile
makefile
Copiar código
DB_USERNAME=your_username
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=workshop2

```

## Usage

### 1. Running the ETL pipeline

You can trigger the ETL pipeline through Airflow. Make sure your Airflow instance is running and that the DAGs are loaded.

1. **Start Airflow** (in the terminal):
    
    ```bash
    bash
    Copiar código
    airflow webserver --port 8080
    airflow scheduler
    
    ```
    
2. Access the Airflow dashboard by visiting `http://localhost:8080` in your browser.
3. Trigger the ETL DAGs (`grammy.py`, `spotify.py`) from the Airflow UI.

### 2. Migrate the merged dataset to PostgreSQL

You can run the migration script to migrate the cleaned and merged dataset to PostgreSQL.

```bash
bash
Copiar código
python merge/Datasets_Merge.py

```

## Data Sources

- **Spotify Dataset**: Contains track-level data including genre and other metadata.
- **Grammy Dataset**: Contains information about Grammy winners and categories.

## Future Work

- Add more data sources to enrich the analysis.
- Implement more complex transformations and validations.