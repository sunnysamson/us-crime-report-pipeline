from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import sys
import os

# Add the scripts folder to the Python path
sys.path.append(os.path.join(os.path.expanduser("~"), "airflow", "DE_Project", "scripts"))

from download_kaggle_dataset import download_kaggle_dataset
from preprocess_and_normalization import preprocess_and_normalization
from load_to_mysql import load_to_mysql
from query_mysql import query_mysql

#from scripts.download_kaggle_dataset import download_kaggle_dataset
#from scripts.preprocess_and_normalization import preprocess_and_normalization
#from scripts.load_to_mysql import load_to_mysql
#from scripts.query_mysql import query_mysql

# Set the KAGGLE_CONFIG_DIR to point to the .kaggle folder
os.environ['KAGGLE_CONFIG_DIR'] = os.path.expanduser("~/.kaggle")

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2025, 1, 1),  # Adjust this as per your requirement
    'catchup': False,
}

# Define the DAG
with DAG('de_project_pipeline',
         default_args=default_args,
         description='A simple pipeline for crime data processing',
         schedule_interval='@daily',  # Executes once a day
         ) as dag:

    # Task 1: Download Dataset
    download_task = PythonOperator(
        task_id='download_kaggle_dataset',
        python_callable=download_kaggle_dataset,
    )

    # Task 2: Preprocess and Normalize Data
    preprocess_task = PythonOperator(
        task_id='preprocess_and_normalize',
        python_callable=preprocess_and_normalization,
    )

    # Task 3: Load Data to MySQL
    load_task = PythonOperator(
        task_id='load_data_to_mysql',
        python_callable=load_to_mysql,
    )

    # Task 4: Query MySQL (as an example query task)
    query_task = PythonOperator(
        task_id='query_mysql',
        python_callable=query_mysql,
        #op_args=["SELECT * FROM crime LIMIT 10;"],  # Example query
    )

    # Define Task dependencies
    download_task >> preprocess_task >> load_task >> query_task
