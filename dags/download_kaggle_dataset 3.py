import os
from kaggle.api.kaggle_api_extended import KaggleApi

def download_kaggle_dataset():
    # Set the KAGGLE_CONFIG_DIR to point to the .kaggle folder
    os.environ['KAGGLE_CONFIG_DIR'] = os.path.expanduser("~/.kaggle")

    # Initialize Kaggle API
    api = KaggleApi()
    api.authenticate()

    # define parameters for download
    dataset = 'arpitsinghaiml/u-s-crime-dataset'  # Example dataset
    target_path = os.path.expanduser("~/airflow/DE_Project/Data")

    # Download and unzip the dataset
    api.dataset_download_files(dataset, path=target_path, unzip=True)

    # renaming the file name
    # Define paths
    old_file_path = os.path.expanduser("~/airflow/DE_project/Data/Crime_Data_from_2020_to_Present.csv")
    new_file_path = os.path.expanduser("~/airflow/DE_project/Data/us_crime_data.csv")

    # Rename file
    os.rename(old_file_path, new_file_path)

if __name__ == "__main__":
    download_kaggle_dataset()
