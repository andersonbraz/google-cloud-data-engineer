from gcloud import helper as gch
from dotenv import load_dotenv
import time
import os

load_dotenv("develop.env")
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

def test_storage():

    bucket_name = 'bucket_andersonbraz'

    gch.create_bucket(bucket_name)

    print("First list of files ::::::::::::::::::::")
    gch.list_files(bucket_name)

    gch.create_folder(bucket_name, '2020')
    gch.create_folder(bucket_name, '2021')
    gch.create_folder(bucket_name, '2022')
    gch.create_folder(bucket_name, '2023')
    gch.create_folder(bucket_name, '2024')
    gch.create_folder(bucket_name, '2025')

    print("Last list of files ::::::::::::::::::::")
    gch.list_files(bucket_name)

    time.sleep(30)

    gch.delete_bucket(bucket_name)

def test_bigquery():

    dataset_id = 'data_users'

    gch.create_dataset(project_id, dataset_id)

def test_load_csv_to_bigquery():
    
    csv_file = 'data/users.csv'
    dataset_id = 'data_users'
    table_id = 'users'

    gch.load_csv_to_bigquery(project_id, csv_file, dataset_id, table_id)
    

if __name__ == '__main__':

    test_bigquery()
