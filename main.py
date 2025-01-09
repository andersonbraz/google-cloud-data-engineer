from gcloud import helper as gch
from dotenv import load_dotenv
import time
import os
from google.cloud import bigquery

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

    dataset_id = 'AdventureWorks'
    csv_file = 'data/AdventureWorks_Customers.csv'
    table_id = 'Customers'

    schema = [
        bigquery.SchemaField("CustomerKey", "INTEGER"),
        bigquery.SchemaField("Prefix", "STRING"),
        bigquery.SchemaField("FirstName", "STRING"),
        bigquery.SchemaField("LastName", "STRING"),
        bigquery.SchemaField("BirthDate", "DATE"),
        bigquery.SchemaField("MaritalStatus", "STRING"),
        bigquery.SchemaField("Gender", "STRING"),
        bigquery.SchemaField("EmailAddress", "STRING"),
        bigquery.SchemaField("AnnualIncome", "INTEGER"),
        bigquery.SchemaField("TotalChildren", "INTEGER"),
        bigquery.SchemaField("EducationLevel", "STRING"),
        bigquery.SchemaField("Occupation", "STRING"),
        bigquery.SchemaField("HomeOwner", "BOOLEAN"),
    ]

    gch.create_dataset_bigquery(project_id, dataset_id)
    gch.create_table_bigquery(project_id, dataset_id, table_id, schema)
    gch.load_table_bigquery(project_id, dataset_id, table_id, csv_file)

if __name__ == '__main__':

    test_bigquery()
