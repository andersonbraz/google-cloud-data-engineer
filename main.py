from google.cloud.pubsub_v1.types import message

from gcloud import helper as gch
from dotenv import load_dotenv
import time
import os
from google.cloud import bigquery

load_dotenv("develop.env")
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

def explore_storage():

    bucket_name = 'bucket_andersonbraz'

    gch.create_bucket(bucket_name)

    print("First list of files ::::::::::::::::::::")
    gch.list_files_bucket(bucket_name)

    gch.create_folder_bucket(bucket_name, '2020')
    gch.create_folder_bucket(bucket_name, '2021')
    gch.create_folder_bucket(bucket_name, '2022')
    gch.create_folder_bucket(bucket_name, '2023')
    gch.create_folder_bucket(bucket_name, '2024')
    gch.create_folder_bucket(bucket_name, '2025')

    print("Last list of files ::::::::::::::::::::")
    gch.list_files_bucket(bucket_name)

    time.sleep(30)

    gch.delete_bucket(bucket_name)

def explore_bigquery():

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
    time.sleep(30)
    gch.delete_dataset_bigquery(project_id, dataset_id)

def explore_pubsub():

    topic_name = "application_sample"
    subscription_name = "application_sample-sub"

    gch.create_topic_pubsub(project_id, topic_name)
    gch.create_subscription_pubsub(project_id, topic_name, subscription_name)
    messages = ["ALPHA", "BETA", "CHARLIE", "DELTA", "ECHO", "FOX", "GOLF"]

    for m in messages:
        gch.publish_message_pubsub(project_id, topic_name, str(m))

if __name__ == '__main__':
    
    explore_storage()
    explore_bigquery()
    explore_pubsub()