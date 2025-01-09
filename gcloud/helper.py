from google.cloud import storage
from google.cloud import compute_v1
from google.cloud import bigquery

def create_dataset_bigquery(project_id, dataset_id):

    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"

    dataset = client.create_dataset(dataset)
    print(f"Dataset {dataset.dataset_id} criado com sucesso.")

def create_table_bigquery(project_id, dataset_id, table_id, csv_file):

    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Pula a primeira linha (cabeçalho)
        autodetect=True  # Detecta automaticamente o esquema
    )

    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            f"{project_id}.{dataset_id}.{table_id}",
            location="US",
            job_config=job_config
        )

    job.result() 

def create_bucket(bucket_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.location = "US"

    bucket = storage_client.create_bucket(bucket, location=bucket.location)

    print(f"Bucket {bucket.name} criado com sucesso.")

def delete_bucket(bucket_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs()

    for blob in blobs:
        blob.delete()

    bucket.delete()
    print(f"Bucket {bucket_name} deletado com sucesso.")

def list_files(bucket_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs()

    for blob in blobs:
        print(blob.name)


def create_folder(bucket_name, folder_name):

  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob_name = f"{folder_name}/"

  blob = bucket.blob(blob_name)
  blob.upload_from_string("")

  print(f"Pasta {folder_name} criada com sucesso no bucket {bucket_name}.")