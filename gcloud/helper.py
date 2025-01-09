from google.cloud import storage


def create_bucket(bucket_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    bucket.location = "us"  # Substitua por uma localização válida (e.g., 'us', 'eu')

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