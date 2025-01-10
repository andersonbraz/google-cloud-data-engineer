import json
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import pubsub
import google.cloud.exceptions

def create_topic_pubsub(project_id, topic_name):

    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    try:
        topic = publisher.create_topic(request={"name": topic_path})
        print(f"Tópico criado: {topic.name}")
    except google.cloud.exceptions.Conflict:
        print(f"Tópico [{topic_path}] já existe.")
    except google.cloud.exceptions.GoogleCloudError as e:
        print(f"Erro ao criar o tópico [{topic_path}]: {e}")

def publish_message_pubsub(project_id, topic_name, message):

    publisher = pubsub.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    data = message.encode('utf-8')

    try:
        future = publisher.publish(topic_path, data)
        message_id = future.result()
        print(f"Mensagem publicada com sucesso! ID da mensagem: {message_id}")
    except Exception as e:
        print(f"Erro ao publicar a mensagem: {e}")


def create_subscription_pubsub(project_id, topic_name, subscription_name):

    publisher = pubsub.PublisherClient()
    subscriber = pubsub.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
    topic_path = publisher.topic_path(project_id, topic_name)

    try:
        subscription = subscriber.create_subscription(
            name=subscription_path, topic=topic_path
        )
        print(f"Assinatura criada: {subscription.name}")
    except google.cloud.exceptions.Conflict:
        print(f"Assinante [{subscription_path}] já existe.")
    except google.cloud.exceptions.GoogleCloudError as e:
        print(f"Erro ao criar o assinante [{subscription_path}]: {e}")

def receive_messages_pubsub(project_id, subscription_name):

    subscriber = pubsub.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)

    def callback(message):
        print(f"Received message: {message.data}")
        message.ack()  # Confirma o recebimento da mensagem

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    try:
        streaming_pull_future.result(timeout=None)
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

    print("Listening stopped.")

def create_dataset_bigquery(project_id, dataset_id):
    try:
        client = bigquery.Client()
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")

        dataset.location = "US"

        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Dataset [{dataset_id}] criado com sucesso.")
    except google.cloud.exceptions.Conflict:
        print(f"Dataset [{dataset_id}] já existe.")
    except google.cloud.exceptions.GoogleCloudError as e:
        print(f"Erro ao criar o dataset [{dataset_id}]: {e}")

def delete_dataset_bigquery(project_id, dataset_id):
    try:
        client = bigquery.Client(project=project_id)
        dataset_ref = client.dataset(dataset_id)

        client.delete_dataset(dataset_ref, delete_contents=True)
        print(f"Dataset [{dataset_id}] excluído com sucesso.")
    except google.cloud.exceptions.NotFound:
        print(f"Dataset [{dataset_id}] não encontrado.")
    except google.cloud.exceptions.GoogleCloudError as e:
        print(f"Erro ao deletar o dataset [{dataset_id}]: {e}")

def create_table_bigquery(project_id, dataset_id, table_id, schema):

    client = bigquery.Client(project=project_id)

    table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}", schema=schema)

    table = client.create_table(table)
    print(f"Tabela [{table.table_id}] criada com sucesso.")

def load_table_bigquery(project_id, dataset_id, table_id, csv_file):

    client = bigquery.Client(project=project_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True  # Detecta Schema
    )

    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            f"{project_id}.{dataset_id}.{table_id}",
            location="US",
            job_config=job_config
        )

    job.result()
    print(f"[{dataset_id}.{table_id}] carregado com sucesso.")

def create_bucket(bucket_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        bucket.location = "US"

        bucket = storage_client.create_bucket(bucket, location=bucket.location)
        print(f"Bucket [{bucket.name}] criado com sucesso.")
    except google.cloud.exceptions.Conflict:
        print(f"Bucket [{bucket_name}] já existe.")
    except google.cloud.exceptions.GoogleCloudError as e:
        print(f"Erro ao criar o bucket [{bucket_name}]: {e}")

def delete_bucket(bucket_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs()

        for blob in blobs:
            blob.delete()

        bucket.delete()
        print(f"Bucket [{bucket_name}] deletado com sucesso.")
    except google.cloud.exceptions.NotFound:
        print(f"Bucket [{bucket_name}] não encontrado.")
    except google.cloud.exceptions.GoogleCloudError as e:
        print(f"Erro ao deletar o bucket [{bucket_name}]: {e}")

def list_files_bucket(bucket_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs()

        for blob in blobs:
            print(blob.name)
    except google.cloud.exceptions.NotFound:
        print(f"Bucket [{bucket_name}] não encontrado.")
    except google.cloud.exceptions.GoogleCloudError as e:
        print(f"Erro ao listar arquivos no bucket [{bucket_name}]: {e}")

def create_folder_bucket(bucket_name, folder_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob_name = f"{folder_name}/"

        blob = bucket.blob(blob_name)
        blob.upload_from_string("")

        print(f"Pasta [{folder_name}] criada com sucesso no bucket {bucket_name}.")
    except google.cloud.exceptions.NotFound:
        print(f"Bucket [{bucket_name}] não encontrado.")
    except google.cloud.exceptions.GoogleCloudError as e:
        print(f"Erro ao criar pasta [{folder_name}] no bucket [{bucket_name}]: {e}")