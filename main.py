from google.cloud import storage

def listar_arquivos_bucket(bucket_name):
    """Lista todos os arquivos em um bucket do Cloud Storage.

    Args:
        bucket_name (str): Nome do bucket.
    """

    # Cria um cliente para interagir com o Cloud Storage
    storage_client = storage.Client()

    # Obtém o bucket
    bucket = storage_client.bucket(bucket_name)

    # Lista os blobs (arquivos) no bucket
    blobs = bucket.list_blobs()

    for blob in blobs:
        print(blob.name)

# Exemplo de uso:
bucket_name = 'bucket_andersonbraz'  # Substitua pelo nome do seu bucket
listar_arquivos_bucket(bucket_name)
