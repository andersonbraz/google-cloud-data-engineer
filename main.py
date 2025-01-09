from google.cloud import storage
from google.cloud.storage import storage_class

def criar_bucket(nome_do_bucket, localizacao="US", classe_armazenamento=storage_class.Standard):
    """Cria um novo bucket no Google Cloud Storage com localização e classe de armazenamento."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.create_bucket(nome_do_bucket, location=localizacao, storage_class=classe_armazenamento)
        print(f"Bucket {bucket.name} criado em {bucket.location} com classe de armazenamento {bucket.storage_class}.")
        return bucket
    except Exception as e:
        print(f"Erro ao criar o bucket: {e}")
        return None

nome_do_bucket = "sample_andersonbraz"
bucket_criado = criar_bucket(nome_do_bucket, "US-CENTRAL1", storage_class.Nearline)

if bucket_criado:
    print(f"Informações do bucket criado: {bucket_criado}")