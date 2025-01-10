# Google Cloud - Data Engineer

Neste repositório tenho alguns fragmentos de códigos de estudos com Google Cloud Plataform (GCP).

## Anotações

| Produto                                 | Descrição                              |
|-----------------------------------------|----------------------------------------|
| [Cloud Storage](notes/cloud-storage.md) | Notas importantes de implementação.    |
| [BigQuery](notes/bigquery.md)           | Notas importantes de implementação.    |

## Criar credenciais de acesso

```text
1. Navegue pelo menu principal
2. Selecione "API e serviços"
3. Selecionar a conta de serviço desejada
4. Selecionar a aba de opção "CHAVES"
5. No botão "ADICIONAR CHAVE" selecionar a opção "Criar nova chave"
6. Baixar o arquivo JSON com dados da credencial 
```

## Guardar arquivo de credencial

```shell
mkdir ~/.credentials/gcloud/
cp ~/Downloads/credentials.json ~/.credentials/gcloud/
```

## Criar variável de ambiente

```shell
export GOOGLE_APPLICATION_CREDENTIALS=~/.credentials/gcloud/credentials.json
```

## Instalar dependências

```shell
pip install google-cloud-storage
pip install google-cloud-compute
pip install google-cloud-bigquery
pip install google-cloud-pubsub
pip install python-dotenv
```

## Executar código

```shell
python main.py
```

## Fontes de Dados

  [Kaggle AdventureWorks](https://www.kaggle.com/datasets/ukveteran/adventure-works)