# Google Cloud - Data Engineer

## Criar credenciais de acesso

```text
1. Navegue pelo menu principal
2. Selecione "API e serviços"
3. Selecionar a conta de serviço desejada
4. Selecionar a aba de opção "CHAVES"
5. No botão "ADICIONAR CHAVE" selcionar a opção "Criar nova chave"
6. Baixar o arquivo JSON com dados da credencial 
```

## Guardar arquivo de cedencial

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
```

## Executar código

```shell
python main.py
```

## Fontes de Dados

  [AdventureWorks_Customers.csv](https://www.kaggle.com/datasets/ukveteran/adventure-works?resource=download&select=AdventureWorks_Customers.csv)