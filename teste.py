from Biblioteca import Google as gs

PROJECT_ID = 'boticario-prd-srv'
BUCKET_NAME = 'boticario-prd-srv'
storage = 'gs://{}/Parquet/vendas.parquet'.format(BUCKET_NAME)
storage = 'gs://boticario-prd-srv/Parquet/vendas.parquet'
storage = 'Parquet/vendas.parquet'
project = '{}'.format(PROJECT_ID)
json_file = './json_gcs.json'

print(storage)
print(project)


result = gs.storageFileExists(project,json_file,storage)
print(result)