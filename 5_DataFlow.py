import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
import warnings
warnings.filterwarnings('ignore')

# Configurações do projeto
PROJECT_ID = 'boticario-prd-srv'
BUCKET_NAME = 'boticario-prd-srv'
DATASET_ID = 'DataFlow'
TABLE_ID = 'tbl_dataflow'
TEMPLATE_LOCATION = 'gs://boticario-prd-srv/dataflow-template/gs-parquet-to-dataflow-to-bigquery'

def run():
    # Opções do pipeline
    pipeline_options = PipelineOptions(
        runner="DataflowRunner",
        project=PROJECT_ID,
        staging_location=f'gs://{BUCKET_NAME}/staging',
        temp_location=f'gs://{BUCKET_NAME}/temp',
        region='us-central1'
    )
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Lendo o arquivo parquet do Cloud Storage
        vendas_data = (p | 'Read Parquet File' >> beam.io.ReadFromParquet(
            f'gs://{BUCKET_NAME}/Parquet/vendas.parquet'
        ))
        
        # Criando o dataset no BigQuery, se necessário
        bq_client = bigquery.Client(project=PROJECT_ID)
        dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
        try:
            bq_client.get_dataset(dataset_ref)
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            bq_client.create_dataset(dataset)
        
        # Verificando a existência da tabela
        table_ref = bigquery.TableReference(dataset_ref, TABLE_ID)
        try:
            bq_client.get_table(table_ref)
            table_exists = True
        except NotFound:
            table_exists = False
        
        # Escrevendo os dados no BigQuery
        vendas_data | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
            table=table_ref.table_id,
            dataset=table_ref.dataset_id,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE if table_exists else beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    # Configurar as credenciais do Google Cloud
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "./json_gcs.json"  # Caminho para o arquivo de credenciais JSON
    
    run()
