import datetime
from airflow import DAG 
from os import getenv
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator, BigQueryCheckOperator, BigQueryExecuteQueryOperator

YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

#[START variaveis env]
GCP_PROJECT_ID = getenv("GCP_PROJECT_ID","boticario-prd-srv")
BQ_DATASETECOMMER_NAME = getenv("BQ_DATASETECOMMER_NAME","Ecommerce")
BQ_DATASETTWITTER_NAME = getenv("BQ_DATASETTWITTER_NAME","Twitter")
BQ_TABLE_NAME = getenv("BQ_TABLE_NAME","tbl_vendas")
CONN_ID = getenv("CONN_ID","cnx-composer-bi")
#[END variaveis env]

#[START Config DAG]
default_args = {
    'owner': 'Anderson Henrique Rodrigues',
    'depends_on_past': False,
    'email': ['andersonhrodrigues@gmail.com'],
    'email_on_failure': ['andersonhrodrigues@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=15),
    'start_date': YESTERDAY,
}

with DAG(
    dag_id="boticario-prd-gcs-parquet-to-bq",
    tags=['dev', 'storage', 'parquet', 'big query'],
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
    catchup=False
) as dag:
#[END Config DAG]

#[START Tarefas]

    bq_create_dataset_Ecommerce = BigQueryCreateEmptyDatasetOperator(
        task_id= "bq_create_dataset_Ecommerce",
        dataset_id=BQ_DATASETECOMMER_NAME,
        gcp_conn_id=CONN_ID
    )
    bq_create_dataset_Twitter = BigQueryCreateEmptyDatasetOperator(
        task_id= "bq_create_dataset_Twitter",
        dataset_id=BQ_DATASETTWITTER_NAME,
        gcp_conn_id=CONN_ID
    )
    gcs_parquet_to_bq = BigQueryInsertJobOperator(
        task_id="gcs_parquet_to_bq",
        gcp_conn_id=CONN_ID,
        configuration={
            "load": {
                "writeDisposition": "WRITE_TRUNCATE",
                "createDisposition": "CREATE_IF_NEEDED",
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASETECOMMER_NAME,
                    "tableId": BQ_TABLE_NAME,
                },
                "sourceUris": [f"gs://boticario-prd-srv/Parquet/vendas.parquet"],
                "sourceFormat": "PARQUET",
            }
        },
    )

    check_bq_tb_count = BigQueryCheckOperator(
        task_id="check_bq_tb_count",
        sql=f"SELECT COUNT(*) FROM {BQ_DATASETECOMMER_NAME}.{BQ_TABLE_NAME}",
        use_legacy_sql=False,
        location="us",
        gcp_conn_id=CONN_ID                
    )

    create_table_fato_vendas_ano_mes = BigQueryExecuteQueryOperator(
        task_id="create_table_fato_vendas_ano_mes", 
        sql="""
                CREATE OR REPLACE TABLE `boticario-prd-srv.Ecommerce.fato_vendas_ano_mes`
                AS

                SELECT 
                 CAST(EXTRACT(YEAR FROM DATA_VENDA) AS STRING) AS Ano
                ,RIGHT(CONCAT('00',EXTRACT(MONTH FROM DATA_VENDA)),2) AS Mes
                ,SUM(QTD_VENDA) AS QtdeVenda
                FROM `boticario-prd-srv.Ecommerce.tbl_vendas`
                GROUP BY 1,2""", 
        use_legacy_sql=False
    )
    create_table_fato_vendas_marca_linha = BigQueryExecuteQueryOperator(
        task_id="create_table_fato_vendas_marca_linha", 
        sql="""
                CREATE OR REPLACE TABLE `boticario-prd-srv.Ecommerce.fato_vendas_marca_linha`
                AS

                SELECT 
                 ID_MARCA AS IdMarca
                ,MARCA AS Marca
                ,ID_LINHA AS IdLinha
                ,LINHA AS Linha
                ,SUM(QTD_VENDA) AS QtdeVenda
                FROM `boticario-prd-srv.Ecommerce.tbl_vendas`
                GROUP BY 1,2,3,4""", 
        use_legacy_sql=False
    )
    create_table_fato_vendas_marca_ano_mes = BigQueryExecuteQueryOperator(
        task_id="create_table_fato_vendas_marca_ano_mes", 
        sql="""
                CREATE OR REPLACE TABLE `boticario-prd-srv.Ecommerce.fato_vendas_marca_ano_mes`
                AS

                SELECT 
                 CAST(EXTRACT(YEAR FROM DATA_VENDA) AS STRING) AS Ano
                ,RIGHT(CONCAT('00',EXTRACT(MONTH FROM DATA_VENDA)),2) AS Mes
                ,ID_MARCA AS IdMarca
                ,MARCA AS Marca
                ,SUM(QTD_VENDA) AS QtdeVenda
                FROM `boticario-prd-srv.Ecommerce.tbl_vendas`
                GROUP BY 1,2,3,4""", 
        use_legacy_sql=False
    )
    create_table_fato_vendas_linha_ano_mes = BigQueryExecuteQueryOperator(
        task_id="create_table_fato_vendas_linha_ano_mes", 
        sql="""
                CREATE OR REPLACE TABLE `boticario-prd-srv.Ecommerce.fato_vendas_linha_ano_mes`
                AS

                SELECT 
                 CAST(EXTRACT(YEAR FROM DATA_VENDA) AS STRING) AS Ano
                ,RIGHT(CONCAT('00',EXTRACT(MONTH FROM DATA_VENDA)),2) AS Mes
                ,ID_LINHA AS IdLinha
                ,LINHA AS Linha
                ,SUM(QTD_VENDA) AS QtdeVenda
                FROM `boticario-prd-srv.Ecommerce.tbl_vendas`
                GROUP BY 1,2,3,4""", 
        use_legacy_sql=False
    )

#[END Tarefas]

#[START Execução das Tarefas]
[bq_create_dataset_Ecommerce,bq_create_dataset_Twitter] >> gcs_parquet_to_bq >> check_bq_tb_count >> [create_table_fato_vendas_ano_mes,create_table_fato_vendas_marca_linha,create_table_fato_vendas_marca_ano_mes,create_table_fato_vendas_linha_ano_mes]
#[END Execução das Tarefas]