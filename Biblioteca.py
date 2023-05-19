import pandas as pd

#Database
import pyodbc 
from requests.utils import requote_uri
from sqlalchemy import create_engine

#Excel
import glob
import datetime

#Google
from google.cloud import storage, bigquery

class Database:
    def __init__(self):
        self.server         = 'localhost\SQLEXPRESS'
        self.database       = 'dbBoticario'
        self.strConn        = 'Driver={};Server={};Database={};Trusted_Connection=yes;'.format('{SQL Server}',self.server,self.database)
        self.strConnGeneric = 'Driver={};Server={};Database={};Trusted_Connection=yes;'.format('{SQL Server}',self.server,'master')
        self.queryCreateDb  = """IF  NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'dbBoticario')
                    BEGIN
	                    CREATE DATABASE dbBoticario
                    END;"""
    
    def sqlConnect(self):
        try:
            conn = pyodbc.connect(self.strConn)
        except Exception as e:
            if 'Cannot open database "dbBoticario"' in str(e):
                conn   = pyodbc.connect(self.strConnGeneric,autocommit=True)
                cursor = conn.cursor()    
                cursor.execute(self.queryCreateDb)
                cursor.close()
                conn.close()

        conn = pyodbc.connect(self.strConn)
        return conn

    def sqlSet(self, df, table):
        quoted = requote_uri(self.strConn)
        engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
        df.to_sql(table, con=engine, if_exists='replace', index=False)                    

    def sqlGet(self, query):                
        conn = Database().sqlConnect()        
        df   = pd.read_sql_query(query,conn)            
        conn.close()
        return df        

class Excel:
    def __init__(self):
        pass
        
    def getExcel(dir_excel):
        df = pd.DataFrame()
        lista = glob.glob(dir_excel, recursive=True)

        for arquivo in lista:
            dataimport = datetime.datetime.now()
            dfExcel = pd.read_excel(io=arquivo,engine='openpyxl') #(arquivo, "Sheet1",engine='openpyxl')
            dfExcel = dfExcel.assign(ARQUIVO=arquivo,DATA_IMPORT=dataimport)
            df = pd.concat([df, dfExcel])
        return df

class Google:
    def __init__(self):
        pass
    
    def googleBigQueryConnect(self,json_auth):    
        conn = bigquery.Client.from_service_account_json(json_auth)
        return conn

    def googleStorageConnect(self,project,json_auth):    
            conn = storage.Client.from_service_account_json(json_auth)
            bucket = conn.get_bucket(project)    
            return bucket   

    def storageFileUpload(project,json_auth,storage,file):
        stg   = Google().googleStorageConnect(project,json_auth)
        stg   = stg.blob(storage)    
        stg.upload_from_filename(file)
        return stg.public_url

    def storageFileExists(project,json_auth,storage):
        stg = Google().googleStorageConnect(project,json_auth)
        result = stg.get_blob(storage)
        return result

    def bqQuery(json_auth,query):
        bq  = Google().googleBigQueryConnect(json_auth)
        result = bq.query(query).result()
        return result

    def bigqueryInsert(project,json_auth,project_dataset_table,url_file,is_increment):
        bq  = Google().googleBigQueryConnect(json_auth)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.autodetect = True
        
        if is_increment: job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND            
        else: job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        if is_increment:
            job_config.schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ]

        load_job = bq.load_table_from_uri(
            url_file, project_dataset_table, job_config=job_config        
        )  
        results = load_job.result()        
        return results

class File:
    def __init__(self):
        pass

    def createParque(self, df,dir):    
        df.to_parquet(dir, engine='pyarrow', compression='gzip')
        