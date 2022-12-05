import pandas as pd

#Database
import pyodbc 
from requests.utils import requote_uri
from sqlalchemy import create_engine

#Excel
import glob
import datetime

#Google

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
        
    def getExcel(self, dir_excel):
        df = pd.DataFrame()
        lista = glob.glob(dir_excel, recursive=True)

        for arquivo in lista:
            dataimport = datetime.datetime.now()
            dfExcel = pd.read_excel(io=arquivo,engine='openpyxl') #(arquivo, "Sheet1",engine='openpyxl')
            dfExcel = dfExcel.assign(ARQUIVO=arquivo,DATA_IMPORT=dataimport)
            df = pd.concat([df, dfExcel])
        return df