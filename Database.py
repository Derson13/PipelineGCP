import pyodbc 
import pandas as pd
from requests.utils import requote_uri
from sqlalchemy import create_engine

class Database:
    def __init__(self) -> None:
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
        df.to_sql(table, con=engine, if_exists='append', index=False)                    

    def sqlGet(self, query):                
        conn = Database().sqlConnect()        
        df   = pd.read_sql_query(query,conn)            
        conn.close()
        return df        
