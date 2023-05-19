from getKeys import Keys
import Biblioteca as bb
from Biblioteca import Excel as ex
from Biblioteca import Google as gs
import warnings
warnings.filterwarnings('ignore')

class ETL:
    def __init__(self):
        
        self.db = bb.Database()
        self.pq = bb.File()        
        self.table = 'Vendas'
        self.dir_excel='./ArquivosExcel/*.xlsx'
        self.nm_file = 'vendas.parquet'
        self.parquet = '.\{}'.format(self.nm_file)
        self.project = 'boticario-prd-srv'
        self.storage = 'Parquet/'
        self.json_auth = Keys.getJsonGCP()
        
        
    def excelToSql(self):                
        df = ex.getExcel(self.dir_excel)
        self.db.sqlSet(df,self.table)
        print('Fase 1: Planilhas carregadas e inseridas no SQL com sucesso!')

    def getSqlToDf(self):
        df = self.db.sqlGet("SELECT DISTINCT * FROM {} WITH(NOLOCK)".format(self.table))
        print('Fase 2: Consulta aos dados no SQL realizada com sucesso!')
        return df

    def dfToParquet(self, df):
        self.pq.createParque(df, self.parquet)
        print('Fase 3: Arquivo parquet gerado localmente com sucesso!')

    def fileToStorage(self):
        result = gs.storageFileUpload(
                             project   = self.project
                            ,json_auth = self.json_auth
                            ,storage   = self.storage + self.nm_file
                            ,file      = self.nm_file
                            )

        print('Fase 4: Arquivo parquet enviado ao Storage com sucesso!')

ETL().excelToSql()
df = ETL().getSqlToDf()
ETL().dfToParquet(df=df)    
ETL().fileToStorage()