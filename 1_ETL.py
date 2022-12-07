from getKeys import Keys
from Biblioteca import File as pq
from Biblioteca import Google as gs
from Biblioteca import Excel as ex
from Biblioteca import Database as db

#excel/sql/parquet
table = 'Vendas'
dir_excel='./ArquivosExcel/*.xlsx'
#parquet
nm_file = 'vendas.parquet'
parquet = '.\{}'.format(nm_file)
#storage
project = 'boticario-prd'
storage = 'Parquet/'
json_auth = Keys.getJsonGCP()


def excelToSql():
    df = ex.getExcel(dir_excel=dir_excel)
    db.sqlSet(df=df,table=table)
    print('Fase 1: Planilhas carregadas e inseridas no SQL com sucesso!')

def getSqlToDf():
    df = db.sqlGet("SELECT * FROM {} WITH(NOLOCK)".format(table))
    print('Fase 2: Consulta aos dados no SQL realizada com sucesso!')
    return df

def dfToParquet(df):
    pq.createParque(df=df, dir=parquet)
    print('Fase 3: Arquivo parquet gerado localmente com sucesso!')

def fileToStorage(parquet):
    result = gs.storageFileUpload(
                         project   = project
                        ,json_auth = json_auth
                        ,storage   = storage + nm_file
                        ,file      = nm_file
                        )

    print('Fase 4: Arquivo parquet enviado ao Storage com sucesso!')

excelToSql()
df = getSqlToDf()
dfToParquet(df=df)    
fileToStorage(parquet=parquet)