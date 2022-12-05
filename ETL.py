import Biblioteca as bib

ex = bib.Excel()
db = bib.Database()

def excelToSql():
    df = ex.getExcel(dir_excel='./ArquivosExcel/*.xlsx')
    db.sqlSet(df=df,table='Vendas')

def sqlToStorage():
    print('teste')