import functions
import time


teste = functions.RAIZ()
teste.check()
log = functions.LOG(time.localtime())
ftp = functions.FTP_Connection('localhost')
etl = functions.ETL('Downloads')
historico = {}
auxiliares = ['']
tmp = functions.TMP()

while True:
    ftp.conectar('leandric','teste123')
    ftp.diretorio('DADOS')
    ftp.down('MICRODADOS_ENEM_2019.csv')
    ftp.diretorio('../ANALISE')
    ftp.down('Tabelas Auxiliares.xlsx')
    dados = etl.load_files('MICRODADOS_ENEM_2019.csv')
    auxilar = etl.load_auxiliares('Tabelas Auxiliares.xlsx')
    dados = etl.desnormalizar(auxilar, dados)
    dados.to_csv('Uploads/DW.csv', encoding='ISO-8859-1', index=False,sep=";")

    tmp.wait(360)