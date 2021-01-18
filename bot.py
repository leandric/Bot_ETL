import functions
import time


teste = functions.RAIZ()
log = functions.LOG(time.localtime())
ftp = functions.FTP_Connection('localhost')
historico = {}

while True:
    teste.check()

    ftp.conectar('leandric','teste123')

    ftp.diretorio('DADOS')
    ftp.down('MICRODADOS_ENEM_2019.csv')

    ftp.diretorio('../ANALISE')
    ftp.listar()
    ftp.down('Tabelas Auxiliares.xlsx')

    #log.gerar()

    time.sleep(30000)