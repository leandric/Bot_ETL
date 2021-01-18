import os
from ftplib import FTP
import pandas as pd

class RAIZ:
    def __init__(self):
        pass

    def check(self):
        #checa se os diretorios existem e os cria caso não exista
        if not os.path.exists('Downloads'):
            os.mkdir('Downloads')
        if not os.path.exists('Uploads'):
            os.mkdir('Uploads')
        if not os.path.exists('Logs'):
            os.mkdir('Logs')
        return True


class LOG:
    def __init__(self, agora):
        self.agora = agora
        self.log = open(f'Logs/log_{self.agora[0]}{self.agora[1]}{self.agora[2]}_{self.agora[3]}{self.agora[4]}.csv','w+')
        self.log.write('Data;Arquivo;Download;Upload;Trans;CLS;Email;Obs')

    def gerar(self, Arquivo, Download, Upload, Trans, CLS, Email, Obs):
        #gera uma linha no arquivo de log
        self.log.write(f'{self.agora[0]}-{self.agora[1]}-{self.agora[2]} {self.agora[3]}:{self.agora[3]};{Arquivo};{Download};{Upload};{Trans};{CLS};{Email};{Obs}')

    def finalizar(self):
        #finaliza o arquivo de log
        self.log.close()


class FTP_Connection:
    def __init__(self, ip='localhost'):
        self.ip = ip
        self.conection = FTP(self.ip)

    def conectar(self, user='', pwd=''):
        #realiza login no FTPserver
        self.conection.login(user, pwd)

    def diretorio(self, diretorio=''):
        #aponta para o diretorio informado no parametro no FTPserver
        self.conection.cwd(diretorio)

    def listar(self):
        #lista os arquivos dentro do diretorio
        return self.conection.retrlines('LIST')

    def down(self, arquivo = ''):
        #fazer o dowload de um arquivo
        with open(f'Downloads/{arquivo}','wb') as local:
            self.conection.retrbinary('RETR ' + arquivo, local.write, 1024)
            local.close()

    def up(self,arquivo):
        #faz o upload de um arquivo
        try:
            local = open(f'Uploads/{arquivo}', rb)
            self.conection.storbinary('STOR ' + arquivo, local, 1024)
        except Exception as erro:
            return erro


class ETL:
    def __init__(self, arquivo_csv='', colunas=[]):
        self.df = pd.to_csv()
        self.variaveis