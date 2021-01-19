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
    def __init__(self,path=''):
        self.path = path

    def load_files(self,base_csv=''):
        colunas = ['SG_UF_RESIDENCIA','NU_IDADE','TP_SEXO','TP_COR_RACA','TP_ANO_CONCLUIU','TP_ESCOLA','IN_NOME_SOCIAL','NU_NOTA_CN','NU_NOTA_CH','NU_NOTA_LC','NU_NOTA_MT','TP_STATUS_REDACAO','NU_NOTA_REDACAO','TP_LINGUA','Q006','Q025','TP_PRESENCA_CN','TP_PRESENCA_CH','TP_PRESENCA_LC','TP_PRESENCA_MT']
        df = pd.read_csv(f'{self.path}/{base_csv}', encoding='ISO-8859-1', usecols=colunas, index_col=None, sep=';')
        return df

    def load_auxiliares(self, arquivo_xls=''):
        #Carrega os dados das tabelas auxiliares
        sheets =[]
        sheets = pd.ExcelFile(f'{self.path}/{arquivo_xls}').sheet_names
        auxiliares = {}

        for sheet in sheets:
            auxiliares[sheet] = pd.read_excel(open(f'{self.path}/{arquivo_xls}', 'rb'), sheet_name=sheet, index_col=None)
        return auxiliares

    def desnormalizar(self, auxiliares={}, df = pd):
        for aux in auxiliares.keys():
            df = pd.merge(left= df, right=auxiliares[aux], on=auxiliares[aux].columns.values[0])

            filtro=['SG_UF_RESIDENCIA'
,'NU_IDADE'
,'NU_NOTA_CN'
,'NU_NOTA_CH'
,'NU_NOTA_LC'
,'NU_NOTA_MT'
,'NU_NOTA_REDACAO'
,'Tipo Fase'
,'Intervalo Idade'
,'Sexo'
,'Raça Declarada'
,'Conclusão Ensino Médio'
,'Tipo Escola'
,'Status da Redação'
,'Idioma Escolhido'
,'Presença Natureza'
,'Presença Humanas'
,'Presença Linguagem'
,'Presença Matemática'
,'Acesso a Internet'
,'Renda Familiar'
,'Nome Social']
        return df[filtro]
