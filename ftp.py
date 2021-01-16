import os
from getpass import getpass
from ftplib import FTP as FTP_conection
from os.path import exists
import shutil
import pandas as pd


class FTP:
    def __init__(self, ip, user):
        self.ip = ip
        self.user = user



    def download(self, diretorio='', arquivo = '', dir_destino='', *, verbose=True):
    #Realiza o download de um arquivo especifico de um diretório de FTP
        if verbose: 
            print('-------------------------------------------------------------------------------------')
            print('Conectando...')

        if not os.path.exists(dir_destino):
            os.mkdir(dir_destino)
            print('Diretorio '+ dir_destino + " Criado.")

        if exists(dir_destino + '/'+ arquivo):
            result = input(arquivo +" já existe no diretório, tecle 'S' para sobreescrever:")
            if result.upper() != 'S':
                if verbose: 
                    print('Download Cancelado.')
                    print('-------------------------------------------------------------------------------------')
                return
        try:
            cx = FTP_conection(self.ip)
            cx.login(user = self.user[0], passwd=self.user[1])
            cx.cwd(diretorio)
            #cx.retrlines('LIST')
            with open(dir_destino + '/' + arquivo, 'wb') as arquivo_local:
                print('Baixando...')
                print(arquivo)
                cx.retrbinary('RETR ' + arquivo, arquivo_local.write, 1024)
                
        except Exception as erro:
            print(erro)

        else:
            cx.quit()
            '''if os.path.isfile('input_FTP/' + arquivo):
                os.remove('input_FTP/' + arquivo)
            shutil.move(arquivo, 'input_FTP/')'''
            if verbose: 
                print("Download Concluído!")
                print("Conexção Encerrada.")
                print('-------------------------------------------------------------------------------------')

    def upload(self, df = pd, nome_arquivo='', diretorio=''):
        try:
            if not os.path.exists('Uploads'):
                os.mkdir('Uploads')
            print(f"Gerando {nome_arquivo} arquivo para Upload")
            df.to_csv(f'Uploads/{nome_arquivo}', encoding = 'ISO-8859-1', index = False, sep=";")
        except Exception as erro:
            print(erro)

        try:
            cx = FTP_conection(self.ip)
            cx.login(user = self.user[0], passwd=self.user[1])
            local = open(f'Uploads/{nome_arquivo}', 'rb')
            cx.cwd(diretorio)    
            cx.storbinary('STOR ' + nome_arquivo, local, 1024)
            local.close()
            cx.quit()
        except Exception as erro:
            print(erro)
        print("Upload Concluído")
        print('-------------------------------------------------------------------------------------')