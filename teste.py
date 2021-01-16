import ftp
import pandas as pd


conexao = ftp3.FTP('localhost', ('leandric','teste123'))

#conexao.download(diretorio='DADOS', arquivo='MICRODADOS_ENEM_2019.csv', dir_destino='downloads',verbose=True)

print('carregando DataFrame...')
df_microdados = pd.read_csv('downloads/MICRODADOS_ENEM_2019.csv',encoding='ISO-8859-1', index_col=None, sep = ';')


conexao.upload(df=df_microdados, nome_arquivo = 'teste.csv', diretorio='FTP')