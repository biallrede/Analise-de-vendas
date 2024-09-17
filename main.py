from query import *
import pandas as pd 
from credentials import credenciais_banco_alldata
import schedule
from apscheduler.schedulers.background import BackgroundScheduler
import threading
from datetime import datetime, date

def verifica_apto_mudanca():
    conn = credenciais_banco_alldata()
    df_vendas = consulta_novos_clientes()

    # Adicionando novas colunas ao DataFrame antes do loop
    df_vendas['ja_foi_cliente'] = ''
    df_vendas['motivo_cancelamento'] = ''
    df_vendas['fotos'] = None

    # Usando o iterrows para evitar problemas com o cont e o append
    for index, row in df_vendas.iterrows():
        id_cliente = row['id_cliente']
        cpf_cnpj = row['cpf_cnpj']
        
        df_fotos_cadastro = consulta_fotos_cadastro_cliente(id_cliente)
        df_verifica_base = consulta_cliente_ja_foi_da_base(cpf_cnpj)
        # print(f"df_verifica_base: {df_verifica_base}")
        if df_verifica_base.empty:
            df_vendas.at[index, 'ja_foi_cliente'] = 'Não'
            df_vendas.at[index, 'motivo_cancelamento'] = 'Cliente novo'
        else:
            df_vendas.at[index, 'ja_foi_cliente'] = 'Sim'
            # Se já possui valor em 'motivo_cancelamento', deixa como está, caso contrário insere uma string qualquer (a ajustar)
            df_vendas.at[index, 'motivo_cancelamento'] = row['motivo_cancelamento']
        
        str_fotos = ''
        for index2, row2 in df_fotos_cadastro.iterrows():
            str_fotos = str_fotos + df_fotos_cadastro.at[index2, 'link'] + ';'

        df_vendas.at[index, 'fotos'] = str_fotos
       

    #convertendo os valores do dataframe
    df_vendas['id_cliente_servico'] = df_vendas['id_cliente_servico'].astype(int)
    df_vendas['nome_cliente'] = df_vendas['nome_cliente'].astype(str)
    df_vendas['cpf_cnpj'] = df_vendas['cpf_cnpj'].astype(str)
    df_vendas['id_cliente'] = df_vendas['id_cliente'].astype(int)
    df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])
    df_vendas['origem_cliente'] = df_vendas['origem_cliente'].astype(str)
    df_vendas['plano'] = df_vendas['plano'].astype(str)
    df_vendas['valor'] = df_vendas['valor'].astype(float)
    df_vendas['cidade'] = df_vendas['cidade'].astype(str)
    df_vendas['estado'] = df_vendas['estado'].astype(str)
    df_vendas['vendedor'] = df_vendas['vendedor'].astype(str)
    df_vendas['contrato_aceito'] = df_vendas['contrato_aceito'].astype(int)
    df_vendas['data_aceito'] = pd.to_datetime(df_vendas['data_aceito'])
    df_vendas['ja_foi_cliente'] = df_vendas['ja_foi_cliente'].astype(str)
    df_vendas['motivo_cancelamento'] = df_vendas['motivo_cancelamento'].astype(str)
    df_vendas['fotos'] = df_vendas['fotos'].astype(str)

    df_vendas.to_sql('HUBSOFT_ANALISE_VENDAS', con=conn, if_exists='replace', index=False)
    print('finalizada a inserção dos dados no banco de dados')

scheduler = BackgroundScheduler()

def rotina1():
    verifica_apto_mudanca()

schedule.every().day.at("03:00").do(rotina1)

scheduler.start()

while True:
    schedule.run_pending()
    threading.Event().wait(1)
