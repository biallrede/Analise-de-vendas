from query import *
import pandas as pd 
from credentials import credenciais_banco_alldata
import schedule
from apscheduler.schedulers.background import BackgroundScheduler
import threading
import time
import re

def verifica_apto_mudanca():
    conn = credenciais_banco_alldata()
    df_vendas = consulta_novos_clientes()
    valor = 0
    # Adicionando novas colunas ao DataFrame antes do loop
    df_vendas['ja_foi_cliente'] = ''
    df_vendas['motivo_cancelamento'] = ''
    df_vendas['valor_desconto'] = None
    df_vendas['plano_muda_valor'] = ''
    df_vendas['fotos'] = None
    df_vendas['meses'] = 0
    df_vendas['desconto_plano'] = 0
    df_vendas['valor_plano'] = df_vendas['valor_plano'].astype(str)
    df_vendas['valor_plano'] = df_vendas['valor_plano'].str.replace(',', '.')
    df_vendas['valor_plano'] = pd.to_numeric(df_vendas['valor_plano'], errors='coerce')

    
    # Usando o iterrows para evitar problemas com o cont e o append
    for index, row in df_vendas.iterrows():
        id_cliente = row['id_cliente']
        cpf_cnpj = row['cpf_cnpj']
        
        df_fotos_cadastro = consulta_fotos_cadastro_cliente(id_cliente)
        df_verifica_base = consulta_cliente_ja_foi_da_base(cpf_cnpj)
       
        if df_verifica_base.empty:
            df_vendas.at[index, 'ja_foi_cliente'] = 'Não'
            df_vendas.at[index, 'motivo_cancelamento'] = 'Cliente novo'
        else:
            df_vendas.at[index, 'ja_foi_cliente'] = 'Sim'
            # Se já possui valor em 'motivo_cancelamento', deixa como está, caso contrário insere uma string qualquer (a ajustar)
            df_vendas.at[index, 'motivo_cancelamento'] = df_verifica_base.loc[0,'motivo_cancelamento']
        
        # cria coluna para verificar se o plano do cliente muda de valor ou não 
        if df_vendas.at[index,'promocao_plano'] == 1:
            # Aplicando a função ao DataFrame
            match = re.search(r'\((\d+) MESES (\d+,\d{2})\)', df_vendas.at[index,'descricao'])
            if match:
                meses = int(match.group(1))  # Primeiro grupo captura os meses
                valor = float(match.group(2).replace(',', '.'))
                df_vendas.at[index,'meses'] = meses
                df_vendas.at[index,'desconto_plano'] = float(valor)
               

            df_vendas.at[index,'plano_muda_valor'] = 'Sim'
            # print('valor: ',df_vendas.at[index,'desconto_plano'] )
            df_vendas.at[index,'valor_desconto'] = df_vendas.at[index,'valor_plano'] - valor
        else:
            df_vendas.at[index,'plano_muda_valor'] = 'Não'
            df_vendas.at[index,'valor_desconto'] = 0
            df_vendas.at[index,'meses'] = 0
            


        str_fotos = ''
        for index2, row2 in df_fotos_cadastro.iterrows():
            str_fotos = str_fotos + df_fotos_cadastro.at[index2, 'link'] + ';'

        df_vendas.at[index, 'fotos'] = str_fotos
    
    df_vendas = pd.DataFrame(df_vendas)
    df_vendas = df_vendas.drop(['descricao','desconto_plano'],axis=1)
    #convertendo os valores do dataframe
    df_vendas['id_cliente_servico'] = df_vendas['id_cliente_servico'].astype(int)
    df_vendas['nome_cliente'] = df_vendas['nome_cliente'].astype(str)
    df_vendas['cpf_cnpj'] = df_vendas['cpf_cnpj'].astype(str)
    df_vendas['id_cliente'] = df_vendas['id_cliente'].astype(int)
    df_vendas['data_venda'] = pd.to_datetime(df_vendas['data_venda'])
    df_vendas['data_habilitacao'] = pd.to_datetime(df_vendas['data_habilitacao'])
    df_vendas['origem_cliente'] = df_vendas['origem_cliente'].astype(str)
    df_vendas['plano'] = df_vendas['plano'].astype(str)
    df_vendas['valor_plano'] = df_vendas['valor_plano'].astype(float)
    df_vendas['cidade'] = df_vendas['cidade'].astype(str)
    df_vendas['estado'] = df_vendas['estado'].astype(str)
    df_vendas['vendedor'] = df_vendas['vendedor'].astype(str)
    df_vendas['contrato_aceito'] = df_vendas['contrato_aceito'].astype(int)
    df_vendas['data_aceito'] = pd.to_datetime(df_vendas['data_aceito'])
    df_vendas['ja_foi_cliente'] = df_vendas['ja_foi_cliente'].astype(str)
    df_vendas['motivo_cancelamento'] = df_vendas['motivo_cancelamento'].astype(str)
    df_vendas['fotos'] = df_vendas['fotos'].astype(str)
    df_vendas['plano_muda_valor'] = df_vendas['plano_muda_valor'].astype(str)
    df_vendas['valor_desconto'] = df_vendas['valor_desconto'].astype(float)
    df_vendas['fidelizado'] = df_vendas['fidelizado'].astype(int)
    df_vendas['dias_decorridos_assinatura'] = df_vendas['dias_decorridos_assinatura'].astype(int)
    df_vendas['promocao_plano'] = df_vendas['promocao_plano'].astype(str)
    df_vendas['meses'] = df_vendas['meses'].astype(int)

    for tentativa in range(5):
        try:
            df_vendas.to_sql('HUBSOFT_ANALISE_VENDAS', con=conn, if_exists='replace', index=False)
            print('finalizada a inserção dos dados no banco de dados')
            break # Para sair do loop
        except Exception as e:
            print(f'Não foi possível inserir os dados no banco de dados. Tentativa {tentativa+1}: {e}'.format(tentativa,e))
            time.sleep(30)  # segundos

scheduler = BackgroundScheduler()

def rotina1():
    verifica_apto_mudanca()

schedule.every().day.at("03:00").do(rotina1)

scheduler.start()

while True:
    schedule.run_pending()
    threading.Event().wait(1)

# verifica_apto_mudanca()