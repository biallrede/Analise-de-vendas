from query import *
import pandas as pd 
from fastapi import FastAPI

app = FastAPI() # criando uma instância da classe FastAPI

@app.get("/extrai_dados_vendas/{id_cliente_servico}") 
def verifica_apto_mudanca():
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
        
        if df_verifica_base.empty:
            df_vendas.at[index, 'ja_foi_cliente'] = 'Não'
            df_vendas.at[index, 'motivo_cancelamento'] = 'Cliente novo'
        else:
            df_vendas.at[index, 'ja_foi_cliente'] = 'Sim'
            # Se já possui valor em 'motivo_cancelamento', deixa como está, caso contrário insere uma string qualquer (a ajustar)
            df_vendas.at[index, 'motivo_cancelamento'] = row['motivo_cancelamento']
            
        df_vendas.at[index, 'fotos'] = df_fotos_cadastro
    json_result = df_vendas.to_json(orient='records', force_ascii=False)
    return json_result