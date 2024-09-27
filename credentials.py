import psycopg2
from sqlalchemy import create_engine
import urllib
import pyodbc

def credenciais_banco():
    conn = psycopg2.connect(
                        host='134.65.24.116',
                        port='9432',
                        database='hubsoft',
                        user='erick_leitura',
                        password='73f4cc9b2667d6c44d20d1a0d612b26c5e1763c2')
    
    return conn

def credenciais_banco_alldata():
# Configuração da conexão com o banco de dados para rodar no servidor
    server = '187.121.151.19'
    database = 'DB_ALLNEXUS'
    username = 'user_allnexus'
    password = 'uKl041xn8HIw0WF'

    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server'
    engine = create_engine(connection_string,fast_executemany=True)
    return engine

# def credenciais_banco_alldata():
# # Configuração da conexão com o banco de dados para rodar local
#     server = '187.121.151.19'
#     database = 'DB_ALLNEXUS'
#     username = 'user_allnexus'
#     password = 'uKl041xn8HIw0WF'

#     connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
#     engine = create_engine(connection_string,fast_executemany=True)
#     return engine

