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

# def credenciais_banco_alldata():
# # Configuração da conexão com o banco de dados
#     conn = pyodbc.connect(
#         'DRIVER={ODBC Driver 18 for SQL Server};'
#         'Server=187.121.151.19;' 
#         'Database=DB_BASE;'
#         'UID=user_allnexus;'
#         'PWD=uKl041xn8HIw0WF;'
#         'TrustServerCertificate=yes;'
#     )
#     return conn

def credenciais_banco_alldata():
# Configuração da conexão com o banco de dados
    server = '187.121.151.19'
    database = 'DB_ALLNEXUS'
    username = 'user_allnexus'
    password = 'uKl041xn8HIw0WF'

    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server'
    engine = create_engine(connection_string,fast_executemany=True)
    # conn = pyodbc.connect(
    #     'DRIVER={ODBC Driver 17 for SQL Server};'
    #     'Server=187.121.151.19;' 
    #     'Database=DB_ALLNEXUS;'
    #     'UID=user_allnexus;'
    #     'PWD=uKl041xn8HIw0WF;'
    #     'TrustServerCertificate=yes;'
    # )
    return engine

# def credenciais_banco_alldata():
# # Configuração da conexão com o banco de dados
#     params = urllib.parse.quote_plus(
#         'DRIVER={ODBC Driver 17 for SQL Server};'
#         'Server=187.121.151.19;' 
#         'Database=DB_ALLNEXUS;'
#         'UID=user_allnexus;'
#         'PWD=uKl041xn8HIw0WF;'
#         'TrustServerCertificate=yes;'
#     )
#     engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
#     return engine