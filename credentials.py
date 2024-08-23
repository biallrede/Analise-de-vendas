import pyodbc


def credenciais_banco():
    conn = pyodbc.connect('Driver={PostgreSQL ODBC Driver(UNICODE)};'
                        'Server=134.65.24.116;'
                        'Port=9432;'
                        'Database=hubsoft;'
                        'Uid=erick_leitura;'
                        'Pwd=73f4cc9b2667d6c44d20d1a0d612b26c5e1763c2;')
    
    return conn