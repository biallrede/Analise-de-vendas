import psycopg2


def credenciais_banco():
    conn = psycopg2.connect(
                        host='134.65.24.116',
                        port='9432',
                        database='hubsoft',
                        user='erick_leitura',
                        password='73f4cc9b2667d6c44d20d1a0d612b26c5e1763c2')
    
    return conn