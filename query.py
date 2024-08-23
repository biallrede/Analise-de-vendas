import pandas as pd 
from credentials import credenciais_banco

def consulta_novos_clientes():
    conn = credenciais_banco()
    query = '''
                select 
                a.id_cliente_servico,
                b.nome_razaosocial as nome_cliente,
                b.cpf_cnpj,
				b.id_cliente,
                a.data_venda::date,
                c.descricao as origem_cliente,
                d.descricao as plano,
                d.valor,
                end3.nome as cidade,
                end4.nome as estado,
                f.name as vendedor,
                g.aceito as contrato_aceito ,
                g.data_aceito::date
                from cliente_servico a
                left join cliente b on b.id_cliente = a.id_cliente
                left join origem_cliente c on c.id_origem_cliente = b.id_origem_cliente
                left join servico d on d.id_servico = a.id_servico 
                left join prospecto e on e.id_cliente = b.id_cliente
                left join users f on f.id  = e.id_usuario
                left join cliente_servico_contrato g on g.id_cliente_servico = a.id_cliente_servico 
                left join (select id_cliente_servico, 
                    id_endereco_numero
                    from cliente_servico_endereco
                    where tipo = 'instalacao'
                ) end1 on end1.id_cliente_servico = a.id_cliente_servico
                left join endereco_numero end2 on end2.id_endereco_numero = end1.id_endereco_numero
                left join cidade end3 on end3.id_cidade = end2.id_cidade
                left join estado end4 on end4.id_estado = end3.id_estado
                where a.data_habilitacao isnull
                and a.data_venda::date = CURRENT_DATE - interval '1 day' 
                and a.origem = 'novo'
                group by a.id_cliente_servico,
                b.nome_razaosocial,
				b.cpf_cnpj,
				b.id_cliente,
                a.data_venda::date,
                c.descricao,
                d.descricao,
                d.valor,
                end3.nome,
                end4.nome,
                f.name,
                g.aceito,
                g.data_aceito::date
                '''
    
    df = pd.read_sql(query,conn)
    conn.close()

    return df

def consulta_cliente_ja_foi_da_base(cpf_compara):
    conn = credenciais_banco()
    query = '''
                select a.id_cliente_servico,
                c.descricao as motivo_cancelamento
                from cliente_servico a
                left join cliente b on b.id_cliente = a.id_cliente
                left join motivo_cancelamento c on c.id_motivo_cancelamento = a.id_motivo_cancelamento
                where a.data_habilitacao notnull
                and a.data_cancelamento notnull
                and b.cpf_cnpj = '{cpf_compara}'
                
                '''.format(cpf_compara = cpf_compara)
    
    df = pd.read_sql(query,conn)
    conn.close()

    return df

def consulta_fotos_cadastro_cliente(id_cliente):
    conn = credenciais_banco()
    query = '''
                select link from cliente_arquivo_upload a
                left join arquivo_upload b on b.id_arquivo_upload = a.id_arquivo_upload
                where a.id_cliente = {id_cliente}
                '''.format(id_cliente = id_cliente)
    
    df = pd.read_sql(query,conn)
    conn.close()

    return df