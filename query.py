import pandas as pd 
from credentials import credenciais_banco

def consulta_novos_clientes():
    conn = credenciais_banco()
    query = '''
            SELECT
                a.id_cliente_servico,
                b.nome_razaosocial AS nome_cliente,
                b.cpf_cnpj,
                b.id_cliente,
                a.data_venda::date,
                a.data_habilitacao::date,
                CASE WHEN a.validade = 12 THEN 1 ELSE 0 END AS fidelizado,
                c.descricao AS origem_cliente,
                d.descricao AS plano,
                d.valor AS valor_plano,
                end3.nome AS cidade,
                end4.nome AS estado,
                f.name AS vendedor,
                CASE WHEN g.aceito = TRUE THEN 1 ELSE 0 END AS contrato_aceito,
                g.data_aceito::date,
				j.descricao,
                CASE 
                    WHEN g.aceito = TRUE 
                    THEN (g.data_aceito::date - a.data_venda::date) 
                    ELSE -1 
                END AS dias_decorridos_assinatura,
                CASE 
                    WHEN j.descricao IS NOT NULL then 1 else 0 end as promocao_plano
            FROM 
                cliente_servico a
            LEFT JOIN cliente b ON b.id_cliente = a.id_cliente
            LEFT JOIN origem_cliente c ON c.id_origem_cliente = b.id_origem_cliente
            LEFT JOIN servico d ON d.id_servico = a.id_servico
            LEFT JOIN prospecto e ON e.id_cliente = b.id_cliente
            LEFT JOIN users f ON f.id = a.id_usuario_vendedor
            LEFT JOIN cliente_servico_contrato g ON g.id_cliente_servico = a.id_cliente_servico
            LEFT JOIN (
                SELECT id_cliente_servico,
                    id_endereco_numero
                FROM cliente_servico_endereco
                WHERE tipo = 'instalacao'
            ) end1 ON end1.id_cliente_servico = a.id_cliente_servico
            LEFT JOIN endereco_numero end2 ON end2.id_endereco_numero = end1.id_endereco_numero
            LEFT JOIN cidade end3 ON end3.id_cidade = end2.id_cidade
            LEFT JOIN estado end4 ON end4.id_estado = end3.id_estado
            LEFT JOIN servico_status h ON h.id_servico_status = a.id_servico_status
            LEFT JOIN cliente_servico_promocao i ON i.id_cliente_servico = a.id_cliente_servico
            LEFT JOIN promocao j ON j.id_promocao = i.id_promocao
            WHERE a.data_habilitacao IS NULL
            AND a.data_venda::date BETWEEN (CURRENT_DATE - INTERVAL '7 day') AND (CURRENT_DATE - INTERVAL '1 day')
            AND a.origem = 'novo'
            AND h.descricao <> 'Cancelado'
            GROUP BY
                a.id_cliente_servico,
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
                g.data_aceito::date,
                j.descricao
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
                select b.link, a.id_cliente from cliente_arquivo_upload a
                left join arquivo_upload b on b.id_arquivo_upload = a.id_arquivo_upload
                where a.id_cliente = {id_cliente}
                '''.format(id_cliente = id_cliente)
    
    df = pd.read_sql(query,conn)
    conn.close()

    return df

