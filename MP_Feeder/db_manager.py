# db_manager.py
import mariadb as mdb
import pandas as pd
import logging
import time
from datetime import datetime, date

# --- FUNÇÃO DE CONEXÃO AUXILIAR ---
def _conectar_db(DB_CONFIG):
    """Função auxiliar interna para abrir conexão com o DB correto."""
    return mdb.connect(**DB_CONFIG, database="dbDrogamais")

# ============================================
# SEÇÃO DE PRODUTOS
# ============================================

def pegar_ultima_att_gtins(DB_CONFIG):
    """
    Busca a data de atualização mais recente da tabela de produtos.
    """
    print("##### COLETANDO ÚLTIMA ATUALIZAÇÃO DOS GTINS #####")
    logging.info("##### COLETANDO ÚLTIMA ATUALIZAÇÃO DOS GTINS #####")

    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    sql = "SELECT MAX(data_atualizacao) FROM bronze_menorPreco_produtos"
    
    cursor.execute(sql)
    resultado = cursor.fetchone()
    cursor.close()
    conn.close()

    ultima_att_gtins_raw = resultado[0] if resultado else None

    # --- LÓGICA DE TRATAMENTO ROBUSTA ---
    if not ultima_att_gtins_raw:
        return None
    if isinstance(ultima_att_gtins_raw, datetime):
        return ultima_att_gtins_raw.date()
    if isinstance(ultima_att_gtins_raw, date):
        return ultima_att_gtins_raw

    # ... (lógica de conversão de string)
    try:
        return datetime.strptime(str(ultima_att_gtins_raw), "%Y-%m-%d %H:%M:%S").date()
    except ValueError:
        try:
            return datetime.strptime(str(ultima_att_gtins_raw), "%Y-%m-%d").date()
        except ValueError:
            logging.error(f"Formato de data desconhecido: {ultima_att_gtins_raw}")
            return None

def fetch_dados_vendas_para_produtos(DB_CONFIG):
    """
    (ETL - Extract) Busca os dados brutos de vendas por valor e qtd.
    A lógica de Transform (merge, drop) foi movida para o main.py.
    """
    logging.info("##### REFAZENDO LISTA DE PRODUTOS (ETAPA 1: EXTRAÇÃO) #####")
    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()

    # 1. Buscando produtos mais vendidos por valor
    logging.info("1. Buscando produtos mais vendidos por valor")
    sql_valor = """
        SELECT 
            v.codigo_interno_produto, 
            v.GTIN, 
            v.descricao_produto, 
            v.apresentacao_produto, 
            v.nome_fantasia_fabricante,
            SUM(v.valor_liquido_total) AS valor_total
        FROM 
            bronze_plugpharma_vendas v
        WHERE 
            v.data_venda >= CURDATE() - INTERVAL 90 DAY
        GROUP BY 
            v.codigo_interno_produto
        ORDER BY valor_total DESC
        LIMIT 3000;
    """
    cursor.execute(sql_valor)
    resultados = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    produtos_por_valor = pd.DataFrame(resultados, columns=columns)

    # 2. Buscando produtos mais vendidos por quantidade
    logging.info("2. Buscando produtos mais vendidos por quantidade")
    sql_qtd = """
        SELECT 
            v.codigo_interno_produto, 
            v.GTIN, 
            v.descricao_produto, 
            v.apresentacao_produto, 
            v.nome_fantasia_fabricante,
            SUM(v.qtd_de_produtos) AS qtd_total
        FROM 
            bronze_plugpharma_vendas v
        WHERE 
            v.data_venda >= CURDATE() - INTERVAL 90 DAY
        GROUP BY 
            v.codigo_interno_produto
        ORDER BY qtd_total DESC
        LIMIT 3000;
    """
    cursor.execute(sql_qtd)
    resultados = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    produtos_por_qtd = pd.DataFrame(resultados, columns=columns)

    cursor.close()
    conn.close()
    
    # 3. Retorna os DFs brutos para o main.py fazer a transformação
    return produtos_por_valor, produtos_por_qtd

def insert_produtos_atualizados(DB_CONFIG, produtos_df):
    """
    INSERT Incremental: Grava uma nova linha para cada execução.
    """
    if produtos_df.empty: return

    logging.info(f"4. Inserindo {len(produtos_df)} produtos (Incremental)...")
    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    
    agora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # INSERT simples, sem 'ON DUPLICATE KEY UPDATE'
    sql = """
        INSERT INTO bronze_menorPreco_produtos 
        (gtin, id_produto, descricao, fabricante, apresentacao, tipo, data_atualizacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    sucessos = 0
    for _, row in produtos_df.iterrows():
        try:
            cursor.execute(sql, (
                row["GTIN"], 
                row["codigo_interno_produto"], 
                row["descricao_produto"], 
                row["nome_fantasia_fabricante"], 
                row["apresentacao_produto"],
                row.get("tipo"), # Pega o tipo vindo do DataFrame
                agora
            ))
            sucessos += 1
        except Exception as e:
            logging.error(f"Erro ao inserir {row['GTIN']}: {e}")

    conn.commit()
    logging.info(f"Sucesso: {sucessos} produtos inseridos.")
    cursor.close()
    conn.close()

def coletar_produtos_no_banco(DB_CONFIG):
    """
    Coleta os GTINs da lista de produtos do dia.
    """
    print("##### COLETANDO OS PRODUTOS NO BANCO #####")
    logging.info("##### COLETANDO OS PRODUTOS NO BANCO #####")

    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()

    sql_data = "SELECT MAX(data_atualizacao) FROM bronze_menorPreco_produtos"
    cursor.execute(sql_data)
    maior_data = cursor.fetchone()
    data_obj = maior_data[0] if maior_data else None

    if not data_obj:
        logging.error("Nenhuma data de atualização encontrada. Tabela de produtos pode estar vazia.")
        return pd.DataFrame(columns=["gtin"]) 
    
    # Converte para objeto date se for datetime
    if isinstance(data_obj, datetime):
        data_obj = data_obj.date()

    sql_gtin = "SELECT gtin FROM bronze_menorPreco_produtos WHERE DATE(data_atualizacao) = %s"
    cursor.execute(sql_gtin, (data_obj,)) 
    gtins = cursor.fetchall()
    
    cursor.close()
    conn.close()

    EANs = pd.DataFrame(gtins, columns=["gtin"])
    return EANs

def fetch_gtins_principais(DB_CONFIG, codigos_internos_list):
    """
    Busca GTIN principal e agora também o TIPO DE PRODUTO.
    """
    if not codigos_internos_list:
        logging.info("Nenhum codigo interno fornecido.")
        return pd.DataFrame(columns=["codigo_interno_produto", "GTIN_principal", "tipo_produto"])
    
    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()

    try:
        placeholders = ', '.join(['%s'] * len(codigos_internos_list))
        # ADICIONADO: tipo_produto no SELECT
        sql = f"""
            SELECT codigo_interno, codigo_barras, tipo_produto
            FROM bronze_plugpharma_produtos 
            WHERE codigo_principal = 1 AND codigo_interno IN ({placeholders})
        """
        cursor.execute(sql, tuple(codigos_internos_list))
        
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=columns)
        
        return df.rename(columns={'codigo_barras': 'GTIN_principal', 'codigo_interno': 'codigo_interno_produto'})

    except Exception as e:
        logging.error(f"Erro ao buscar GTINs: {e}")
        return pd.DataFrame(columns=["codigo_interno_produto", "GTIN_principal", "tipo_produto"])
    finally:
        cursor.close()
        conn.close()

# ============================================
# SEÇÃO DE LOJAS E GEOHASH
# ============================================

def pegar_geohashs_BD(DB_CONFIG):
    """
    Coleta os Geohashs das cidades onde temos lojas ativas no 'COMPARADOR DE PREÇOS'.
    """
    print("##### PEGANDO GEOHASHS #####")
    logging.info("##### PEGANDO GEOHASHS #####")

    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()

    sql = """
        WITH auditorias_filtradas AS (
            SELECT DISTINCT userEmail
            FROM dbSults.tb_report_auditoria_embedded
            WHERE reportName = 'COMPARADOR DE PREÇOS'
        )
        SELECT 
            p.geohash
        FROM 
            dbDrogamais.bronze_lojas AS b
        JOIN 
            auditorias_filtradas AS a 
            -- Força o mesmo collate nos dois lados
            ON a.userEmail COLLATE utf8mb4_uca1400_ai_ci = b.email COLLATE utf8mb4_uca1400_ai_ci
        JOIN 
            dbDrogamais.bronze_cidades AS p 
            -- Força o mesmo collate nos dois lados
            ON b.cidade COLLATE utf8mb4_uca1400_ai_ci = p.cidade_normalizada COLLATE utf8mb4_uca1400_ai_ci
        GROUP BY 
            p.geohash; 
    """
    
    cursor.execute(sql)
    geohash = cursor.fetchall()
    print(f"##### { len(geohash)} GEOHASHS COLETADOS NO BANCO #####")
    logging.info(f"##### { len(geohash)} GEOHASHS COLETADOS NO BANCO #####")
    
    cursor.close()
    conn.close()

    Geohashs = pd.DataFrame(geohash, columns=["geohash"])
    return Geohashs

def coletar_lojas_do_banco(DB_CONFIG):
    """
    Coleta a lista de IDs de lojas já cadastrados E que possuem coordenadas completas.
    """
    print("##### COLETANDO LOJAS CADASTRADAS NO BANCO (COM COORDENADAS) #####")
    logging.info("##### COLETANDO LOJAS CADASTRADAS NO BANCO (COM COORDENADAS) #####")

    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()

    sql = "SELECT id_loja FROM bronze_menorPreco_lojas WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    
    cursor.execute(sql)
    lista_lojas = cursor.fetchall()
    cursor.close()
    conn.close()

    Lojas = pd.DataFrame(lista_lojas, columns=["id_loja"])
    return Lojas

def inserir_lojas_sc(Lojas_SC, now_obj, DB_CONFIG):
    """
    (ETL - Load) Insere um DataFrame de lojas novas (sem cadastro) no banco.
    Usa INSERT IGNORE para evitar falhas com duplicatas.
    """
    print("##### INSERINDO LOJAS NÃO CADASTRADAS #####")
    logging.info("##### INSERINDO LOJAS NÃO CADASTRADAS #####")
    
    if Lojas_SC.empty:
        print("##### NENHUMA LOJA NOVA PARA INSERIR. #####")
        logging.info("##### NENHUMA LOJA NOVA PARA INSERIR. #####")
        return

    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    Lojas_SC = Lojas_SC.astype(object).where(pd.notnull(Lojas_SC), None)

    data_tuples = [
        (
            row.id_loja, row.nome_fantasia, row.razao_social, row.logradouro,
            row.Latitude, row.Longitude, 
            row.cidade,
            row.geohash, now_obj
        )
        for row in Lojas_SC.itertuples(index=False)
    ]

    sql = """
        INSERT INTO bronze_menorPreco_lojas
        (id_loja, nome_fantasia, razao_social, logradouro, latitude, longitude, cidade, geohash, data_atualizacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            geohash = VALUES(geohash),
            data_atualizacao = VALUES(data_atualizacao);
    """

    try:
        start_time = time.time()
        print(f"Iniciando 'executemany' para {len(data_tuples)} lojas...")
        
        cursor.executemany(sql, data_tuples)
        conn.commit()
        end_time = time.time()
        
        print(f"Commit de {len(data_tuples)} lojas concluído. Tempo: {end_time - start_time:.2f} segundos.")
        print(f"##### {cursor.rowcount} (Reportado pelo driver) NOVAS LOJAS INSERIDAS #####")
        logging.info(f"##### {cursor.rowcount} (Reportado pelo driver) NOVAS LOJAS INSERIDAS #####")

    except Exception as e:
        print(f"❌ Erro no 'executemany' de lojas: {e}")
        logging.error(f"❌ Erro no 'executemany' de lojas: {e}", exc_info=True)
        conn.rollback()
        raise e 
    finally:
        cursor.close()
        conn.close()
    
# ============================================
# SEÇÃO DE NOTAS FISCAIS
# ============================================

def pegar_ultimo_gtin(DB_CONFIG):
    """
    Pega o GTIN da última nota fiscal PROCESSADA (data_atualizacao),
    para garantir a rotação correta dos grupos.
    """
    print("##### COLETANDO ÚLTIMO GTIN INSERIDO/ATUALIZADO #####")
    logging.info("##### COLETANDO ÚLTIMO GTIN INSERIDO/ATUALIZADO #####")

    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    
    # ALTERAÇÃO AQUI: Mudamos de 'ORDER BY date' para 'ORDER BY data_atualizacao'
    sql = "SELECT gtin FROM bronze_menorPreco_notas ORDER BY data_atualizacao DESC LIMIT 1"
    
    cursor.execute(sql)
    dados = cursor.fetchone()
    cursor.close()
    conn.close()

    gtin = dados[0] if dados else None 
    return gtin

def inserir_notas(Notas, now_obj, DB_CONFIG):
    """
    (ETL - Load) Insere um DataFrame de notas fiscais no banco.
    Usa INSERT IGNORE para evitar falhas com duplicatas.
    """
    print("##### INSERINDO NOTAS NO BANCO #####")
    logging.info("##### INSERINDO NOTAS NO BANCO #####")
    
    if Notas.empty:
        print("##### NENHUMA NOTA NOVA PARA INSERIR. #####")
        logging.info("##### NENHUMA NOTA NOVA PARA INSERIR. #####")
        return

    conn = _conectar_db(DB_CONFIG)
    cursor = conn.cursor()
    Notas = Notas.where(pd.notnull(Notas), None) 

    data_tuples = []
    for row in Notas.itertuples(index=False):
        # CORREÇÃO DE NORMALIZAÇÃO DE GTIN
        gtin_normalizado = str(row.gtin).zfill(14)
        data_tuples.append((
            row.id_nota, row.datahora, row.id_loja, row.geohash, 
            gtin_normalizado, # <-- GTIN Normalizado
            row.descricao,
            row.valor_desconto, row.valor_tabela, row.valor, 
            row.cidade, now_obj
        ))

    sql = """
        INSERT INTO bronze_menorPreco_notas 
        (id_nota, date, id_loja, geohash, gtin, descricao, valor_desconto, valor_tabela, valor, cidade, data_atualizacao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            data_atualizacao = VALUES(data_atualizacao);
    """

    try:
        start_time = time.time()
        print(f"Iniciando 'executemany' para {len(data_tuples)} notas...")
        
        cursor.executemany(sql, data_tuples)
        conn.commit()
        end_time = time.time()
        
        print(f"Commit de {len(data_tuples)} notas concluído. Tempo: {end_time - start_time:.2f} segundos.")
        print(f"##### {cursor.rowcount} (Reportado pelo driver) NOVAS NOTAS INSERIDAS #####")
        logging.info(f"##### {cursor.rowcount} (Reportado pelo driver) NOVAS NOTAS INSERIDAS #####")

    except Exception as e:
        print(f"❌ Erro no 'executemany': {e}")
        logging.error(f"❌ Erro no 'executemany': {e}", exc_info=True)
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()