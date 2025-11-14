import pandas as pd
from datetime import datetime, timezone, timedelta

# --- Imports do Prefect ---
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.blocks.system import Secret 

# --- Imports das suas funções de lógica (do seu projeto) ---
from MP_Feeder.db_manager import (
    pegar_ultima_att_gtins, fetch_dados_vendas_para_produtos, 
    insert_produtos_atualizados, pegar_geohashs_BD,
    coletar_produtos_no_banco, pegar_ultimo_gtin, coletar_lojas_do_banco,
    inserir_lojas_sc, inserir_notas,
    fetch_gtins_principais
)
from MP_Feeder.api_services import (
    buscar_notas, buscar_lat_lon_lojas_sc_nominatim, mandarMSG
)
from MP_Feeder.etl_utils import (
    transformar_dados_produtos,
    grupo_eans_selecionados, # Usando a nova versão sem arquivo
    gerar_consultas,
    setup_logging
)

# --- 1. TASK DE CREDENCIAIS ---
# Esta é a task que substitui o config.py
@task
def load_credentials():
    """Carrega as credenciais dos Blocks do Prefect."""
    logger = get_run_logger()
    logger.info("Carregando credenciais dos Blocks...")

    # O Bloco Secret já retorna um dicionário!
    db_config = Secret.load("db-config-mp").get()

    # O Bloco Secret já retorna um dicionário!
    telegram_config = Secret.load("telegram-config").get()
    
    # Extrai os valores do dicionário
    telegram_token = telegram_config.get("token")
    telegram_chat_id = telegram_config.get("chat_id")

    logger.info("Credenciais carregadas com sucesso.")
    return db_config, telegram_token, telegram_chat_id

# --- 2. TRANSFORME TODAS AS SUAS FUNÇÕES EM TASKS ---
# Todas as funções que usam credenciais agora as recebem como parâmetros.

# --- Tasks do db_manager.py ---
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def pegar_ultima_att_gtins_task(DB_CONFIG):
    return pegar_ultima_att_gtins(DB_CONFIG)

@task
def fetch_dados_vendas_task(DB_CONFIG):
    return fetch_dados_vendas_para_produtos(DB_CONFIG)

@task(retries=3, retry_delay_seconds=30)
def insert_produtos_atualizados_task(DB_CONFIG, produtos_df):
    insert_produtos_atualizados(DB_CONFIG, produtos_df)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=6))
def pegar_geohashs_task(DB_CONFIG):
    return pegar_geohashs_BD(DB_CONFIG)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def coletar_produtos_no_banco_task(DB_CONFIG):
    return coletar_produtos_no_banco(DB_CONFIG)

@task
def pegar_ultimo_gtin_task(DB_CONFIG):
    return pegar_ultimo_gtin(DB_CONFIG)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def coletar_lojas_do_banco_task(DB_CONFIG):
    return coletar_lojas_do_banco(DB_CONFIG)

@task(retries=3, retry_delay_seconds=120)
def inserir_lojas_sc_task(DB_CONFIG, Lojas_SC, now_obj):
    inserir_lojas_sc(Lojas_SC, now_obj, DB_CONFIG)

@task(retries=3, retry_delay_seconds=120)
def inserir_notas_task(DB_CONFIG, Notas, now_obj):
    inserir_notas(Notas, now_obj, DB_CONFIG)

@task
def fetch_gtins_principais_task(DB_CONFIG, codigos_para_buscar):
    return fetch_gtins_principais(DB_CONFIG, codigos_para_buscar)

# --- Tasks do etl_utils.py ---
@task
def transformar_dados_produtos_task(produtos_por_valor, produtos_por_qtd):
    return transformar_dados_produtos(produtos_por_valor, produtos_por_qtd)

@task
def grupo_eans_selecionados_task(EANs, ult_gtin):
    # Usa a nova função de etl_utils que não precisa de 'arquivo_indice'
    return grupo_eans_selecionados(EANs, ult_gtin)

@task
def gerar_consultas_task(Geohashs, EANs_Selecionados):
    return gerar_consultas(Geohashs, EANs_Selecionados)

# --- Tasks do api_services.py ---
@task(retries=2, retry_delay_seconds=30)
def buscar_notas_task(Consultas, Lojas):
    # Usa a nova função de api_services que não precisa de tokens
    return buscar_notas(Consultas, Lojas)

@task(retries=3, retry_delay_seconds=5)
def buscar_lat_lon_task(Lojas_SC_geral_sem_duplicatas):
    return buscar_lat_lon_lojas_sc_nominatim(Lojas_SC_geral_sem_duplicatas)

@task
def mandarMSG_task(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID):
    mandarMSG(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)


# --- 3. NOTIFICADOR DE FALHA ---
def on_failure_notification(flow, flow_run, state):
    """Envia uma notificação de falha para o Telegram."""
    logger = get_run_logger()
    msg = f"❌ FALHA no flow '{flow_run.name}'. Erro: {state.message}"
    logger.error(msg)
    try:
        # Carrega os segredos do NOVO bloco (já como dicionário)
        telegram_config = Secret.load("telegram-config").get()
        token = telegram_config.get("token")
        chat_id = telegram_config.get("chat_id")
        
        mandarMSG.fn(msg, token, chat_id)
    except Exception as e:
        logger.error(f"Falha ao enviar notificação de falha: {e}")


# --- 4. O FLOW PRINCIPAL ---

@flow(name="MP Feeder ETL", on_failure=[on_failure_notification])
def mp_feeder_flow():
    """
    Este flow substitui o 'main.py'. Ele roda UM lote
    de 100 GTINs, do começo ao fim, com tolerância a falhas.
    """
    logger = get_run_logger()
    setup_logging() # Configura o log em arquivo (opcional, mas bom)

    # --- PASSO 1: CARREGAR CREDENCIAIS ---
    DB_CONFIG, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID = load_credentials()
    
    gmt_menos_3 = timezone(timedelta(hours=-3))
    now_gmt3 = datetime.now(gmt_menos_3)
    today_gmt3 = now_gmt3.date()
    
    logger.info("##### INICIANDO MP Feeder Flow... #####")

    # --- Lógica do 'run_normal_flow' ---
    
    ### II Verificação da Lista de Produtos (GTINs):
    ultima_att_gtins = pegar_ultima_att_gtins_task(DB_CONFIG)
    
    ### III Atualização da Lista de Produtos (Se necessário):
    data_31_dias_atras = today_gmt3 - pd.Timedelta(days=31)
    data_para_checar = ultima_att_gtins or data_31_dias_atras
    diferenca_em_dias = (today_gmt3 - data_para_checar).days

    if diferenca_em_dias > 30: 
        logger.info("##### 🔄 ATUALIZANDO LISTA DE PRODUTOS (MAIS DE 30 DIAS) #####")
        
        produtos_por_valor, produtos_por_qtd = fetch_dados_vendas_task(DB_CONFIG)
        Produtos_top_1000 = transformar_dados_produtos_task(produtos_por_valor, produtos_por_qtd)
        
        if not Produtos_top_1000.empty:
            codigos_para_buscar = Produtos_top_1000["codigo_interno_produto"].tolist()
            df_gtins_principais = fetch_gtins_principais_task(DB_CONFIG, codigos_para_buscar)

            if not df_gtins_principais.empty:
                Produtos_atualizados_merge = pd.merge(
                    Produtos_top_1000, 
                    df_gtins_principais, 
                    on='codigo_interno_produto', 
                    how='left'
                )
                Produtos_atualizados_merge['GTIN'] = Produtos_atualizados_merge['GTIN_principal'].fillna(
                    Produtos_atualizados_merge['GTIN']
                )
                Produtos_limpos = Produtos_atualizados_merge.drop(columns=['GTIN_principal'])
            else:
                Produtos_limpos = Produtos_top_1000
                logger.warning("Nenhum GTIN principal encontrado. Usando GTINs originais da venda.")
        else:
            Produtos_limpos = Produtos_top_1000 # Vazio
        
        insert_produtos_atualizados_task(DB_CONFIG, Produtos_limpos)
    else:
        logger.info("##### LISTA DE PRODUTOS ATUALIZADA RECENTEMENTE. #####")

    ### IV Coleta dos Alvos de Consulta (Geohashs e EANs):
    Geohashs = pegar_geohashs_task(DB_CONFIG)
    EANs = coletar_produtos_no_banco_task(DB_CONFIG)
    ult_gtin = pegar_ultimo_gtin_task(DB_CONFIG)
    Lojas = coletar_lojas_do_banco_task(DB_CONFIG) 
    
    EANs_Selecionados = grupo_eans_selecionados_task(EANs, ult_gtin)
    Consultas = gerar_consultas_task(Geohashs, EANs_Selecionados)

    ### VI Coleta de Notas (Loop Principal):
    # A task 'buscar_notas_task' já tem a lógica de loop e circuit breaker.
    # A task retorna (Notas_df, Lojas_SC_df, run_completo, 0)
    Notas_geral, Lojas_SC_geral, run_completo, _ = buscar_notas_task(Consultas, Lojas)
    
    # --- Lógica de CARGA (do 'main.py') ---
    
    ### VIII Carga de Novas Lojas
    if not Lojas_SC_geral.empty:
        Lojas_SC_geral_sem_duplicatas = Lojas_SC_geral.drop_duplicates(subset=["id_loja"]).reset_index(drop=True)
        logger.info(f"##### 💾 SALVANDO {len(Lojas_SC_geral_sem_duplicatas)} LOJAS... #####")
        
        Lojas_SC_com_latlon = buscar_lat_lon_task(Lojas_SC_geral_sem_duplicatas)
        
        inserir_lojas_sc_task(DB_CONFIG, Lojas_SC_com_latlon, now_gmt3)
    else:
        logger.info("##### NENHUMA LOJA NOVA ENCONTRADA PARA CARGA. #####")

    ### IX Carga das Notas Fiscais:
    if not Notas_geral.empty:
        Notas_geral_sem_duplicatas = Notas_geral.drop_duplicates(subset=["id_nota"]).reset_index(drop=True)
        logger.info(f"##### 💾 SALVANDO {len(Notas_geral_sem_duplicatas)} NOTAS... #####")
        
        inserir_notas_task(DB_CONFIG, Notas_geral_sem_duplicatas, now_gmt3)
    else:
        logger.info("##### NENHUMA NOTA NOVA ENCONTRADA PARA CARGA. #####")

    # --- Lógica de SUCESSO ---
    
    if run_completo:
        msg = f"✅ Sucesso no flow 'MP Feeder ETL'. Lote de {len(EANs_Selecionados)} GTINs concluído."
        logger.info(msg)
        mandarMSG_task(msg, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    else:
        logger.warning("Run da API 'buscar_notas' foi parcial (circuit breaker).")

if __name__ == "__main__":
    # Permite executar o flow localmente para teste
    mp_feeder_flow()