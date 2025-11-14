import json
from prefect.blocks.system import Secret
from prefect.flows import flow # (Seu import de 'flow' estava no lugar errado)
from prefect.tasks import task # (Seu import de 'task' estava no lugar errado)
from prefect import get_run_logger # (Seu import de 'get_run_logger' estava no lugar errado)

@task
def load_credentials():
    """Carrega as credenciais dos Blocks do Prefect."""
    logger = get_run_logger()
    logger.info("Carregando credenciais dos Blocks...")

    # Carrega o DB_CONFIG (como texto) e o converte para JSON
    db_config_text = Secret.load("db-config-mp").get()
    db_config = json.loads(db_config_text) 

    # Carrega os outros segredos
    telegram_token = Secret.load("telegram-token").get()
    telegram_chat_id = Secret.load("telegram-chat-id").get()

    logger.info("Credenciais carregadas com sucesso.")
    return db_config, telegram_token, telegram_chat_id

# --- Tasks do db_manager.py ---
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def pegar_ultima_att_gtins_task(DB_CONFIG): # <-- MUDANÇA: Aceita DB_CONFIG
    return pegar_ultima_att_gtins(DB_CONFIG) # <-- MUDANÇA: Usa a variável

@task
def fetch_dados_vendas_task(DB_CONFIG): # <-- MUDANÇA: Aceita DB_CONFIG
    return fetch_dados_vendas_para_produtos(DB_CONFIG) # <-- MUDANÇA

# ... (faça o mesmo para todas as tasks que usam DB_CONFIG) ...

@task
def mandarMSG_task(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID): # <-- MUDANÇA
    mandarMSG(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID) # <-- MUDANÇA


# --- Notificador de Falha ---
def on_failure_notification(flow, flow_run, state):
    """Envia uma notificação de falha para o Telegram."""
    logger = get_run_logger()
    msg = f"❌ FALHA no flow '{flow_run.name}'. Erro: {state.message}"
    logger.error(msg)
    try:
        # Carrega os segredos aqui também, pois o flow falhou
        token = Secret.load("telegram-token").get()
        chat_id = Secret.load("telegram-chat-id").get()
        mandarMSG.fn(msg, token, chat_id) # .fn() chama a função original
    except Exception as e:
        logger.error(f"Falha ao enviar notificação de falha: {e}")


# --- 2. O NOVO FLOW (Substituto do main.py e flow.py) ---

@flow(name="MP Feeder ETL", on_failure=[on_failure_notification])
def mp_feeder_flow():
    # ... (o resto do seu setup de logger e datas) ...
    
    # --- MUDANÇA: PRIMEIRO PASSO É CARREGAR CREDENCIAIS ---
    DB_CONFIG, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID = load_credentials()
    
    logger.info("##### INICIANDO MP Feeder Flow... #####")

    # --- Lógica do 'run_normal_flow' ---
    
    ### II Verificação da Lista de Produtos (GTINs):
    ultima_att_gtins = pegar_ultima_att_gtins_task(DB_CONFIG) # <-- MUDANÇA: Passa a variável
    
    ### III Atualização da Lista de Produtos (Se necessário):
    data_31_dias_atras = today_gmt3 - pd.Timedelta(days=31)
    data_para_checar = ultima_att_gtins or data_31_dias_atras
    diferenca_em_dias = (today_gmt3 - data_para_checar).days

    if diferenca_em_dias > 30: 
        logger.info("##### 🔄 ATUALIZANDO LISTA DE PRODUTOS (MAIS DE 30 DIAS) #####")
        
        produtos_por_valor, produtos_por_qtd = fetch_dados_vendas_task(DB_CONFIG) # <-- MUDANÇA
        
        # ... (resto da lógica) ...

        # ... (Passar DB_CONFIG para fetch_gtins_principais_task e insert_produtos_atualizados_task) ...
        
    # ... (Passar DB_CONFIG para todas as tasks que precisam dele) ...
    
    # Exemplo no final
    if run_completo:
        msg = f"✅ Sucesso no flow 'MP Feeder ETL'. Lote de {len(EANs_Selecionados)} GTINs concluído."
        logger.info(msg)
        mandarMSG_task(msg, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID) # <-- MUDANÇA
    else:
        logger.warning("Run da API 'buscar_notas' foi parcial (circuit breaker).")

if __name__ == "__main__":
    # Permite executar o flow localmente para teste
    mp_feeder_flow()