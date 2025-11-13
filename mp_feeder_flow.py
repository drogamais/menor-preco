import pandas as pd
from datetime import datetime, timezone, timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

# Importar suas credenciais (o Prefect pode lidar com isso de forma mais segura
# com "Blocks", mas vamos usar seu config.py por enquanto)
try:
    from config import DB_CONFIG, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
except ImportError:
    print("Execute o script a partir da pasta raiz ou configure 'config.py'")
    exit()

# Importar TODAS as suas funções de lógica de negócio
from MP_Feeder.db_manager import (
    pegar_ultima_att_gtins, fetch_dados_vendas_para_produtos, 
    insert_produtos_atualizados, pegar_geohashs_BD,
    coletar_produtos_no_banco, pegar_ultimo_gtin, coletar_lojas_do_banco,
    inserir_lojas_sc, inserir_notas, fetch_gtins_principais
)
from MP_Feeder.api_services import (
    buscar_notas, buscar_lat_lon_lojas_sc_nominatim, mandarMSG
)
from MP_Feeder.etl_utils import (
    transformar_dados_produtos,
    grupo_eans_selecionados, # Vamos usar a lógica de seleção de grupo
    gerar_consultas,
    setup_logging
)

# --- 1. DECORAR SUAS FUNÇÕES COMO TASKS ---
# Agora, envolvemos suas funções com o decorador @task.
# Adicionamos 'retries' onde há I/O (rede, banco) para substituir
# a sua lógica de recuperação de .csv.

# --- Tasks do db_manager.py ---
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def pegar_ultima_att_gtins_task():
    return pegar_ultima_att_gtins(DB_CONFIG)

@task
def fetch_dados_vendas_task():
    return fetch_dados_vendas_para_produtos(DB_CONFIG)

@task(retries=3, retry_delay_seconds=30)
def insert_produtos_atualizados_task(produtos_df):
    insert_produtos_atualizados(DB_CONFIG, produtos_df)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=6))
def pegar_geohashs_task():
    return pegar_geohashs_BD(DB_CONFIG)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def coletar_produtos_no_banco_task():
    return coletar_produtos_no_banco(DB_CONFIG)

@task
def pegar_ultimo_gtin_task():
    # Esta task substitui a necessidade do 'ultimo_indice.txt'
    return pegar_ultimo_gtin(DB_CONFIG)

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def coletar_lojas_do_banco_task():
    return coletar_lojas_do_banco(DB_CONFIG)

@task(retries=3, retry_delay_seconds=120)
def inserir_lojas_sc_task(Lojas_SC, now_obj):
    # Se esta task falhar, o Prefect tentará novamente 3x (em 2 min).
    # Os dados de Lojas_SC ficam seguros com o Prefect.
    # Adeus 'lojas_parciais.csv'
    inserir_lojas_sc(Lojas_SC, now_obj, DB_CONFIG)

@task(retries=3, retry_delay_seconds=120)
def inserir_notas_task(Notas, now_obj):
    # Idem. Adeus 'notas_parciais.csv'
    inserir_notas(Notas, now_obj, DB_CONFIG)

@task
def fetch_gtins_principais_task(codigos_para_buscar):
    return fetch_gtins_principais(DB_CONFIG, codigos_para_buscar)

# --- Tasks do etl_utils.py ---
@task
def transformar_dados_produtos_task(produtos_por_valor, produtos_por_qtd):
    return transformar_dados_produtos(produtos_por_valor, produtos_por_qtd)

@task
def grupo_eans_selecionados_task(EANs, ult_gtin):
    # Removemos o parâmetro 'arquivo_indice'.
    # A lógica interna de 'grupo_eans_selecionados'
    # que checava se o arquivo existia deve ser removida.
    # Assumindo que foi removida, ela agora é uma função pura.
    
    # --- SIMULAÇÃO DA LÓGICA SEM O ARQUIVO ---
    # (O ideal é refatorar 'grupo_eans_selecionados' para não precisar disto)
    produtos_por_grupo = 100
    grupos = [
        EANs.iloc[i : i + produtos_por_grupo]
        for i in range(0, len(EANs), produtos_por_grupo)
    ]
    if ult_gtin is None or EANs.empty:
        return grupos[0] if grupos else pd.DataFrame(columns=["gtin"])

    for idx, grupo in enumerate(grupos):
        if ult_gtin in grupo["gtin"].values:
            if idx + 1 < len(grupos):
                return grupos[idx + 1] # Sempre pega o PRÓXIMO grupo
            else:
                return grupos[0] # Volta ao início
    
    return grupos[0] # Fallback
    # --- FIM DA SIMULAÇÃO ---
    
    # Se você modificar 'grupo_eans_selecionados' para remover a lógica do 
    # 'arquivo_indice', pode chamar diretamente:
    # return grupo_eans_selecionados(EANs, ult_gtin, None) # Passando None para o arquivo


@task
def gerar_consultas_task(Geohashs, EANs_Selecionados):
    return gerar_consultas(Geohashs, EANs_Selecionados)

# --- Tasks do api_services.py ---
@task(retries=2, retry_delay_seconds=30)
def buscar_notas_task(Consultas, Lojas):
    # Modificamos 'buscar_notas' para não precisar mais do 
    # 'arquivo_indice' e não salvar nada.
    # Ela só precisa retornar os dataframes e se o run foi completo.
    
    # ATENÇÃO: Esta é uma chamada "fictícia". Você deve refatorar
    # 'buscar_notas' para remover os parâmetros
    # 'arquivo_indice', 'TELEGRAM_TOKEN', 'TELEGRAM_CHAT_ID'.
    # A notificação de erro será feita pelo 'on_failure' do flow.
    
    Notas_df, Lojas_SC_df, run_completo, _ = buscar_notas(
        Consultas, Lojas, 
        ultimo_indice=0, # Começa do 0, já que as Consultas são só do lote
        arquivo_indice="dummy.txt", # Não será usado
        TELEGRAM_TOKEN=None, # Não será usado
        TELEGRAM_CHAT_ID=None # Não será usado
    )
    # Ignoramos o 'indice_para_salvar', pois não é mais necessário
    return Notas_df, Lojas_SC_df, run_completo

@task(retries=3, retry_delay_seconds=5)
def buscar_lat_lon_task(Lojas_SC_geral_sem_duplicatas):
    return buscar_lat_lon_lojas_sc_nominatim(Lojas_SC_geral_sem_duplicatas)

@task
def mandarMSG_task(message):
    # Esta task será usada para notificações
    mandarMSG(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)


# --- Notificador de Falha ---
def on_failure_notification(flow, flow_run, state):
    """Envia uma notificação de falha para o Telegram."""
    logger = get_run_logger()
    msg = f"❌ FALHA no flow '{flow_run.name}'. Erro: {state.message}"
    logger.error(msg)
    try:
        mandarMSG_task.fn(msg) # .fn() chama a função original
    except Exception as e:
        logger.error(f"Falha ao enviar notificação de falha: {e}")


# --- 2. O NOVO FLOW (Substituto do main.py e flow.py) ---

@flow(name="MP Feeder ETL", on_failure=[on_failure_notification])
def mp_feeder_flow():
    """
    Este flow substitui o 'main.py'. Ele roda UM lote
    de 100 GTINs, do começo ao fim, com tolerância a falhas.
    Agende este flow para rodar a cada 10-15 minutos.
    """
    logger = get_run_logger()
    setup_logging() # Configura o log em arquivo (opcional, mas bom)

    gmt_menos_3 = timezone(timedelta(hours=-3))
    now_gmt3 = datetime.now(gmt_menos_3)
    today_gmt3 = now_gmt3.date()
    
    logger.info("##### INICIANDO MP Feeder Flow... #####")

    # --- Lógica do 'run_normal_flow' ---
    
    ### II Verificação da Lista de Produtos (GTINs):
    ultima_att_gtins = pegar_ultima_att_gtins_task()
    
    ### III Atualização da Lista de Produtos (Se necessário):
    data_31_dias_atras = today_gmt3 - pd.Timedelta(days=31)
    data_para_checar = ultima_att_gtins or data_31_dias_atras
    diferenca_em_dias = (today_gmt3 - data_para_checar).days

    if diferenca_em_dias > 30: 
        logger.info("##### 🔄 ATUALIZANDO LISTA DE PRODUTOS (MAIS DE 30 DIAS) #####")
        
        produtos_por_valor, produtos_por_qtd = fetch_dados_vendas_task()
        Produtos_top_1000 = transformar_dados_produtos_task(produtos_por_valor, produtos_por_qtd)
        
        if not Produtos_top_1000.empty:
            codigos_para_buscar = Produtos_top_1000["codigo_interno_produto"].tolist()
            df_gtins_principais = fetch_gtins_principais_task(codigos_para_buscar)

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
        
        insert_produtos_atualizados_task(Produtos_limpos)
    else:
        logger.info("##### LISTA DE PRODUTOS ATUALIZADA RECENTEMENTE. #####")

    ### IV Coleta dos Alvos de Consulta (Geohashs e EANs):
    # Estas tasks podem rodar em paralelo
    Geohashs = pegar_geohashs_task()
    EANs = coletar_produtos_no_banco_task()
    ult_gtin = pegar_ultimo_gtin_task()
    Lojas = coletar_lojas_do_banco_task() 
    
    # Seleciona o grupo deste run
    EANs_Selecionados = grupo_eans_selecionados_task(EANs, ult_gtin)
    Consultas = gerar_consultas_task(Geohashs, EANs_Selecionados)

    ### VI Coleta de Notas (Loop Principal):
    # A task buscar_notas já tem a lógica de loop e circuit breaker.
    # Nós só a executamos 1x e confiamos no retry do Prefect se ela falhar.
    Notas_geral, Lojas_SC_geral, run_completo = buscar_notas_task(Consultas, Lojas)
    
    # --- Lógica de CARGA (do 'main.py') ---
    
    ### VIII Carga de Novas Lojas
    if not Lojas_SC_geral.empty:
        Lojas_SC_geral_sem_duplicatas = Lojas_SC_geral.drop_duplicates(subset=["id_loja"]).reset_index(drop=True)
        logger.info(f"##### 💾 SALVANDO {len(Lojas_SC_geral_sem_duplicatas)} LOJAS... #####")
        
        # O enrequecimento de Lat/Lon é uma task separada
        Lojas_SC_com_latlon = buscar_lat_lon_task(Lojas_SC_geral_sem_duplicatas)
        
        # A carga no DB é a task final com retries
        inserir_lojas_sc_task(Lojas_SC_com_latlon, now_gmt3)
    else:
        logger.info("##### NENHUMA LOJA NOVA ENCONTRADA PARA CARGA. #####")

    ### IX Carga das Notas Fiscais:
    if not Notas_geral.empty:
        Notas_geral_sem_duplicatas = Notas_geral.drop_duplicates(subset=["id_nota"]).reset_index(drop=True)
        logger.info(f"##### 💾 SALVANDO {len(Notas_geral_sem_duplicatas)} NOTAS... #####")
        
        # A carga no DB é a task final com retries
        inserir_notas_task(Notas_geral_sem_duplicatas, now_gmt3)
    else:
        logger.info("##### NENHUMA NOTA NOVA ENCONTRADA PARA CARGA. #####")

    # --- Lógica de SUCESSO ---
    # Não precisamos mais do 'handle_api_fail', pois se a API falhar,
    # a task 'buscar_notas_task' vai falhar e o 'on_failure' vai nos avisar.
    
    if run_completo:
        msg = f"✅ Sucesso no flow 'MP Feeder ETL'. Lote de {len(EANs_Selecionados)} GTINs concluído."
        logger.info(msg)
        mandarMSG_task(msg)
    else:
        # A própria task 'buscar_notas_task' deve ter nos avisado
        # (se você mantiver essa lógica nela)
        logger.warning("Run da API 'buscar_notas' foi parcial (circuit breaker).")

if __name__ == "__main__":
    # Permite executar o flow localmente para teste
    mp_feeder_flow()