# main.py
import os
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd

# Importa as configs
from config import DB_CONFIG, GOOGLE_API_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

# Importa os módulos de execução
from flow import run_recovery_flow, run_normal_flow
from error_handler import handle_execution_error, handle_api_fail, handle_success
from etl_utils import setup_logging
# --- MUDANÇA: Precisamos das funções de inserir e API aqui agora ---
from db_manager import inserir_lojas_sc, inserir_notas
from api_services import buscar_lat_lon_lojas_sc

def main():
    # --- CONFIGURAÇÕES DE EXECUÇÃO ---
    gmt_menos_3 = timezone(timedelta(hours=-3))
    now_gmt3 = datetime.now(gmt_menos_3)
    today_gmt3 = now_gmt3.date()
    
    # Agrupa todas as configs
    configs = {
        "DB_CONFIG": DB_CONFIG,
        "GOOGLE_API_KEY": GOOGLE_API_KEY,
        "TELEGRAM_TOKEN": TELEGRAM_TOKEN,
        "TELEGRAM_CHAT_ID": TELEGRAM_CHAT_ID,
        "arquivo_indice": "ultimo_indice.txt",
        "arquivo_notas_parciais": "notas_parciais.csv",
        "arquivo_lojas_parciais": "lojas_parciais.csv"
    }
    
    # --- LOOP PRINCIPAL DE EXECUÇÃO ---
    while True:
        Notas_geral = pd.DataFrame()
        Lojas_SC_geral = pd.DataFrame()
        run_completo = False
        indice_para_salvar = 0

        try:
            # --- PASSO 1: VERIFICAR/EXECUTAR RECUPERAÇÃO DE CSV ---
            if os.path.exists(configs['arquivo_notas_parciais']):
                run_recovery_flow(configs, now_gmt3)
                print("##### RECUPERAÇÃO CONCLUÍDA. CONTINUANDO PARA A COLETA DA API... #####")
                print("\n" + "="*50 + "\n")
                continue 
            
            # --- PASSO 2: EXECUTAR FLUXO NORMAL (SÓ EXTRAÇÃO/TRANSFORMAÇÃO) ---
            else:
                # O flow agora SÓ coleta e transforma, e RETORNA os dados
                Notas_geral, Lojas_SC_geral, run_completo, indice_para_salvar = run_normal_flow(
                    configs, now_gmt3, today_gmt3
                )
            
            # --- PASSO 3: CARGA (LOAD) ---
            # A lógica de Carga (insert) foi movida para o 'main'.
            # Se 'inserir_lojas_sc' falhar, o 'except' abaixo será
            # acionado e terá o 'Notas_geral' e 'Lojas_SC_geral' preenchidos.
            
            ### VIII Carga de Novas Lojas
            if not Lojas_SC_geral.empty:
                Lojas_SC_geral_sem_duplicatas = Lojas_SC_geral.drop_duplicates(subset=["id_loja"]).reset_index(drop=True)
                print(f"##### 💾 SALVANDO {len(Lojas_SC_geral_sem_duplicatas)} LOJAS (TOTAIS OU PARCIAIS)... #####")
                Lojas_SC_com_latlon = buscar_lat_lon_lojas_sc(Lojas_SC_geral_sem_duplicatas, GOOGLE_API_KEY)
                inserir_lojas_sc(Lojas_SC_com_latlon, now_gmt3, DB_CONFIG)
            else:
                print("##### NENHUMA LOJA NOVA ENCONTRADA PARA CARGA. #####")

            ### IX Carga das Notas Fiscais:
            if not Notas_geral.empty:
                Notas_geral_sem_duplicatas = Notas_geral.drop_duplicates(subset=["id_nota"]).reset_index(drop=True)
                print(f"##### 💾 SALVANDO {len(Notas_geral_sem_duplicatas)} NOTAS (TOTAIS OU PARCIAIS)... #####")
                inserir_notas(Notas_geral_sem_duplicatas, now_gmt3, DB_CONFIG)
            else:
                print("##### NENHUMA NOTA NOVA ENCONTRADA PARA CARGA. #####")

            # --- PASSO 4: LIDAR COM O RESULTADO ---
            if run_completo:
                handle_success(
                    configs['arquivo_indice'], now_gmt3, 
                    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
                )
                break 
            else:
                handle_api_fail(indice_para_salvar)

        except Exception as e:
            # --- PASSO 5: LIDAR COM FALHA CATASTRÓFICA ---
            # Agora, se 'inserir_lojas_sc' ou 'inserir_notas' falharem,
            # as variáveis 'Notas_geral' e 'Lojas_SC_geral' estarão PREENCHIDAS
            # e o 'handle_execution_error' salvará o CSV.
            handle_execution_error(
                e, Notas_geral, Lojas_SC_geral, indice_para_salvar,
                now_gmt3, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
            )

if __name__ == "__main__":
    setup_logging()
    main()