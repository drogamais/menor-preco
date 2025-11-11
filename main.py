# main.py (Seu novo MP_Feeder.py)

import os
import logging
from datetime import datetime, timezone, timedelta
import pandas as pd

# Importa SUAS próprias ferramentas!
from config import DB_CONFIG, GOOGLE_API_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
from db_manager import (
    pegar_ultima_att_gtins, fetch_dados_vendas_para_produtos, 
    insert_produtos_atualizados, pegar_geohashs_BD,
    coletar_produtos_no_banco, pegar_ultimo_gtin, coletar_lojas_do_banco,
    inserir_lojas_sc, inserir_notas
)
from api_services import (
    buscar_notas, buscar_lat_lon_lojas_sc, mandarMSG
)
from etl_utils import (
    setup_logging, recuperar_ultimo_indice, transformar_dados_produtos,
    grupo_eans_selecionados, gerar_consultas, finalizar_indice
)

######################
### PRINCIPAL (main):
######################
def main():
    # --- CORREÇÃO DE FUSO HORÁRIO ---
    gmt_menos_3 = timezone(timedelta(hours=-3))
    now_gmt3 = datetime.now(gmt_menos_3)
    today_gmt3 = now_gmt3.date()
    
    arquivo_indice = "ultimo_indice.txt" 
    arquivo_notas_parciais = "notas_parciais.csv"
    arquivo_lojas_parciais = "lojas_parciais.csv"
    
    # Este loop garante que, após recuperar, o script continue para a coleta.
    while True:
        
        run_completo = False 
        indice_para_salvar = 0
        Notas_geral = pd.DataFrame()
        Lojas_SC_geral = pd.DataFrame()

        try:
            # --- PASSO 1: VERIFICAR CACHE LOCAL (DADOS PARCIAIS) ---
            if os.path.exists(arquivo_notas_parciais):
                print("##### ⚠️ DADOS PARCIAIS .CSV ENCONTRADOS. TENTANDO CARREGAR... #####")
                logging.warning("##### DADOS PARCIAIS .CSV ENCONTRADOS. TENTANDO CARREGAR... #####")
                
                # Carrega os dados salvos da última falha
                Notas_csv = pd.read_csv(arquivo_notas_parciais)
                Lojas_csv = pd.DataFrame()
                if os.path.exists(arquivo_lojas_parciais):
                    Lojas_csv = pd.read_csv(arquivo_lojas_parciais)
                
                # Pega o índice que já foi salvo na falha anterior
                ultimo_indice = recuperar_ultimo_indice(arquivo_indice)
                indice_para_salvar = ultimo_indice
                run_completo = False
                
                print(f"##### DADOS CARREGADOS. TENTANDO INSERIR NO BANCO (LOTE DO ÍNDICE {ultimo_indice}) #####")
                
                # 1. Carrega as lojas que JÁ ESTÃO no banco ANTES de fazer qualquer coisa
                print("##### Verificando lojas já cadastradas no banco... #####")
                Lojas_no_banco_df = coletar_lojas_do_banco(DB_CONFIG) # <-- Passa DB_CONFIG
                lojas_ja_cadastradas_set = set(Lojas_no_banco_df["id_loja"])
                print(f"##### {len(lojas_ja_cadastradas_set)} lojas CADASTRADAS no banco. #####")
                
                # --- INSERE AS LOJAS DO CSV (JÁ FILTRADAS) ---
                if not Lojas_csv.empty:
                    Lojas_csv_sem_duplicatas = Lojas_csv.drop_duplicates(subset=["id_loja"]).reset_index(drop=True)
                    
                    # 2. Filtra o DataFrame do CSV ANTES de enviá-lo para a API do Google
                    lojas_para_buscar_latlon = Lojas_csv_sem_duplicatas[
                        ~Lojas_csv_sem_duplicatas['id_loja'].isin(lojas_ja_cadastradas_set)
                    ]
                    total_para_buscar = len(lojas_para_buscar_latlon)
                    total_ignoradas_csv = len(Lojas_csv_sem_duplicatas) - total_para_buscar
                    
                    if total_ignoradas_csv > 0:
                        print(f"##### Ignorando {total_ignoradas_csv} lojas do CSV que já estão no banco. #####")
                    
                    if total_para_buscar > 0:
                        print(f"##### 💾 RECUPERANDO {total_para_buscar} LOJAS (REAIS) DO CSV... #####")
                        # 3. Agora só chama a API do Google para lojas realmente novas
                        Lojas_SC_com_latlon = buscar_lat_lon_lojas_sc(lojas_para_buscar_latlon, GOOGLE_API_KEY) # <-- Passa API_KEY
                        inserir_lojas_sc(Lojas_SC_com_latlon, now_gmt3, DB_CONFIG) # <-- Passa DB_CONFIG
                    else:
                        print("##### Nenhuma loja nova do CSV para buscar/inserir. #####")
                
                # --- INSERE AS NOTAS DO CSV ---
                if not Notas_csv.empty:
                    Notas_csv_sem_duplicatas = Notas_csv.drop_duplicates(subset=["id_nota"]).reset_index(drop=True)
                    print(f"##### 💾 RECUPERANDO {len(Notas_csv_sem_duplicatas)} NOTAS DO CSV... #####")
                    inserir_notas(Notas_csv_sem_duplicatas, now_gmt3, DB_CONFIG) # <-- Passa DB_CONFIG
                
                # --- LIMPA O CSV APÓS SUCESSO ---
                print("##### SUCESSO AO SALVAR DADOS DO CSV. LIMPANDO ARQUIVOS. #####")
                os.remove(arquivo_notas_parciais)
                if os.path.exists(arquivo_lojas_parciais):
                    os.remove(arquivo_lojas_parciais)
                
                # Salva o índice (ainda estamos em run_completo=False)
                with open(arquivo_indice, "w") as f:
                    f.write(str(indice_para_salvar))
                print(f"##### ⚠️ RUN PARCIAL. ÍNDICE MANTIDO EM: {indice_para_salvar} #####")
                
                # --- CONTINUA O LOOP PARA INICIAR A COLETA ---
                print("##### RECUPERAÇÃO CONCLUÍDA. CONTINUANDO PARA A COLETA DA API... #####")
                print("\n" + "="*50 + "\n")
                continue

            else:
                # --- EXECUÇÃO NORMAL (SEM CSV) ---
                print("##### NENHUM DADO PARCIAL ENCONTRADO. INICIANDO COLETA NORMAL... #####")
                
                ### II Verificação da Lista de Produtos (GTINs):
                ultima_att_gtins = pegar_ultima_att_gtins(DB_CONFIG) # <-- Passa DB_CONFIG
                
                ### III Atualização da Lista de Produtos (Se necessário):
                data_31_dias_atras = today_gmt3 - pd.Timedelta(days=31)
                data_para_checar = ultima_att_gtins or data_31_dias_atras
                diferenca_em_dias = (today_gmt3 - data_para_checar).days

                if diferenca_em_dias > 30: 
                    print("##### 🔄 ATUALIZANDO LISTA DE PRODUTOS (MAIS DE 30 DIAS) #####")
                    logging.info("Atualizando lista de produtos (Mais de 30 dias)...")
                    
                    # --- CORREÇÃO: LÓGICA ETL ---
                    # 1. Extrair (Fetch)
                    produtos_por_valor, produtos_por_qtd = fetch_dados_vendas_para_produtos(DB_CONFIG)
                    
                    # 2. Transformar (Usando sua nova função utilitária)
                    Produtos_limpos = transformar_dados_produtos(produtos_por_valor, produtos_por_qtd)

                    # 3. Carregar (Load)
                    insert_produtos_atualizados(DB_CONFIG, Produtos_limpos)
                    # --- FIM DA CORREÇÃO ---
                else:
                    print("##### LISTA DE PRODUTOS ATUALIZADA RECENTEMENTE. PULANDO ATUALIZAÇÃO. #####")
                    logging.info("##### LISTA DE PRODUTOS ATUALIZADA RECENTEMENTE. PULANDO ATUALIZAÇÃO. #####")

                ### IV Coleta dos Alvos de Consulta (Geohashs e EANs):
                Geohashs = pegar_geohashs_BD(DB_CONFIG)
                EANs = coletar_produtos_no_banco(DB_CONFIG)
                ult_gtin = pegar_ultimo_gtin(DB_CONFIG)
                EANs_Selecionados = grupo_eans_selecionados(EANs, ult_gtin, arquivo_indice)
                Consultas = gerar_consultas(Geohashs, EANs_Selecionados)
                Lojas = coletar_lojas_do_banco(DB_CONFIG) 

                ### V Tratamento de Falhas (Recuperação de Índice):
                ultimo_indice = recuperar_ultimo_indice(arquivo_indice)
                indice_para_salvar = ultimo_indice

                ### VI Coleta de Notas (Loop Principal de Requisições e Tentativas):
                Notas_geral, Lojas_SC_geral, run_completo, indice_para_salvar = buscar_notas(
                    Consultas, Lojas, ultimo_indice, arquivo_indice,
                    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID # <-- Passa tokens
                )
            
            # --- PASSO 2: TENTATIVA DE CARGA (SÓ DOS DADOS DA API) ---
            
            ### VIII Carga de Novas Lojas (Lojas Sem Cadastro):
            if not Lojas_SC_geral.empty:
                Lojas_SC_geral_sem_duplicatas = Lojas_SC_geral.drop_duplicates(subset=["id_loja"]).reset_index(drop=True)
                print(f"##### 💾 SALVANDO {len(Lojas_SC_geral_sem_duplicatas)} LOJAS (TOTAIS OU PARCIAIS)... #####")
                Lojas_SC_com_latlon = buscar_lat_lon_lojas_sc(Lojas_SC_geral_sem_duplicatas, GOOGLE_API_KEY) # <-- Passa API_KEY
                inserir_lojas_sc(Lojas_SC_com_latlon, now_gmt3, DB_CONFIG) # <-- Passa DB_CONFIG
            else:
                print("##### NENHUMA LOJA NOVA ENCONTRADA PARA CARGA. #####")

            ### IX Carga das Notas Fiscais:
            if not Notas_geral.empty:
                Notas_geral_sem_duplicatas = Notas_geral.drop_duplicates(subset=["id_nota"]).reset_index(drop=True)
                print(f"##### 💾 SALVANDO {len(Notas_geral_sem_duplicatas)} NOTAS (TOTAIS OU PARCIAIS)... #####")
                inserir_notas(Notas_geral_sem_duplicatas, now_gmt3, DB_CONFIG) # <-- Passa DB_CONFIG
            else:
                print("##### NENHUMA NOTA NOVA ENCONTRADA PARA CARGA. #####")

            # --- PASSO 3: SUCESSO! ATUALIZAR O ÍNDICE E DECIDIR SE CONTINUA ---

            ### VII Limpeza e Concatenação (Atualização do Índice)
            if run_completo:
                finalizar_indice(arquivo_indice)
                print("##### ✅ MP Feeder CONCLUÍDO COM SUCESSO (LOTE ATUAL) #####")
                logging.info("##### ✅ MP Feeder CONCLUÍDO COM SUCESSO (LOTE ATUAL) #####")
                msg = f"{now_gmt3.strftime('%Y-%m-%d %H:%M:%S')} - ✅ Atualização do MENOR PREÇO realizada com sucesso!"
                mandarMSG(msg, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID) # <-- Passa tokens
                
                break # Sai do 'while True'

            else:
                # Se foi parcial (circuit breaker), salvamos o índice e continuamos o loop
                with open(arquivo_indice, "w") as f:
                    f.write(str(indice_para_salvar))
                print(f"##### ⚠️ RUN PARCIAL (API). ÍNDICE SALVO EM: {indice_para_salvar}. REINICIANDO... #####")
                logging.warning(f"##### ⚠️ RUN PARCIAL (API). ÍNDICE SALVO EM: {indice_para_salvar}. REINICIANDO... #####")

        except Exception as e:
            # --- PASSO 4: FALHA! (EX: BANCO OFFLINE) ---
            print(f"❌ Erro na execução: {e}")
            logging.error(f"❌ Erro na execução: {e}", exc_info=True) 

            # Se a falha foi de conexão com o banco E temos dados para salvar
            if ("connect" in str(e) or "10060" in str(e)) and (not Notas_geral.empty or not Lojas_SC_geral.empty):
                
                print("##### ❌ FALHA DE CONEXÃO COM O BANCO. SALVANDO DADOS PARCIAIS EM ARQUIVOS CSV... #####")
                logging.critical("FALHA DE CONEXÃO COM O BANCO. Salvando dados parciais em CSV.")
                
                try:
                    if not Notas_geral.empty:
                        Notas_geral.to_csv(arquivo_notas_parciais, index=False)
                    
                    if not Lojas_SC_geral.empty:
                        Lojas_SC_geral.to_csv(arquivo_lojas_parciais, index=False)
                    
                    with open(arquivo_indice, "w") as f:
                        f.write(str(indice_para_salvar))
                    
                    print(f"##### DADOS SALVOS EM {arquivo_notas_parciais} E ÍNDICE SALVO EM {indice_para_salvar} #####")
                    logging.info(f"DADOS SALVOS EM {arquivo_notas_parciais} E ÍNDICE SALVO EM {indice_para_salvar}")
                    
                except Exception as e_csv:
                    print(f"❌❌ ERRO CRÍTICO AO SALVAR ARQUIVO CSV: {e_csv}")
                    logging.critical(f"ERRO CRÍTICO AO SALVAR ARQUIVO CSV: {e_csv}")

            # Envia notificação de erro (exceto se for o erro de 'circuit break' que já notifica)
            if "LIMITE DE ERROS CONSECUTIVOS" not in str(e):
                msg_erro = f"{now_gmt3.strftime('%Y-%m-%d %H:%M:%S')} - ❌ Erro ao atualizar o MENOR PREÇO = {e}"
                mandarMSG(msg_erro, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID) # <-- Passa tokens
            
            exit() # Termina o script em caso de falha

if __name__ == "__main__":
    setup_logging() # <-- Executa o setup de logging PRIMEIRO
    main()