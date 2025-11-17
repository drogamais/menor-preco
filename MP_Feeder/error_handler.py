# error_handler.py
import os
import logging
import pandas as pd

# É preciso importar o mandarMSG aqui, pois este módulo é responsável por notificar
from MP_Feeder.api_services import mandarMSG 
from MP_Feeder.etl_utils import finalizar_indice # Precisamos disso para o handle_success

def save_partial_data(Notas_geral, Lojas_SC_geral, indice_para_salvar):
    """
    Salva os DataFrames e o índice em arquivos CSV locais.
    """
    arquivo_notas_parciais = "notas_parciais.csv"
    arquivo_lojas_parciais = "lojas_parciais.csv"
    arquivo_indice = "ultimo_indice.txt"
    
    try:
        if not Notas_geral.empty:
            Notas_geral.to_csv(arquivo_notas_parciais, index=False)
        
        if not Lojas_SC_geral.empty:
            Lojas_SC_geral.to_csv(arquivo_lojas_parciais, index=False)
        
        # Salva o índice de qualquer maneira
        with open(arquivo_indice, "w") as f:
            f.write(str(indice_para_salvar))
        
        print(f"##### DADOS SALVOS EM {arquivo_notas_parciais} E ÍNDICE SALVO EM {indice_para_salvar} #####")
        logging.info(f"DADOS SALVOS EM {arquivo_notas_parciais} E ÍNDICE SALVO EM {indice_para_salvar}")
    
    except Exception as e_csv:
        print(f" ERRO CRÍTICO AO SALVAR ARQUIVO CSV: {e_csv}")
        logging.critical(f"ERRO CRÍTICO AO SALVAR ARQUIVO CSV: {e_csv}")

def handle_execution_error(e, Notas_geral, Lojas_SC_geral, indice_para_salvar, now_gmt3, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID):
    """
    Handler principal de erros. Decide se salva CSVs e notifica.
    """
    print(f" Erro na execução: {e}")
    logging.error(f" Erro na execução: {e}", exc_info=True) 

    # Se a falha foi de conexão com o banco E temos dados para salvar
    if ("connect" in str(e) or "10060" in str(e)) and (not Notas_geral.empty or not Lojas_SC_geral.empty):
        print("#####  FALHA DE CONEXÃO COM O BANCO. SALVANDO DADOS PARCIAIS... #####")
        logging.critical("FALHA DE CONEXÃO COM O BANCO. Salvando dados parciais em CSV.")
        save_partial_data(Notas_geral, Lojas_SC_geral, indice_para_salvar)
    
    # Envia notificação de erro (exceto se for o erro de 'circuit break' que já notifica)
    if "LIMITE DE ERROS CONSECUTIVOS" not in str(e):
        msg_erro = f"{now_gmt3.strftime('%Y-%m-%d %H:%M:%S')} -  Erro ao atualizar o MENOR PREÇO = {e}"
        mandarMSG(msg_erro, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    
    exit() # Termina o script em caso de falha

def handle_api_fail(indice_para_salvar):
    """
    Salva o índice parcial quando o 'circuit breaker' da API é ativado.
    """
    arquivo_indice = "ultimo_indice.txt"
    with open(arquivo_indice, "w") as f:
        f.write(str(indice_para_salvar))
    print(f"#####  RUN PARCIAL (API). ÍNDICE SALVO EM: {indice_para_salvar}. REINICIANDO... #####")
    logging.warning(f"#####  RUN PARCIAL (API). ÍNDICE SALVO EM: {indice_para_salvar}. REINICIANDO... #####")

def handle_success(arquivo_indice, now_gmt3, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID):
    """
    Limpa o índice e envia notificação de sucesso.
    """
    finalizar_indice(arquivo_indice) # Vem do etl_utils
    print("#####  MP Feeder CONCLUÍDO COM SUCESSO (LOTE ATUAL) #####")
    logging.info("#####  MP Feeder CONCLUÍDO COM SUCESSO (LOTE ATUAL) #####")
    msg = f"{now_gmt3.strftime('%Y-%m-%d %H:%M:%S')} -  Atualização do MENOR PREÇO realizada com sucesso!"
    mandarMSG(msg, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID) # Vem do api_services