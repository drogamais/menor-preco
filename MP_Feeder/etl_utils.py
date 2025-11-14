# MP_Feeder/etl_utils.py
import pandas as pd
import os
import logging
from datetime import datetime

def setup_logging():
    log_dir = "logs/"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    agora = datetime.now().strftime("%Y%m%d_%H%M%S")
    logging.basicConfig(
    level=logging.INFO
    )

# ============================================
# SEÇÃO DE LÓGICA DE NEGÓCIO
# ============================================

def transformar_dados_produtos(produtos_por_valor, produtos_por_qtd):
    """Limpa e mescla os produtos."""
    print("3. Mesclando e limpando bases de dados")
    Produtos = pd.merge(produtos_por_qtd, produtos_por_valor, how="inner")
    Produtos.drop_duplicates(inplace=True)
    Produtos.drop(columns=["valor_total", "qtd_total"], inplace=True, errors='ignore')
    
    # Filtros
    Produtos = Produtos[~Produtos["GTIN"].isin(["99999999999999", "88888888888888", "78000001"])]
    Produtos["GTIN"] = Produtos["GTIN"].astype(str)
    Produtos.drop_duplicates(subset=['GTIN'], keep='first', inplace=True)
    
    return Produtos.head(1000)

def grupo_eans_selecionados(EANs, ult_gtin):
    """
    Seleciona o PRÓXIMO grupo de 100 produtos baseado no último GTIN processado.
    Refatorado para Prefect: Não lê arquivos locais.
    """
    logging.info("##### SELECIONANDO GRUPO DE GTINS #####")

    produtos_por_grupo = 100
    # Divide em grupos de 100
    grupos = [
        EANs.iloc[i : i + produtos_por_grupo]
        for i in range(0, len(EANs), produtos_por_grupo)
    ]
    
    # Se não tem histórico ou lista vazia, começa do início
    if ult_gtin is None or EANs.empty or not grupos:
        print("Iniciando do primeiro grupo (sem histórico).")
        return grupos[0] if grupos else pd.DataFrame(columns=["gtin"])

    # Procura onde o último GTIN estava
    for idx, grupo in enumerate(grupos):
        if ult_gtin in grupo["gtin"].values:
            # Se achou, pega o PRÓXIMO
            if idx + 1 < len(grupos):
                print(f"Último GTIN estava no grupo {idx+1}. Indo para o {idx+2}.")
                return grupos[idx + 1]
            else:
                # Se era o último, volta para o primeiro (loop infinito)
                print("Ciclo concluído. Voltando para o Grupo 1.")
                return grupos[0]

    # Se o GTIN do banco não está na lista atual (ex: lista mudou), recomeça
    print("GTIN antigo não encontrado na lista atual. Reiniciando do Grupo 1.")
    return grupos[0]

def gerar_consultas(Geohashs, EANs):
    """Gera o produto cartesiano (todas as combinações)"""
    print("##### CALCULANDO CONSULTAS #####")
    if EANs.empty or Geohashs.empty:
        return pd.DataFrame(columns=["gtin", "geohash", "index"])

    EANs = EANs.copy()
    EANs.loc[:, "key"] = 1
    Geohashs = Geohashs.copy()
    Geohashs.loc[:, "key"] = 1

    consultas = pd.merge(EANs, Geohashs, on="key").drop("key", axis=1)
    consultas.reset_index(drop=True, inplace=True)
    consultas["index"] = consultas.index
    
    print(f"##### {len(consultas)} CONSULTAS GERADAS #####")
    return consultas

# As funções recuperar_ultimo_indice e finalizar_indice foram removidas 
# pois o Prefect não usa mais arquivos de texto para controle.