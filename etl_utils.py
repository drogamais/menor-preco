# etl_utils.py
import pandas as pd
import os
import logging
from datetime import datetime # <-- CORREÇÃO DE IMPORTAÇÃO

def setup_logging():
    """
    Configura o logging para salvar em um arquivo com data/hora.
    """
    log_dir = "logs/"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    agora = datetime.now().strftime("%Y%m%d_%H%M%S") # <-- Mudei para datetime.now()
    logging.basicConfig(
        filename=os.path.join(log_dir, f"requests-{agora}.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

# ============================================
# SEÇÃO DE GERENCIAMENTO DE ESTADO (ÍNDICE)
# ============================================

def recuperar_ultimo_indice(arquivo_indice):
    """
    Lê o arquivo de texto e recupera o último índice salvo.
    Retorna 0 se o arquivo não existir.
    """
    print("##### RECUPERANDO ÚLTIMO INDICE (SE EXISTIR) #####")
    logging.info("##### RECUPERANDO ÚLTIMO INDICE (SE EXISTIR) #####")
    try:
        with open(arquivo_indice, "r") as f:
            ultimo_indice = int(f.read().strip())
            print(f"##### ÚLTIMO ÍNDICE RECUPERADO: {ultimo_indice} #####")
            logging.info(f"##### ÚLTIMO ÍNDICE RECUPERADO: {ultimo_indice} #####")
    except FileNotFoundError:
        ultimo_indice = 0
        print("##### ARQUIVO DE ÍNDICE NÃO ENCONTRADO, COMEÇANDO DO 0 #####")
        logging.info("##### ARQUIVO DE ÍNDICE NÃO ENCONTRADO, COMEÇANDO DO 0 #####")
    return ultimo_indice
    
def finalizar_indice(arquivo_indice):
    """
    Remove o arquivo de índice em caso de uma execução bem-sucedida.
    """
    print("##### FINALIZANDO ÍNDICES DA EXECUÇÃO #####")
    logging.info("##### FINALIZANDO ÍNDICES DA EXECUÇÃO #####")
    if os.path.exists(arquivo_indice):
        os.remove(arquivo_indice)
        print(f"##### ARQUIVO DE ÍNDICE {arquivo_indice} REMOVIDO COM SUCESSO #####")
        logging.info(f"##### ARQUIVO DE ÍNDICE {arquivo_indice} REMOVIDO COM SUCESSO #####")

# ============================================
# SEÇÃO DE LÓGICA DE NEGÓCIO E TRANSFORMAÇÃO
# ============================================

def transformar_dados_produtos(produtos_por_valor, produtos_por_qtd):
    """
    (ETL - Transform) Recebe os DataFrames brutos do banco e aplica
    a lógica de merge, filtro e limpeza do Pandas.
    """
    print("3. Mesclando as duas bases de dados")
    logging.info("3. Mesclando as duas bases de dados")
    
    Produtos = pd.merge(produtos_por_qtd, produtos_por_valor, how="inner")

    Produtos.drop_duplicates(inplace=True)
    
    # Remove colunas desnecessárias do merge
    Produtos.drop(columns=["valor_total", "qtd_total"], inplace=True, errors='ignore') 
    
    # Filtra GTINs inválidos
    Produtos = Produtos[Produtos["GTIN"] != "99999999999999"]
    Produtos = Produtos[Produtos["GTIN"] != "88888888888888"]
    
    # Aplica o limite
    Produtos = Produtos.head(1000)
    
    # Garante unicidade pelo ID interno antes de carregar
    Produtos.drop_duplicates(subset=['codigo_interno_produto'], keep='first', inplace=True)
    
    return Produtos

def grupo_eans_selecionados(EANs, ult_gtin, arquivo_indice):
    """
    Divide a lista de EANs em grupos e seleciona o próximo grupo a ser
    executado, com base no último GTIN da execução anterior.
    """
    logging.info("##### SELECIONANDO GRUPO DE GTINS #####")

    produtos_por_grupo = 100
    grupos = [
        EANs.iloc[i : i + produtos_por_grupo]
        for i in range(0, len(EANs), produtos_por_grupo)
    ]
    
    if ult_gtin is None or EANs.empty:
        print("Nenhum GTIN anterior encontrado ou lista de EANs vazia. Retornando o primeiro grupo.")
        logging.info("Nenhum GTIN anterior encontrado ou lista de EANs vazia. Retornando o primeiro grupo.")
        return grupos[0] if grupos else pd.DataFrame(columns=["gtin"])

    for idx, grupo in enumerate(grupos):
        if ult_gtin in grupo["gtin"].values:
            print(f"GTIN {ult_gtin} encontrado no Grupo {idx + 1}")
            logging.info(f"GTIN {ult_gtin} encontrado no Grupo {idx + 1}")

            if idx + 1 < len(grupos):
                try:
                    # Verifica se o arquivo de índice existe
                    with open(arquivo_indice, "r") as f:
                        # Se existe, é porque a última falhou. Repete o grupo atual.
                        logging.info(f"##### ÚLTIMA EXECUÇÃO NÃO FINALIZADA, ENVIANDO GRUPO {idx + 1} #####")
                        print(f"##### ÚLTIMA EXECUÇÃO NÃO FINALIZADA, ENVIANDO GRUPO {idx + 1} #####")
                        return grupos[idx]
                except FileNotFoundError:
                    # Se não existe, a última foi bem-sucedida. Envia o próximo grupo.
                    print(f"Enviando Grupo {idx + 2} para busca...")
                    logging.info(f"Enviando Grupo {idx + 2} para busca...")
                    return grupos[idx + 1]  
            else:
                # Era o último grupo. Volta para o primeiro.
                print("Não há mais grupos para enviar. Retornando o primeiro grupo.")
                logging.info("Não há mais grupos para enviar. Retornando o primeiro grupo.")
                return grupos[0]  

    print(f"GTIN {ult_gtin} não encontrado em nenhum dos grupos. Retornando o primeiro grupo.")
    logging.info(f"GTIN {ult_gtin} não encontrado em nenhum dos grupos. Retornando o primeiro grupo.")
    return grupos[0] if grupos else pd.DataFrame(columns=["gtin"])

def gerar_consultas(Geohashs, EANs):
    """
    Gera o "produto cartesiano" entre EANs e Geohashs para criar
    a lista de todas as consultas que a API do Menor Preço deve fazer.
    """
    print("##### CALCULANDO O NÚMERO DE CONSULTAS QUE SERÃO FEITAS #####")
    logging.info("##### CALCULANDO O NÚMERO DE CONSULTAS QUE SERÃO FEITAS #####")

    if EANs.empty or Geohashs.empty:
        print("##### ATENÇÃO: Lista de EANs ou Geohashs está vazia. Nenhuma consulta será gerada. #####")
        logging.warning("##### ATENÇÃO: Lista de EANs ou Geohashs está vazia. Nenhuma consulta será gerada. #####")
        return pd.DataFrame(columns=["gtin", "geohash", "index"])

    EANs = EANs.copy()
    EANs.loc[:, "key"] = 1
    Geohashs = Geohashs.copy()
    Geohashs.loc[:, "key"] = 1

    consultas = pd.merge(
        EANs,
        Geohashs,
        on="key",
    ).drop("key", axis=1)
    consultas.reset_index(drop=True, inplace=True)
    consultas["index"] = consultas.index
    
    print(f"##### {len(consultas)} CONSULTAS GERADAS #####")
    logging.info(f"##### {len(consultas)} CONSULTAS GERADAS #####")

    return consultas