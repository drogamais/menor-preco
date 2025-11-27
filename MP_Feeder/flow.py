# flow.py
import os
import logging
import pandas as pd

# Importa as ferramentas de cada módulo
from MP_Feeder.db_manager import (
    pegar_ultima_att_gtins, fetch_dados_vendas_para_produtos, 
    insert_produtos_atualizados, pegar_geohashs_BD,
    coletar_produtos_no_banco, pegar_ultimo_gtin, coletar_lojas_do_banco,
    inserir_lojas_sc, inserir_notas,
    fetch_gtins_principais
)
from MP_Feeder.api_services import buscar_notas, buscar_lat_lon_lojas_sc_nominatim
from MP_Feeder.etl_utils import (
    recuperar_ultimo_indice, transformar_dados_produtos,
    grupo_eans_selecionados, gerar_consultas
)

def run_recovery_flow(configs, now_gmt3):
    """
    Executa a lógica de recuperação de dados parciais (CSV).
    (Esta função já está correta, pois ela só carrega dados)
    """
    print("##### ⚠️ DADOS PARCIAIS .CSV ENCONTRADOS. TENTANDO CARREGAR... #####")
    logging.warning("##### DADOS PARCIAIS .CSV ENCONTRADOS. TENTANDO CARREGAR... #####")
    
    # Desempacota configs
    DB_CONFIG = configs['DB_CONFIG']
    arquivo_indice = configs['arquivo_indice']
    arquivo_notas_parciais = configs['arquivo_notas_parciais']
    arquivo_lojas_parciais = configs['arquivo_lojas_parciais']
    
    # Carrega os dados salvos da última falha
    Notas_csv = pd.read_csv(arquivo_notas_parciais)
    Lojas_csv = pd.DataFrame()
    if os.path.exists(arquivo_lojas_parciais):
        Lojas_csv = pd.read_csv(arquivo_lojas_parciais)
    
    # Pega o índice que já foi salvo na falha anterior
    ultimo_indice = recuperar_ultimo_indice(arquivo_indice)
    print(f"##### DADOS CARREGADOS. TENTANDO INSERIR NO BANCO (LOTE DO ÍNDICE {ultimo_indice}) #####")
    
    # 1. Carrega as lojas que JÁ ESTÃO no banco
    print("##### Verificando lojas já cadastradas no banco... #####")
    Lojas_no_banco_df = coletar_lojas_do_banco(DB_CONFIG)
    lojas_ja_cadastradas_set = set(Lojas_no_banco_df["id_loja"])
    print(f"##### {len(lojas_ja_cadastradas_set)} lojas CADASTRADAS no banco. #####")
    
    # 2. Insere as LOJAS do CSV (já filtradas)
    if not Lojas_csv.empty:
        Lojas_csv_sem_duplicatas = Lojas_csv.drop_duplicates(subset=["id_loja"]).reset_index(drop=True)
        lojas_para_buscar_latlon = Lojas_csv_sem_duplicatas[
            ~Lojas_csv_sem_duplicatas['id_loja'].isin(lojas_ja_cadastradas_set)
        ]
        total_para_buscar = len(lojas_para_buscar_latlon)
        total_ignoradas_csv = len(Lojas_csv_sem_duplicatas) - total_para_buscar
        
        if total_ignoradas_csv > 0:
            print(f"##### Ignorando {total_ignoradas_csv} lojas do CSV que já estão no banco. #####")
        
        if total_para_buscar > 0:
            print(f"##### 💾 RECUPERANDO {total_para_buscar} LOJAS (REAIS) DO CSV... #####")
            Lojas_SC_com_latlon = buscar_lat_lon_lojas_sc_nominatim(lojas_para_buscar_latlon)
            inserir_lojas_sc(Lojas_SC_com_latlon, now_gmt3, DB_CONFIG)
        else:
            print("##### Nenhuma loja nova do CSV para buscar/inserir. #####")
    
    # 3. Insere as NOTAS do CSV
    if not Notas_csv.empty:
        Notas_csv_sem_duplicatas = Notas_csv.drop_duplicates(subset=["id_nota"]).reset_index(drop=True)
        print(f"##### 💾 RECUPERANDO {len(Notas_csv_sem_duplicatas)} NOTAS DO CSV... #####")
        inserir_notas(Notas_csv_sem_duplicatas, now_gmt3, DB_CONFIG)
    
    # 4. Limpa os arquivos CSV
    print("##### SUCESSO AO SALVAR DADOS DO CSV. LIMPANDO ARQUIVOS. #####")
    os.remove(arquivo_notas_parciais)
    if os.path.exists(arquivo_lojas_parciais):
        os.remove(arquivo_lojas_parciais)
    
    # Retorna o índice para o 'main' poder salvar
    return ultimo_indice

def run_normal_flow(configs, now_gmt3, today_gmt3):
    """
    Executa o fluxo normal de ETL (Extração e Transformação).
    A Carga (Load) foi movida para o main.py.
    """
    print("##### NENHUM DADO PARCIAL ENCONTRADO. INICIANDO COLETA NORMAL... #####")
    
    # Desempacota configs
    DB_CONFIG = configs['DB_CONFIG']
    TELEGRAM_TOKEN = configs['TELEGRAM_TOKEN']
    TELEGRAM_CHAT_ID = configs['TELEGRAM_CHAT_ID']
    arquivo_indice = configs['arquivo_indice']

    ### II Verificação da Lista de Produtos (GTINs):
    ultima_att_gtins = pegar_ultima_att_gtins(DB_CONFIG)
    
    ### III Atualização da Lista de Produtos (Se necessário):
    data_31_dias_atras = today_gmt3 - pd.Timedelta(days=31)
    data_para_checar = ultima_att_gtins or data_31_dias_atras
    diferenca_em_dias = (today_gmt3 - data_para_checar).days

    if diferenca_em_dias > 30: 
        print("##### 🔄 ATUALIZANDO LISTA DE PRODUTOS (MAIS DE 30 DIAS) #####")
        logging.info("Atualizando lista de produtos (Mais de 30 dias)...")
        
        # 1. Extrair (Vendas)
        produtos_por_valor, produtos_por_qtd = fetch_dados_vendas_para_produtos(DB_CONFIG)
        
        # 2. Transformar (Passo 1: Pegar Top 1000 produtos)
        Produtos_top_1000 = transformar_dados_produtos(produtos_por_valor, produtos_por_qtd)
        
        # 3. Extrair (Passo 2: Buscar GTINs principais e TIPO para o Top 1000)
        if not Produtos_top_1000.empty:
            codigos = Produtos_top_1000["codigo_interno_produto"].tolist()
            df_extras = fetch_gtins_principais(DB_CONFIG, codigos)

            if not df_extras.empty:
                # Merge para trazer GTIN novo e TIPO
                Produtos_atualizados = pd.merge(
                    Produtos_top_1000, df_extras, 
                    on='codigo_interno_produto', how='left'
                )
                
                # Prioriza GTIN principal
                Produtos_atualizados['GTIN'] = Produtos_atualizados['GTIN_principal'].fillna(Produtos_atualizados['GTIN'])
                
                # Renomeia tipo_produto -> tipo
                Produtos_atualizados.rename(columns={'tipo_produto': 'tipo'}, inplace=True)
                
                # Limpa colunas extras
                Produtos_limpos = Produtos_atualizados.drop(columns=['GTIN_principal'], errors='ignore')
            else:
                
                Produtos_limpos = Produtos_top_1000
                Produtos_limpos['tipo'] = None # Garante a coluna mesmo vazia

            # Envia para o insert (agora incremental)
            insert_produtos_atualizados(DB_CONFIG, Produtos_limpos)
        
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

    ### VI Coleta de Notas (Loop Principal):
    Notas_geral, Lojas_SC_geral, run_completo, indice_para_salvar = buscar_notas(
        Consultas, Lojas, ultimo_indice, arquivo_indice,
        TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
    )
    
    # --- MUDANÇA: REMOVIDA A ETAPA DE CARGA (VIII e IX) ---
    # Os DataFrames agora são apenas retornados
    
    # Retorna o estado da execução para o 'main' decidir o que fazer
    return Notas_geral, Lojas_SC_geral, run_completo, indice_para_salvar