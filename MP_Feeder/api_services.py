# MP_Feeder/api_services.py
import requests
import pandas as pd
import logging
import time
import concurrent.futures
from prefect import get_run_logger

# --- SERVIÇO 1: NOTA PARANÁ (MENOR PREÇO) ---

def buscar_notas(Consultas, Lojas):
    """
    Busca as notas fiscais na API do Menor Preço.
    Refatorado para Prefect: Não salva estado, não manda Telegram, apenas retorna dados.
    """
    logger = get_run_logger()
    logger.info("##### COLETANDO NOTAS #####")

    url = "https://menorpreco.notaparana.pr.gov.br/api/v1/produtos"
    notas = []
    lojas_sem_cadastro = []
    
    erros_consecutivos = 0
    LIMITE_ERROS_CONSECUTIVOS = 5
    
    run_completo = True 
    
    # Lógica para filtrar consultas já feitas removida, pois o Flow manda o lote exato.
    
    lojas_cadastradas = set(Lojas["id_loja"])
    total_consultas = len(Consultas)

    if total_consultas == 0:
        logger.info("##### NENHUMA CONSULTA NESTE LOTE. #####")
        return pd.DataFrame(), pd.DataFrame(), True

    for i, (_, row) in enumerate(Consultas.iterrows(), start=1):
        hash_local = row["geohash"]
        ean = row["gtin"]
        indice_atual = row['index'] 

        try:
            time.sleep(0.3)
            params = { "gtin": ean, "local": hash_local, "raio": "20" }
            
            response = requests.get(url, params=params, timeout=20) 
            status_code = response.status_code

            if status_code == 200:
                erros_consecutivos = 0
                data = response.json()
                produtos_encontrados = data.get("produtos", [])
                num_produtos = len(produtos_encontrados)

                for produto in produtos_encontrados:
                    notas.append({
                        "id_nota": produto.get("id"),
                        "datahora": produto.get("datahora"),
                        "id_loja": produto.get("estabelecimento", {}).get("codigo"),
                        "geohash": hash_local,
                        "gtin": produto.get("gtin"),
                        "descricao": produto.get("desc"),
                        "valor_desconto": produto.get("valor_desconto"),
                        "valor_tabela": produto.get("valor_tabela"),
                        "valor": produto.get("valor"),
                        "cidade": produto.get("estabelecimento", {}).get("mun", ""),
                    })
                    id_loja = produto.get("estabelecimento", {}).get("codigo")
                    if id_loja not in lojas_cadastradas:
                        logradouro = (
                            f"{produto.get('estabelecimento', {}).get('tp_logr', '')} "
                            f"{produto.get('estabelecimento', {}).get('nm_logr', '')}, "
                            f"{produto.get('estabelecimento', {}).get('nr_logr', '')}"
                        )
                        lojas_sem_cadastro.append({
                            "id_loja": id_loja,
                            "nome_fantasia": produto.get("estabelecimento", {}).get("nm_fan", ""),
                            "razao_social": produto.get("estabelecimento", {}).get("nm_emp", ""),
                            "logradouro": logradouro.strip(),
                            "cidade": produto.get("estabelecimento", {}).get("mun", ""),
                            "geohash": hash_local,
                            "local": produto.get("local", ""),
                        })
                
                logger.info(f"{i}/{total_consultas} - {ean} - 200 - Encontrados: {num_produtos}")

            elif status_code == 204:
                erros_consecutivos = 0
                logger.info(f"{i}/{total_consultas} - {ean} - 204 - Sem dados")

            elif status_code in (404, 401, 403):
                # Erros críticos lançam exceção para o Prefect pegar e alertar
                raise Exception(f"ERRO FATAL API MENOR PREÇO: {status_code}")
            
            elif 400 <= status_code < 500:
                logger.info(f"❌ Erro de CLIENTE ({status_code}) para GTIN {ean}. PULANDO...")
                logger.warning(f"{i}/{total_consultas} - {ean} - {status_code} - Erro de cliente.")
            
            elif 500 <= status_code < 600:
                raise Exception(f"⚠️ Erro no servidor ({status_code})")
            else:
                raise Exception(f"⚠️ Status code inesperado ({status_code}) para {ean}")

        except Exception as e:
            logger.info(f"❌ Erro na requisição: {e}. PULANDO...")
            logger.error(f"Erro na requisição: {e}")
            
            erros_consecutivos += 1
            if erros_consecutivos >= LIMITE_ERROS_CONSECUTIVOS:
                logger.info(f"❌ LIMITE DE ERROS CONSECUTIVOS ({LIMITE_ERROS_CONSECUTIVOS}) ATINGIDO.")
                # Retorna o que pegou até agora, mas marca como incompleto
                run_completo = False 
                break 
            continue 
    
    logger.info("##### PREPARANDO DADOS COLETADOS... #####")
    Notas_df, Lojas_SC_df = _preparar_saida(notas, lojas_sem_cadastro)
    
    # Retorna APENAS dados e status. O Prefect lida com o resto.
    return Notas_df, Lojas_SC_df, run_completo, 0


def _preparar_saida(notas, lojas_sem_cadastro):
    """Auxiliar para limpar DataFrames."""
    Lojas_SC = pd.DataFrame(lojas_sem_cadastro).drop_duplicates(subset="id_loja")
    if not Lojas_SC.empty:
        Lojas_SC["endereco"] = (
            Lojas_SC["logradouro"] + ", " + Lojas_SC["cidade"] + ", PR, Brasil"
        )
    
    Notas = pd.DataFrame(notas).drop_duplicates(subset="id_nota")
    if "datahora" in Notas.columns and not Notas.empty:
        Notas["datahora"] = Notas["datahora"].astype(str).str.rstrip("Z")
        Notas["datahora"] = pd.to_datetime(Notas["datahora"], errors="coerce")
        Notas["datahora"] = Notas["datahora"].dt.strftime("%Y-%m-%d %H:%M:%S")
        
    return Notas, Lojas_SC

# --- SERVIÇO 2: NOMINATIM GEOCODING ---

def _obter_lat_lon_nominatim(endereco, index, total):
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": endereco, "format": "jsonv2", "addressdetails": 1, "limit": 1}
    headers = {"User-Agent": "mp-feeder/1.0 (admin@drogamais.com.br)"}

    try:
        resposta = requests.get(url, params=params, headers=headers, timeout=20)
        if resposta.status_code != 200: return None, None
        dados = resposta.json()
        if isinstance(dados, list) and len(dados) > 0:
            return dados[0].get("lat"), dados[0].get("lon")
        return None, None
    except Exception:
        return None, None

def buscar_lat_lon_lojas_sc_nominatim(Lojas_SC):
    logger = get_run_logger()
    logger.info("##### BUSCANDO LATITUDE E LONGITUDE (NOMINATIM) #####")

    if Lojas_SC.empty: return Lojas_SC

    # Limite de segurança para não bloquear o IP
    if len(Lojas_SC) > 100:
        Lojas_SC = Lojas_SC.head(100)

    total_lojas = len(Lojas_SC)
    latitudes, longitudes = [], []
    tarefas = []

    for row in Lojas_SC.itertuples():
        endereco_limpo = limpar_endereco_para_nominatim(row.endereco)
        tarefas.append((endereco_limpo, row.Index, total_lojas))

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        for lat, lon in executor.map(lambda p: _obter_lat_lon_nominatim(*p), tarefas):
            latitudes.append(lat)
            longitudes.append(lon)
            time.sleep(1.1) # Respeita limite da API gratuita

    Lojas_SC["Latitude"] = latitudes
    Lojas_SC["Longitude"] = longitudes
    return Lojas_SC

def limpar_endereco_para_nominatim(endereco: str) -> str:
    if not endereco: return ""
    e = endereco.upper().strip().replace("(MUNICÍPIO", "").replace("MUNICIPIO", "").replace(")", "")
    while "  " in e: e = e.replace("  ", " ")
    if "PR" not in e.split(",")[-2:] and not e.endswith("PR"): e = e.rstrip(", ") + ", PR"
    if "BRASIL" not in e: e = e.rstrip(", ") + ", BRASIL"
    return e.replace(",,", ",").replace(", ,", ", ").strip(" ,")

# --- SERVIÇO 3: TELEGRAM (Mantido para uso no Flow) ---
def mandarMSG(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID):
    logger = get_run_logger()
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        requests.get(url, params={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=10)
    except Exception as e:
        logger.info(f"Erro ao enviar Telegram: {e}")