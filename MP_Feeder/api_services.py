# api_services.py
import requests
import pandas as pd
import logging
import time
import concurrent.futures

# --- SERVIÇO 1: NOTA PARANÁ (MENOR PREÇO) ---

def buscar_notas(Consultas, Lojas, ultimo_indice, arquivo_indice, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID):
    """
    Busca as notas fiscais na API do Menor Preço.
    Agora recebe os tokens do Telegram para poder notificar em caso de erro.
    """
    print("##### COLETANDO NOTAS #####")
    logging.info("##### COLETANDO NOTAS #####")

    url = "https://menorpreco.notaparana.pr.gov.br/api/v1/produtos"
    notas = []
    lojas_sem_cadastro = []
    
    erros_consecutivos = 0
    LIMITE_ERROS_CONSECUTIVOS = 5
    
    run_completo = True 
    indice_salvar = ultimo_indice

    if Consultas.empty:
        print("##### NENHUMA CONSULTA PARA REALIZAR. PULANDO A COLETA DE NOTAS. #####")
        logging.warning("##### NENHUMA CONSULTA PARA REALIZAR. PULANDO A COLETA DE NOTAS. #####")
        return pd.DataFrame(), pd.DataFrame(), run_completo, indice_salvar

    Consultas = Consultas[Consultas["index"] >= ultimo_indice]
    lojas_cadastradas = set(Lojas["id_loja"])
    
    total_consultas = len(Consultas)
    if total_consultas == 0:
        print("##### NENHUMA CONSULTA RESTANTE A PARTIR DO ÍNDICE RECUPERADO. #####")
        logging.warning("##### NENHUMA CONSULTA RESTANTE A PARTIR DO ÍNDICE RECUPERADO. #####")
        return pd.DataFrame(), pd.DataFrame(), run_completo, indice_salvar

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
                    notas.append(
                        {
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
                        }
                    )
                    id_loja = produto.get("estabelecimento", {}).get("codigo")
                    if id_loja not in lojas_cadastradas:
                        logradouro = (
                            f"{produto.get('estabelecimento', {}).get('tp_logr', '')} "
                            f"{produto.get('estabelecimento', {}).get('nm_logr', '')}, "
                            f"{produto.get('estabelecimento', {}).get('nr_logr', '')}"
                        )
                        lojas_sem_cadastro.append(
                            {
                                "id_loja": id_loja,
                                "nome_fantasia": produto.get("estabelecimento", {}).get("nm_fan", ""),
                                "razao_social": produto.get("estabelecimento", {}).get("nm_emp", ""),
                                "logradouro": logradouro.strip(),
                                "cidade": produto.get("estabelecimento", {}).get("mun", ""),
                                "geohash": hash_local,
                                "local": produto.get("local", ""),
                            }
                        )
                
                print(f"{i}/{total_consultas} (Índice: {indice_atual}) - {ean} - 200 (Encontrados: {num_produtos})")
                logging.info(f"{i}/{total_consultas} (Índice: {indice_atual}) - {ean} - 200 - Encontrados: {num_produtos}")
                
                indice_salvar = indice_atual + 1

            elif status_code == 204:
                erros_consecutivos = 0
                print(f"{i}/{total_consultas} (Índice: {indice_atual}) - {ean} - 204 (Sem dados)")
                logging.info(f"{i}/{total_consultas} (Índice: {indice_atual}) - {ean} - 204 - Sem dados")
                indice_salvar = indice_atual + 1

            elif status_code in (404, 401, 403):
                msg_erro = f"❌ ERRO GRAVE {status_code}: API Menor Preço pode estar offline ou URL errada. Script interrompido."
                logging.critical(msg_erro)
                mandarMSG(msg_erro, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID) # <-- CORREÇÃO
                raise Exception(f"ERRO FATAL {status_code}. Abortando.")
            
            elif 400 <= status_code < 500:
                print(f"❌ Erro de CLIENTE ({status_code}) para GTIN {ean}. PULANDO...")
                logging.warning(f"{i}/{total_consultas} (Índice: {indice_atual}) - {ean} - {status_code} - Erro de cliente. Pulando.")
                indice_salvar = indice_atual + 1
            
            elif 500 <= status_code < 600:
                raise Exception(f"⚠️ Erro no servidor ({status_code})")
            else:
                raise Exception(f"⚠️ Status code inesperado ({status_code}) para {ean}")

        except Exception as e:
            if "ERRO FATAL" in str(e):
                raise e

            print(f"❌ Erro na requisição para GTIN {ean} e geohash {hash_local}: {e}. PULANDO...")
            logging.error(f"Erro na requisição para GTIN {ean} e geohash {hash_local}: {e}. PULANDO...")
            
            erros_consecutivos += 1
            
            if erros_consecutivos >= LIMITE_ERROS_CONSECUTIVOS:
                print(f"❌ LIMITE DE {LIMITE_ERROS_CONSECUTIVOS} ERROS CONSECUTIVOS ATINGIDO.")
                logging.critical(f"LIMITE DE {LIMITE_ERROS_CONSECUTIVOS} ERROS CONSECUTIVOS ATINGIDO. INTERROMPENDO O LOOP.")
                
                msg_erro = f"❌ ERRO GRAVE: {LIMITE_ERROS_CONSECUTIVOS} erros consecutivos no Menor Preço. Loop interrompido. Salvando dados parciais..."
                mandarMSG(msg_erro, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID) # <-- CORREÇÃO
                
                run_completo = False 
                break 
            
            continue 
    
    print("##### PREPARANDO DADOS COLETADOS PARA SAÍDA... #####")
    Notas_df, Lojas_SC_df = _preparar_saida(notas, lojas_sem_cadastro)
    
    return Notas_df, Lojas_SC_df, run_completo, indice_salvar


def _preparar_saida(notas, lojas_sem_cadastro):
    """
    Função auxiliar privada. Converte as listas de resultados em DataFrames limpos.
    """
    Lojas_SC = pd.DataFrame(lojas_sem_cadastro).drop_duplicates(subset="id_loja")
    if not Lojas_SC.empty:
        Lojas_SC["endereco"] = (
            Lojas_SC["logradouro"] + ", " + Lojas_SC["cidade"] + ", PR, Brasil"
        )
    else:
        print("#### ⚠️  AVISO: TODAS AS LOJAS BUSCADAS DETÉM CADASTRO. #### ")

    Notas = pd.DataFrame(notas).drop_duplicates(subset="id_nota")

    if "datahora" in Notas.columns and not Notas.empty:
        Notas["datahora"] = Notas["datahora"].astype(str).str.rstrip("Z")
        Notas["datahora"] = pd.to_datetime(Notas["datahora"], errors="coerce")
        Notas["datahora"] = Notas["datahora"].dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        print("⚠️ Nenhuma nota com campo 'datahora' foi retornada.")
        
    return Notas, Lojas_SC

# --- SERVIÇO 2: NOMINATIM GEOCODING ---

def _obter_lat_lon_nominatim(endereco, index, total):
    """
    Função auxiliar privada. Busca a coordenada de UM endereço usando Nominatim (OpenStreetMap).
    Não requer chave de API.
    """

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": endereco,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": 1,
    }
    headers = {"User-Agent": "mp-feeder/1.0 (contato@seudominio.com)"}

    try:
        resposta = requests.get(url, params=params, headers=headers, timeout=20)
        status_code = resposta.status_code

        if status_code != 200:
            logging.error(f"{index+1}/{total} - {endereco} - ERRO HTTP: {status_code}")
            return None, None

        dados = resposta.json()
        if isinstance(dados, list) and len(dados) > 0:
            item = dados[0]
            latitude = item.get("lat")
            longitude = item.get("lon")
            display_name = item.get("display_name")

            logging.info(
                f"{index+1}/{total} - {endereco} - SUCESSO - LAT: {latitude}, LON: {longitude}"
            )
            return latitude, longitude
        else:
            logging.warning(f"{index+1}/{total} - {endereco} - Não encontrado no Nominatim")
            return None, None

    except requests.RequestException as e:
        logging.error(f"{index+1}/{total} - {endereco} - ERRO de rede/timeout - {e}")
        return None, None


def buscar_lat_lon_lojas_sc_nominatim(Lojas_SC):
    """
    Versão sem Google. Usa Nominatim (gratuito e sem chave) para buscar coordenadas.
    """

    print("##### BUSCANDO LATITUDE E LONGITUDE (NOMINATIM) #####")
    logging.info("##### BUSCANDO LATITUDE E LONGITUDE (NOMINATIM) #####")

    LIMITE_DIARIO_GEOCODING = 100  # respeite o uso público
    total_encontradas = len(Lojas_SC)

    if total_encontradas > LIMITE_DIARIO_GEOCODING:
        print(f"⚠️ Limitando para {LIMITE_DIARIO_GEOCODING} lojas/dia (limite ético Nominatim).")
        Lojas_SC = Lojas_SC.sample(n=LIMITE_DIARIO_GEOCODING).copy()

    elif Lojas_SC.empty:
        print("##### NENHUMA LOJA NOVA PARA BUSCAR LAT/LON. #####")
        logging.info("##### NENHUMA LOJA NOVA PARA BUSCAR LAT/LON. #####")
        return Lojas_SC

    total_lojas = len(Lojas_SC)
    latitudes = []
    longitudes = []
    tarefas = []

    # monta tarefas
    for row in Lojas_SC.itertuples():
        # Padroniza endereço para evitar confusões
        endereco_limpo = limpar_endereco_para_nominatim(row.endereco)

        tarefas.append((endereco_limpo, row.Index, total_lojas))

    print(f"Iniciando busca paralela de {total_lojas} endereços...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        for lat, lon in executor.map(lambda p: _obter_lat_lon_nominatim(*p), tarefas):
            latitudes.append(lat)
            longitudes.append(lon)
            time.sleep(1.1)  # respeitar limite de 1 req/s no Nominatim

    Lojas_SC["Latitude"] = latitudes
    Lojas_SC["Longitude"] = longitudes

    print("✅ Busca concluída (Nominatim)!")
    return Lojas_SC


def limpar_endereco_para_nominatim(endereco: str) -> str:
    if not endereco:
        return ""

    e = endereco.upper().strip()

    # Remove "(MUNICÍPIO ...)" e parênteses soltos
    e = e.replace("(MUNICÍPIO", "")
    e = e.replace("(MUNICIPIO", "")
    e = e.replace("MUNICÍPIO", "")
    e = e.replace("MUNICIPIO", "")
    e = e.replace(")", "")
    e = e.replace("(", "")

    # Remove múltiplos espaços
    while "  " in e:
        e = e.replace("  ", " ")

    # Remove sufixos já existentes para recompor corretamente
    for sufixo in [", BRASIL", "BRASIL", ", PR", "PR"]:
        if e.endswith(sufixo):
            e = e[: -len(sufixo)].rstrip(" ,")
    
    # Reconstrói o final padronizado
    e = f"{e}, PR, BRASIL"

    # Trata vírgulas duplicadas
    while ",," in e:
        e = e.replace(",,", ",")
    e = e.replace(", ,", ", ").strip(" ,")

    return e



# --- SERVIÇO 3: TELEGRAM ---
def mandarMSG(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID):
    """
    Envia uma mensagem para o Telegram.
    Recebe o Token e o Chat ID como parâmetros.
    """
    token = TELEGRAM_TOKEN 
    chat_id = TELEGRAM_CHAT_ID 
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {"chat_id": chat_id, "text": message}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status() 
        logging.info(f"Mensagem enviada para o Telegram: {message}")
    except requests.RequestException as e:
        print(f"❌ ERRO AO ENVIAR MENSAGEM PARA O TELEGRAM: {e}")
        logging.error(f"❌ ERRO AO ENVIAR MENSAGEM PARA O TELEGRAM: {e}")