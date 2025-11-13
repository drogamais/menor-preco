import requests
import json

def testar_nominatim_api(endereco):
    """
    Versão de teste isolada para o Nominatim (OpenStreetMap).
    Não requer chave de API.
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": endereco,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": 1,
    }
    headers = {"User-Agent": "meu-app-teste/1.0 (email@exemplo.com)"}  # obrigatório

    print(f"\n--- 1. TESTANDO ENDEREÇO: '{endereco}' ---")

    try:
        resposta = requests.get(url, params=params, headers=headers, timeout=20)
        status_code = resposta.status_code 
        print(f"--- 2. Status Code HTTP: {status_code} ---")

        if status_code != 200:
            print(f"❌ ERRO HTTP: {status_code}")
            return None, None

        dados = resposta.json()
        # print(json.dumps(dados, indent=2, ensure_ascii=False))  # se quiser ver o JSON completo

        if isinstance(dados, list) and len(dados) > 0:
            item = dados[0]
            latitude = item.get("lat")
            longitude = item.get("lon")
            display_name = item.get("display_name")

            print(f"✅ SUCESSO!")
            print(f"   Endereço encontrado: {display_name}")
            print(f"   Latitude: {latitude}")
            print(f"   Longitude: {longitude}")
            return latitude, longitude
        else:
            print(f"⚠️ AVISO: Nenhum resultado encontrado para '{endereco}'.")
            return None, None

    except requests.RequestException as e:
        print(f"❌ ERRO DE REDE (Timeout ou DNS): {e}")
        return None, None


# --- EXECUÇÃO DOS TESTES ---
if __name__ == "__main__":
    # Teste 1: Um endereço que DEVE funcionar

    #testar_nominatim_api("RUA PARANAPANEMA, 1541, PR, Brasil")

    
    testar_nominatim_api("RUA MARANHAO, 143, CENTRO, LONDRINA, PR, BRASIL")
    testar_nominatim_api("RUA MARANHAO, 143, CENTRO, LONDRINA, PR")
    testar_nominatim_api("RUA MARANHAO, 143, CENTRO, LONDRINA")
    testar_nominatim_api("RUA MARANHAO, 143")

    # Teste 2: Um endereço que NÃO DEVE ser encontrado
    testar_nominatim_api("Rua Fictícia, 1234, Nárnia")
