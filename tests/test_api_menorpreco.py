# tests/test_api_menorpreco.py

import requests
import json
import time


# --- DADOS DE TESTE ---
GEOHASH_LONDRINA = "6gge7u6cc"  # Geohash para o centro de Londrina, PR
GTIN_COMUM = "7894900011517"  # Coca-Cola 2L
GTIN_COMUM2 = "0000000000001"  # Produto inexistente
RAIO_KM = "20"


def formatar_endereco(estabelecimento):
    """Monta um endereço legível a partir do JSON da API."""
    if not estabelecimento:
        return "Endereço não disponível"

    tp_logr = estabelecimento.get("tp_logr", "") or ""
    nm_logr = estabelecimento.get("nm_logr", "") or ""
    nr_logr = estabelecimento.get("nr_logr", "") or ""
    complemento = estabelecimento.get("complemento", "") or ""
    bairro = estabelecimento.get("bairro", "") or ""
    uf = estabelecimento.get("uf", "") or ""
    cod_mun = estabelecimento.get("mun", "") or ""

    # Formata o endereço principal
    endereco = f"{tp_logr} {nm_logr}, {nr_logr}".strip().replace("  ", " ")
    if complemento:
        endereco += f" ({complemento})"
    if bairro:
        endereco += f" - {bairro}"
    if uf:
        endereco += f", {uf}"
    if cod_mun:
        endereco += f" (município {cod_mun})"

    return endereco.strip().strip(",")


def testar_api_menor_preco(gtin, geohash, raio):
    """
    Testa uma consulta isolada na API do Menor Preço,
    imitando a lógica do api_services.py
    """
    url = "https://menorpreco.notaparana.pr.gov.br/api/v1/produtos"
    params = {"gtin": gtin, "local": geohash, "raio": raio}

    print(f"\n--- TESTANDO CONSULTA ---")
    print(f" GTIN: {gtin}")
    print(f" Geohash: {geohash}")
    print(f" Raio: {raio} km")

    try:
        start_time = time.time()
        resposta = requests.get(url, params=params, timeout=20)
        duration = time.time() - start_time
        status_code = resposta.status_code

        print(f"--- HTTP {status_code} --- (Duração: {duration:.2f}s)")

        if status_code == 200:
            dados = resposta.json()
            produtos_encontrados = dados.get("produtos", [])
            num_produtos = len(produtos_encontrados)
            print(f"✅ API respondeu com {num_produtos} notas encontradas.")

            if num_produtos > 0:
                primeira_nota = produtos_encontrados[0]
                estabelecimento = primeira_nota.get("estabelecimento", {})

                print("\n--- Exemplo da primeira nota ---")
                print(json.dumps(primeira_nota, indent=2, ensure_ascii=False))

                endereco_formatado = formatar_endereco(estabelecimento)
                print(f"\n📍 Endereço formatado: {endereco_formatado}")

        elif status_code == 204:
            print(f"✅ SUCESSO (sem dados): nenhum resultado para esta busca.")

        elif status_code in (404, 401, 403):
            print(f"❌ ERRO CRÍTICO: Status {status_code} — verifique a URL ou autenticação.")

        elif 500 <= status_code < 600:
            print(f"❌ ERRO DE SERVIDOR ({status_code}) — API do Menor Preço está com instabilidade.")

        else:
            print(f"❌ ERRO INESPERADO ({status_code})")

    except requests.RequestException as e:
        print(f"❌ ERRO DE REDE (Timeout ou DNS): {e}")


# --- EXECUÇÃO DOS TESTES ---
if __name__ == "__main__":
    print("Iniciando Teste 1: Produto comum (deve achar notas)")
    testar_api_menor_preco(GTIN_COMUM, GEOHASH_LONDRINA, RAIO_KM)

    print("\n" + "=" * 50 + "\n")

    print("Iniciando Teste 2: Produto falso (deve retornar 204 - Sem dados)")
    testar_api_menor_preco(GTIN_COMUM2, GEOHASH_LONDRINA, RAIO_KM)
