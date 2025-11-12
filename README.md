# MP Feeder (v1.30)

Algoritmo em Python para a captação de notas fiscais da plataforma Menor Preço (Nota Paraná) e inserção em um banco de dados MariaDB.

O script foi desenvolvido para coletar dados de preços de concorrentes com base em uma lista de produtos (GTINs) e geolocalizações (Geohashs) pré-definidas.

## O que este projeto faz (Principais Funcionalidades)

Este projeto é um pipeline de ETL (Extração, Transformação e Carga) completo e resiliente.

* **Atualização Inteligente de Produtos:** Periodicamente (a cada 30+ dias), o script reconstrói a lista de 1000 produtos-alvo (`bronze_menorPreco_produtos`). Ele cruza os 2000 produtos mais vendidos por *valor* e *quantidade* da `bronze_plugpharma_vendas` e, em seguida, busca o **GTIN principal** (`codigo_principal = 1`) para cada um na `bronze_plugpharma_produtos`.
* **Coleta Rotativa (Batch):** O script não consulta os 1000 produtos de uma vez. Ele divide a lista em lotes de 100 GTINs e processa um lote por execução, continuando de onde parou na execução anterior (lógica gerenciada pelo `ultimo_indice.txt` e `pegar_ultimo_gtin`).
* **Coleta Ampla de Dados:** Utiliza os GTINs do lote como "isca" na API do Menor Preço. No entanto, ele salva *todos* os produtos que a API retorna na nota fiscal, não apenas o produto-isca. Isso enriquece a tabela `bronze_menorPreco_notas` com uma vasta gama de produtos concorrentes.
* **Geocodificação de Novas Lojas:** Ao encontrar uma loja (`id_loja`) não cadastrada na `bronze_menorPreco_lojas`, o script utiliza a API do Google Geocoding para buscar suas coordenadas de latitude e longitude antes de salvá-la.
* **Tolerância a Falhas (Banco de Dados):** Se a inserção final no banco de dados falhar (ex: perda de conexão), o `handle_execution_error` é acionado. Ele salva *todos* os dados coletados (notas e lojas) em arquivos `.csv` locais (`notas_parciais.csv`, `lojas_parciais.csv`).
* **Recuperação Automática:** Na próxima execução, o `main.py` detecta esses arquivos `.csv`. Ele primeiro executa o `run_recovery_flow`, que carrega os dados desses CSVs no banco de dados e depois os apaga, garantindo que nenhum dado seja perdido antes de iniciar uma nova coleta.
* **Monitoramento e Notificações:** Envia mensagens de sucesso ou erro para um chat do Telegram, permitindo o monitoramento remoto da execução.

---

## Como Usar

### 1. Pré-requisitos

Garanta que você tenha um banco de dados MariaDB acessível. O script espera se conectar a um banco chamado `dbDrogamais`.

Você precisará das seguintes tabelas (fontes e destino):
* `bronze_plugpharma_vendas` (para análise de vendas)
* `bronze_plugpharma_produtos` (para buscar GTINs principais)
* `bronze_cidades` (para buscar geohashs)
* `dbSults.tb_report_auditoria_embedded` (para filtrar geohashs)
* `bronze_menorPreco_produtos` (destino da lista de 1000 produtos)
* `bronze_menorPreco_notas` (destino dos dados brutos da API)
* `bronze_menorPreco_lojas` (destino das lojas concorrentes)

### 2. Instalação

Clone o repositório e instale as dependências do Python:

```bash
pip install -r requirements.txt
```
---------------
### 3. Configuração

O script usa um arquivo config.py para armazenar suas chaves e senhas. Este arquivo é ignorado pelo Git.

* 1. Copie o arquivo de exemplo:
```bash
copy config.py.example config.py
```

* 2. Abra o config.py e preencha as variáveis com suas credenciais:

`DB_CONFIG`: Dicionário com user, password, host e port do seu MariaDB.

`GOOGLE_API_KEY`: Sua chave da API do Google Cloud (para o Geocoding).

`TELEGRAM_TOKEN`: O token do seu Bot do Telegram.

`TELEGRAM_CHAT_ID`: O ID do chat para onde as notificações serão enviadas.

----------------------

### 4. Execução

Uma vez configurado, basta executar o `main.py`:

```bash
python main.py
```

O script cuidará do resto, seja iniciando uma nova coleta ou recuperando dados de uma execução anterior com falha.

## Fluxo de Execução

### 1. main.py é iniciado.

### 2. Verifica Falha Anterior: O script procura pelo arquivo notas_parciais.csv.

### 3. Fluxo de Recuperação (Se .csv existe):

    flow.run_recovery_flow é chamado.

    Os dados dos arquivos .csv são lidos e inseridos no banco de dados.

    Os arquivos .csv são removidos após o sucesso da carga.

### 4. Fluxo Normal (Se .csv não existe):

    ow.run_normal_flow é chamado.

*   **[E] Extração:**

    (Opcional) Atualiza a lista de 1000 produtos-alvo se tiver > 30 dias.

    Seleciona o lote de 100 GTINs do dia.

    Gera a lista de consultas (Geohash x GTIN).

* **[T] Transformação (Coleta):**

    api_services.buscar_notas coleta os dados da API do Menor Preço.

    Retorna os DataFrames Notas_geral e Lojas_SC_geral para o main.py.

* **[L] Carga:**

    main.py recebe os DataFrames.

    (Opcional) api_services.buscar_lat_lon_lojas_sc enriquece Lojas_SC_geral com Lat/Lon do Google.

    db_manager.inserir_lojas_sc e db_manager.inserir_notas carregam os dados no MariaDB.

### 5. Finalização

    Sucesso: handle_success limpa o ultimo_indice.txt e envia notificação de sucesso via Telegram.

    Falha (Ex: DB Offline): handle_execution_error é chamado, save_partial_data cria os arquivos .csv para a próxima execução (Passo 2) e envia notificação de erro.


## Estrutura do Projeto

*   `main.py`: Ponto de entrada. Orquestra os fluxos (normal vs. recuperação) e a etapa de Carga (Load).

*   `flow.py`: Contém a lógica de negócio principal para run_normal_flow (Extração e Transformação) e run_recovery_flow (Carga de CSVs).

*   `db_manager.py`: Abstrai toda a comunicação com o banco de dados MariaDB. Contém todas as queries SQL (SELECTs e INSERTs).

*   `api_services.py`: Gerencia todas as chamadas para APIs externas (Nota Paraná, Google Geocoding e Telegram).

*   `etl_utils.py`: Funções auxiliares de transformação de dados (lógica de Pandas), gerenciamento de estado (leitura/escrita do ultimo_indice.txt) e configuração de logging.

*   `error_handler.py`: Funções centralizadas para lidar com exceções, salvar dados parciais em CSV e notificar falhas.

*   `config.py` (e `.example`): Armazena as credenciais e chaves de API.

*   `requirements.txt`: Lista de pacotes Python necessários.

*   `.gitignore`: Define os arquivos que não devem ser versionados (logs, config.py, arquivos .csv, etc.).