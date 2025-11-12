# 🛍️ MP Feeder (v1.30) 🛒

Algoritmo em Python para a captação de notas fiscais da plataforma Menor Preço (Nota Paraná) e inserção em um banco de dados MariaDB.

O script foi desenvolvido para coletar dados de preços de concorrentes com base em uma lista de produtos (GTINs) e geolocalizações (Geohashs) pré-definidas.

## 🧭 Sumário

* [Principais Funcionalidades](#-principais-funcionalidades)
* [Como Usar](#-como-usar)
* [Fluxo de Execução](#-fluxo-de-execução)
* [Estrutura do Projeto](#-estrutura-do-projeto)

## 🎯 Resumo do Projeto

Este é um pipeline de ETL robusto e tolerante a falhas projetado para:

*   Coletar dados de preços da API do Menor Preço (Nota Paraná).

*   Enriquecer os dados com geocodificação de lojas (Google API) e notificações (Telegram).

*   Carregar os dados em um banco MariaDB, com lógica de recuperação automática em caso de falha.

## ✨ Principais Funcionalidades

Este projeto é um pipeline de ETL (Extração, Transformação e Carga) completo e resiliente.

<details> 
    <summary>🧠 <strong>Atualização Inteligente de Produtos</strong></summary> Periodicamente (a cada 30+ dias), o script reconstrói a lista de 1000 produtos-alvo (<code>bronze_menorPreco_produtos</code>). Ele cruza os 2000 produtos mais vendidos por <i>valor</i> e <i>quantidade</i> da <code>bronze_plugpharma_vendas</code> e, em seguida, busca o <strong>GTIN principal</strong> (<code>codigo_principal = 1</code>) para cada um na <code>bronze_plugpharma_produtos</code>. 
</details>

<details> 
    <summary>🔄 <strong>Coleta Rotativa (Batch)</strong></summary> 
    O script não consulta os 1000 produtos de uma vez. Ele divide a lista em lotes de 100 GTINs e processa um lote por execução, continuando de onde parou na execução anterior (lógica gerenciada pelo <code>ultimo_indice.txt</code> e <code>pegar_ultimo_gtin</code>). 
</details>

<details> 
    <summary>🎣 <strong>Coleta Ampla de Dados</strong></summary> 
    Utiliza os GTINs do lote como "isca" na API do Menor Preço. No entanto, ele salva <i>todos</i> os produtos que a API retorna na nota fiscal, não apenas o produto-isca. Isso enriquece a tabela <code>bronze_menorPreco_notas</code> com uma vasta gama de produtos concorrentes. 
</details>

<details> 
    <summary>🗺️ <strong>Geocodificação de Novas Lojas</strong></summary> 
    Ao encontrar uma loja (<code>id_loja</code>) não cadastrada na <code>bronze_menorPreco_lojas</code>, o script utiliza a API do Google Geocoding para buscar suas coordenadas de latitude e longitude antes de salvá-la. 
</details>

<details> 
    <summary>🛡️ <strong>Tolerância a Falhas (Banco de Dados)</strong></summary> 
    Se a inserção final no banco de dados falhar (ex: perda de conexão), o <code>handle_execution_error</code> é acionado. Ele salva <i>todos</i> os dados coletados (notas e lojas) em arquivos <code>.csv</code> locais (<code>notas_parciais.csv</code>, <code>lojas_parciais.csv</code>). 
</details>

<details> 
    <summary>🔁 <strong>Recuperação Automática</strong></summary> 
    Na próxima execução, o <code>main.py</code> detecta esses arquivos <code>.csv</code>. Ele primeiro executa o <code>run_recovery_flow</code>, que carrega os dados desses CSVs no banco de dados e depois os apaga, garantindo que nenhum dado seja perdido antes de iniciar uma nova coleta. 
</details>

<details> 
    <summary>🔔 <strong>Monitoramento e Notificações</strong></summary> 
    Envia mensagens de sucesso ou erro para um chat do Telegram, permitindo o monitoramento remoto da execução. 
</details>

---

## 🚀 Como Usar

<details> 
    <summary><strong>1. 📋 Pré-requisitos</strong></summary>

Garanta que você tenha um banco de dados MariaDB acessível. O script espera se conectar a um banco chamado <code>dbDrogamais</code>.

Você precisará das seguintes tabelas (fontes e destino):

<ul> 
    <li><code>bronze_plugpharma_vendas</code> (para análise de vendas)</li> 
    <li><code>bronze_plugpharma_produtos</code> (para buscar GTINs principais)</li> 
    <li><code>bronze_cidades</code> (para buscar geohashs)</li> 
    <li><code>dbSults.tb_report_auditoria_embedded</code> (para filtrar geohashs)</li> 
    <li><code>bronze_menorPreco_produtos</code> (destino da lista de 1000 produtos)</li> 
    <li><code>bronze_menorPreco_notas</code> (destino dos dados brutos da API)</li> 
    <li><code>bronze_menorPreco_lojas</code> (destino das lojas concorrentes)</li> 
</ul> 

</details>

<details>
    <summary><strong>2. 💻 Instalação</strong></summary>

Clone o repositório e instale as dependências do Python:

```bash
pip install -r requirements.txt
```

</details>

<details> 
    <summary><strong>3. 🔑 Configuração</strong></summary>

O script usa um arquivo <code>config.py</code> para armazenar suas chaves e senhas. Este arquivo é ignorado pelo Git.

Copie o arquivo de exemplo (use <code>copy</code> no Windows ou <code>cp</code> no Linux/Mac):
```bash
copy config.py.example config.py
```
Abra o <code>config.py</code> e preencha as variáveis com suas credenciais:

<ul>
    <li><strong><code>DB_CONFIG</code></strong>: Dicionário com <code>user</code>, <code>password</code>, <code>host</code> e <code>port</code> do seu MariaDB.</li> 
    <li><strong><code>GOOGLE_API_KEY</code></strong>: Sua chave da API do Google Cloud (para o Geocoding).</li> 
    <li><strong><code>TELEGRAM_TOKEN</code></strong>: O token do seu Bot do Telegram.</li> 
    <li><strong><code>TELEGRAM_CHAT_ID</code></strong>: O ID do chat para onde as notificações serão enviadas.</li> 
</ul>

</details>

<details> 
    <summary><strong>4. 🏗️ Inicialização do Banco (Primeira Execução)</strong></summary>
Antes de executar o pipeline principal pela primeira vez, você precisa garantir que as tabelas de destino existam. O script init_db.py faz isso para você.
    
```bash
python init_db.py
```

Este script irá criar as tabelas bronze_menorPreco_produtos, bronze_menorPreco_lojas e bronze_menorPreco_notas com o esquema e collate corretos, caso elas ainda não existam.

</details>

<details> 
    <summary><strong>4. ▶️ Execução</strong></summary>

Uma vez configurado, basta executar o <code>main.py</code>:

```bash
python main.py
```

O script cuidará do resto, seja iniciando uma nova coleta ou recuperando dados de uma execução anterior com falha.

</details>

---

## 📊 Fluxo de Execução

<details>
    <summary><strong>1. main.py</strong></summary>
    <ul>
        <li>Inicio da orquestração</li>
    </ul>
</details>

<details>
    <summary><strong>2. Verifica Falha Anterior</strong></summary>
    <ul>
        <li>O script procura pelo arquivo notas_parciais.csv.</li>
    </ul>
</details>

<details> 
    <summary><strong>3. Fluxo de Recuperação (Se .csv existe)</strong></summary> 
    <ul> 
        <li><code>flow.run_recovery_flow</code> é chamado.</li> 
        <li>Os dados dos arquivos .csv são lidos e inseridos no banco de dados.</li> 
        <li>Os arquivos .csv são removidos após o sucesso da carga.</li> 
    </ul> 
</details>

<details> 
    <summary><strong>4. Fluxo Normal (Se .csv não existe)</strong></summary> 
    <ul> 
        <li><code>flow.run_normal_flow</code> é chamado.</li> 
        <li><strong>[E] Extração:</strong> 
            <ul> 
                <li>(Opcional) Atualiza a lista de 1000 produtos-alvo se tiver > 30 dias.</li> 
                <li>Seleciona o lote de 100 GTINs do dia.</li> 
                <li>Gera a lista de consultas (Geohash x GTIN).</li> 
            </ul> 
        </li> 
        <li><strong>[T] Transformação (Coleta):</strong> 
            <ul> 
                <li><code>api_services.buscar_notas</code> coleta os dados da API do Menor Preço.</li> 
                <li>Retorna os DataFrames <code>Notas_geral</code> e <code>Lojas_SC_geral</code> para o <code>main.py</code>.</li> 
            </ul> 
        </li> 
        <li><strong>[L] Carga:</strong> 
            <ul> 
                <li><code>main.py</code> recebe os DataFrames.</li> 
                <li>(Opcional) <code>api_services.buscar_lat_lon_lojas_sc</code> enriquece <code>Lojas_SC_geral</code> com Lat/Lon do Google.</li> 
                <li><code>db_manager.inserir_lojas_sc</code> e <code>db_manager.inserir_notas</code> carregam os dados no MariaDB.</li> 
            </ul> 
        </li> 
    </ul> 
</details>

<details> 
    <summary><strong>5. Finalização</strong></summary> 
    <ul> 
        <li><strong>Sucesso:</strong> <code>handle_success</code> limpa o <code>ultimo_indice.txt</code> e envia notificação de sucesso via Telegram.</li> 
        <li><strong>Falha (Ex: DB Offline):</strong> <code>handle_execution_error</code> é chamado, <code>save_partial_data</code> cria os arquivos .csv para a próxima execução e envia notificação de erro.</li> 
    </ul> 
</details>

---

## 📂 Estrutura do Projeto

<details> <summary>🚦 <strong>main.py</strong></summary> Ponto de entrada. Orquestra os fluxos (normal vs. recuperação) e a etapa de Carga (Load). </details>

<details> <summary>🏃‍♂️ <strong>flow.py</strong></summary> Contém a lógica de negócio principal para <code>run_normal_flow</code> (Extração e Transformação) e <code>run_recovery_flow</code> (Carga de CSVs). </details>

<details> <summary>🗃️ <strong>db_manager.py</strong></summary> Abstrai toda a comunicação com o banco de dados MariaDB. Contém todas as queries SQL (SELECTs e INSERTs). </details>

<details> <summary>☁️ <strong>api_services.py</strong></summary> Gerencia todas as chamadas para APIs externas (Nota Paraná, Google Geocoding e Telegram). </details>

<details> <summary>🛠️ <strong>etl_utils.py</strong></summary> Funções auxiliares de transformação de dados (Pandas), gerenciamento de estado (leitura/escrita do <code>ultimo_indice.txt</code>) e configuração de logging. </details>

<details> <summary>🚨 <strong>error_handler.py</strong></summary> Funções centralizadas para lidar com exceções, salvar CSVs e notificar falhas. </details>

<details> <summary>🏗️ <strong>init_db.py</strong></summary> Script de inicialização. Garante que as tabelas de destino (bronze_menorPreco_*) existam no banco de dados com o esquema e collate corretos. </details>

<details> <summary>🔒 <strong>config.py (e .example)</strong></summary> Armazena as credenciais e chaves de API. </details>

<details> <summary>📦 <strong>requirements.txt</strong></summary> Lista de pacotes Python necessários. </details>

<details> <summary>🙈 <strong>.gitignore</strong></summary> Define os arquivos que não devem ser versionados (logs, config.py, arquivos .csv, etc.). </details>
