# export_schema.py
import mariadb as mdb
import sys
import os
import re

# Adiciona a pasta raiz ao sys.path para que 'config' seja encontrado
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    from config import DB_CONFIG
except ImportError:
    print("❌ ERRO: 'config.py' não encontrado.")
    print("   Por favor, copie 'config.py.example' para 'config.py' e preencha suas credenciais.")
    sys.exit(1)

# --- CONFIGURAÇÃO ---
# 1. Defina os NOMES dos objetos que você quer exportar
LISTA_TABELAS = [
    'bronze_menorPreco_lojas',
    'bronze_menorPreco_produtos',
    'bronze_menorPreco_notas',
    'silver_menorPreco_notas'
]

LISTA_PROCEDURES = [
    'proc_atualiza_silver_menorPreco_notas' # Use o nome exato da sua procedure no banco
]

# 2. Defina ONDE salvar os arquivos
OUTPUT_DIR = 'migrations'
# --------------------

def clean_table_sql(sql_create, table_name):
    """Limpa o SQL de 'SHOW CREATE TABLE' e garante 'IF NOT EXISTS'."""
    
    # 1. Remove o *valor* do auto_increment (ex: AUTO_INCREMENT=12345)
    #    Isso evita conflitos entre ambientes (Produção vs. Dev).
    #    IMPORTANTE: Isso NÃO remove a *propriedade* AUTO_INCREMENT da coluna.
    sql_no_auto_increment = re.sub(r'AUTO_INCREMENT=\d+\s', '', sql_create)
    
    # 2. Garante que 'IF NOT EXISTS' esteja presente
    #    (O SHOW CREATE TABLE já retorna com `)
    sql_if_not_exists = re.sub(
        r'CREATE TABLE', 
        'CREATE TABLE IF NOT EXISTS', 
        sql_no_auto_increment,
        count=1, # Substitui apenas a primeira ocorrência
        flags=re.IGNORECASE
    )
    
    # Adiciona um ponto-e-vírgula por segurança
    if not sql_if_not_exists.strip().endswith(';'):
        sql_if_not_exists += ';'
        
    return (
        f"-- Definição da tabela: {table_name}\n"
        f"{sql_if_not_exists}\n"
    )

def clean_procedure_sql(sql_create, proc_name):
    """Limpa o SQL de 'SHOW CREATE PROCEDURE' e adiciona DELIMITER."""
    
    # 1. Troca 'CREATE PROCEDURE' por 'CREATE PROCEDURE'
    #    Isso torna o script "idempotente" (pode rodar várias vezes)
    sql_procedure = re.sub(
        r'CREATE\s+(DEFINER=.*?)\s+PROCEDURE', 
        'CREATE PROCEDURE', 
        sql_create, 
        count=1, # Substitui apenas a primeira ocorrência
        flags=re.IGNORECASE
    )
    
    # 2. Monta o arquivo .sql final com DELIMITER
    #    Isso é essencial para que clientes SQL (DBeaver, HeidiSQL)
    #    entendam que a procedure é um bloco único.
    return (
        f"-- Definição da procedure: {proc_name}\n\n"
        f"DELIMITER $$\n\n"
        f"{sql_procedure}$$\n\n"
        f"DELIMITER ;\n"
    )

def exportar_schema():
    """
    Conecta ao banco, busca o código-fonte de tabelas e procedures
    e salva em arquivos .sql versionáveis.
    """
    conn = None
    cursor = None
    
    # Garante que a pasta 'migrations' exista
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Salvando arquivos de schema em: '{OUTPUT_DIR}/'")
    
    try:
        conn = mdb.connect(**DB_CONFIG, database=DB_CONFIG.get('database', 'dbDrogamais'))
        cursor = conn.cursor()
        
        # --- 1. EXPORTAR TABELAS ---
        print("\n--- Exportando Tabelas ---")
        for i, table_name in enumerate(LISTA_TABELAS, 1):
            print(f"Buscando: {table_name}...")
            try:
                cursor.execute(f"SHOW CREATE TABLE {table_name};")
                resultado = cursor.fetchone()
                
                if not resultado:
                    print(f"⚠️ AVISO: Tabela '{table_name}' não encontrada.")
                    continue
                    
                # O SQL está na segunda coluna (índice 1)
                sql_original = resultado[1]
                sql_limpo = clean_table_sql(sql_original, table_name)
                
                # Salva o arquivo com prefixo v1 (estrutura)
                filename = os.path.join(OUTPUT_DIR, f'v1_{i:02d}_{table_name}.sql')
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(sql_limpo)
                print(f" -> Salvo em: {filename}")
            
            except mdb.Error as e:
                print(f"⚠️ AVISO: Não foi possível exportar a tabela '{table_name}'. Erro: {e}")

        # --- 2. EXPORTAR PROCEDURES ---
        print("\n--- Exportando Procedures ---")
        for i, proc_name in enumerate(LISTA_PROCEDURES, 1):
            print(f"Buscando: {proc_name}...")
            try:
                cursor.execute(f"SHOW CREATE PROCEDURE {proc_name};")
                resultado = cursor.fetchone()

                if not resultado:
                    print(f"⚠️ AVISO: Procedure '{proc_name}' não encontrada.")
                    continue

                # O SQL está na terceira coluna (índice 2)
                sql_original = resultado[2]
                sql_limpo = clean_procedure_sql(sql_original, proc_name)
                
                # Salva o arquivo com prefixo v2 (lógica)
                filename = os.path.join(OUTPUT_DIR, f'v2_{i:02d}_{proc_name}.sql')
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(sql_limpo)
                print(f" -> Salvo em: {filename}")

            except mdb.Error as e:
                print(f"⚠️ AVISO: Não foi possível exportar a procedure '{proc_name}'. Erro: {e}")


        print("\n" + "="*50)
        print("✅ SUCESSO! Schema do banco exportado.")
        print("   Verifique os arquivos na pasta 'migrations' e adicione ao Git.")
        print("="*50)

    except mdb.Error as e:
        print(f"❌ Erro de Banco de Dados: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    exportar_schema()