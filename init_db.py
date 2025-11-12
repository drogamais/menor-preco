# init_db.py
import mariadb as mdb
from config import DB_CONFIG
import os
import re
import sys # <-- Adicionado para 'exit'

# Define o diretório onde os arquivos .sql estão
MIGRATIONS_DIR = 'migrations'

def read_sql_file(filename):
    """
    Lê o conteúdo de um arquivo .sql do diretório de migrações.
    """
    filepath = os.path.join(MIGRATIONS_DIR, filename)
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return f.read()
    except FileNotFoundError:
        print(f"❌ ERRO: Arquivo de migração não encontrado: {filepath}")
        raise
    except Exception as e:
        print(f"❌ ERRO ao ler o arquivo {filepath}: {e}")
        raise

def clean_procedure_sql(sql_raw):
    """
    Remove os comandos DELIMITER para que o conector Python possa
    executar o 'CREATE PROCEDURE' como um único comando.
    """
    # Remove comentários -- que podem estar antes do DELIMITER
    sql_no_comments = re.sub(r'--.*?\n', '\n', sql_raw)
    
    # Encontra o bloco principal do CREATE PROCEDURE
    match = re.search(r'DELIMITER\s+\$\$(.*)\$\$\s*DELIMITER\s+;', sql_no_comments, re.IGNORECASE | re.DOTALL)
    
    if match:
        # Retorna apenas o bloco interno
        return match.group(1).strip()
    
    # Fallback: se o regex falhar, tenta uma limpeza mais simples
    sql_cleaned = re.sub(r'DELIMITER\s+\$\$\s*', '', sql_no_comments, flags=re.IGNORECASE)
    sql_cleaned = re.sub(r'\$\$\s*DELIMITER\s+;\s*$', '', sql_cleaned, flags=re.IGNORECASE | re.DOTALL)
    
    return sql_cleaned.strip()


def inicializar_banco():
    """
    Conecta ao banco, encontra TODOS os arquivos .sql na pasta 'migrations'
    e os executa em ordem alfabética.
    """
    conn = None
    cursor = None
    current_file = None # Variável para saber qual arquivo falhou
    
    try:
        # --- 1. Encontrar e ordenar os arquivos de migração ---
        print(f"Procurando por arquivos .sql em '{MIGRATIONS_DIR}'...")
        
        if not os.path.exists(MIGRATIONS_DIR):
            print(f"❌ ERRO: O diretório '{MIGRATIONS_DIR}' não foi encontrado.")
            sys.exit(1)
            
        # Lista todos os arquivos no diretório
        all_files = os.listdir(MIGRATIONS_DIR)
        
        # Filtra apenas os arquivos .sql
        sql_files = [f for f in all_files if f.endswith('.sql')]
        
        # Ordena os arquivos (v1_01, v1_02, v2_01, etc.)
        sql_files.sort()
        
        if not sql_files:
            print(f"Nenhum arquivo .sql encontrado em '{MIGRATIONS_DIR}'. Nada a fazer.")
            return

        print(f"Arquivos de migração encontrados e ordenados:")
        for f in sql_files:
            print(f"  - {f}")
        
        # --- 2. Conectar ao banco ---
        print("\nConectando ao banco de dados 'dbDrogamais'...")
        conn = mdb.connect(**DB_CONFIG, database="dbDrogamais")
        cursor = conn.cursor()
        
        print("\n--- Iniciando execução das migrações ---")

        # --- 3. Executar cada arquivo ---
        for filename in sql_files:
            current_file = filename # Armazena o arquivo atual para o log de erro
            print(f"Executando: {filename}...")
            
            # Lê o conteúdo do arquivo
            sql_raw = read_sql_file(filename)
            
            sql_to_execute = ""
            
            # Verifica se é uma procedure (pela convenção de nome v2_)
            if filename.startswith('v2_'):
                sql_to_execute = clean_procedure_sql(sql_raw)
            else:
                # É um arquivo de tabela (v1_)
                sql_to_execute = sql_raw
            
            # Executa o SQL
            if sql_to_execute.strip():
                # Executa o conteúdo do arquivo como um único comando
                cursor.execute(sql_to_execute) 
            else:
                print(f"⚠️ AVISO: Nenhum SQL executável encontrado em {filename}, pulando...")

        # --- 4. Commit ---
        conn.commit()
        print("\n" + "="*50)
        print("✅ SUCESSO! Todas as migrações foram executadas.")
        print("="*50)
    
    except mdb.Error as e:
        print(f"\n❌ ERRO DE BANCO DE DADOS (ao executar {current_file}): {e}")
        if conn:
            print("Executando rollback...")
            conn.rollback() # Desfaz qualquer alteração parcial
    except Exception as e:
        # Captura outros erros (ex: FileNotFoundError)
        print(f"\n❌ Erro inesperado (no arquivo {current_file}): {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Conexão com o banco fechada.")

if __name__ == "__main__":
    print("Iniciando setup do banco de dados...")
    inicializar_banco()