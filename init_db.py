# init_db.py
import mariadb as mdb
from config import DB_CONFIG
import os # <-- Adicionado
import re # <-- Adicionado para uma limpeza mais robusta

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
    # (procura por algo entre "DELIMITER $$" e "$$ DELIMITER ;")
    match = re.search(r'DELIMITER\s+\$\$(.*)\$\$\s*DELIMITER\s+;', sql_no_comments, re.IGNORECASE | re.DOTALL)
    
    if match:
        # Retorna apenas o bloco interno
        return match.group(1).strip()
    
    # Fallback: se o regex falhar, tenta uma limpeza mais simples
    sql_cleaned = re.sub(r'DELIMITER\s+\$\$\s*', '', sql_no_comments, flags=re.IGNORECASE)
    sql_cleaned = re.sub(r'\$\$\s*DELIMITER\s+;\s*$', '', sql_cleaned, flags=re.IGNORECASE | re.DOTALL)
    
    # Remove espaços em branco extras do início e fim
    return sql_cleaned.strip()


def inicializar_banco():
    """
    Conecta ao banco de dados e garante que as tabelas e
    procedures de destino do Menor Preço existam.
    """
    conn = None
    cursor = None
    
    try:
        # Conecta ao banco de dados principal
        conn = mdb.connect(**DB_CONFIG, database="dbDrogamais")
        cursor = conn.cursor()

        # --- 1. Carregar Tabelas ---
        
        print("Verificando tabela 'bronze_menorPreco_lojas'...")
        sql_lojas = read_sql_file('v1_01_bronze_menorPreco_lojas.sql')
        cursor.execute(sql_lojas)
        
        print("Verificando tabela 'bronze_menorPreco_produtos'...")
        sql_produtos = read_sql_file('v1_02_bronze_menorPreco_produtos.sql')
        cursor.execute(sql_produtos)
        
        print("Verificando tabela 'bronze_menorPreco_notas'...")
        sql_notas = read_sql_file('v1_03_bronze_menorPreco_notas.sql')
        cursor.execute(sql_notas)
        
        print("Verificando tabela 'silver_menorPreco_notas'...")
        sql_silver_notas = read_sql_file('v1_04_silver_menorPreco_notas.sql')
        cursor.execute(sql_silver_notas)

        # --- 2. Carregar Procedure ---
        
        print("Verificando procedure 'proc_atualiza_silver_menorPreco_notas'...")
        sql_proc_raw = read_sql_file('v2_01_proc_atualiza_silver_menorPreco_notas.sql')
        
        # Limpa o SQL da procedure (remove DELIMITERs)
        sql_proc_clean = clean_procedure_sql(sql_proc_raw)
        
        # Executa o 'CREATE PROCEDURE'
        if sql_proc_clean:
            cursor.execute(sql_proc_clean)
        else:
            print("⚠️ AVISO: Não foi possível limpar o SQL da procedure, pulando...")

        conn.commit()
        print("✅ Verificação do esquema do banco (Bronze e Silver) concluída com sucesso!")
    
    except mdb.Error as e:
        print(f"❌ Erro ao inicializar o banco de dados: {e}")
        if conn:
            conn.rollback() # Desfaz qualquer alteração parcial
    except Exception as e:
        # Captura outros erros (ex: FileNotFoundError)
        print(f"❌ Erro inesperado: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    print("Iniciando setup do banco de dados...")
    inicializar_banco()