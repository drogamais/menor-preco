# executar_silver.py
import mariadb
import sys
import os
import time

# Encontra o arquivo config.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, PROJECT_ROOT)
try:
    from config import DB_CONFIG
except ImportError:
    print("❌ ERRO: 'config.py' não encontrado na pasta raiz do projeto.")
    sys.exit(1)

def rodar_procedure_silver():
    """
    Conecta ao banco e executa a procedure
    'proc_atualiza_silver_menorPreco_notas'.
    """
    conn = None
    cursor = None
    procedure_name = "proc_atualiza_silver_menorPreco_notas"

    try:
        # 1. Conectar ao banco
        print("Conectando ao banco de dados 'dbDrogamais'...")
        conn = mariadb.connect(**DB_CONFIG, database="dbDrogamais")
        # Aumenta o timeout padrão de escrita, pois a procedure pode demorar
        conn.write_timeout = 300 # 5 minutos
        
        cursor = conn.cursor()

        # 2. Chamar a procedure
        print(f"Executando 'CALL {procedure_name}()'...")
        print("Isso pode demorar vários minutos. Por favor, aguarde...")
        
        start_time = time.time()
        
        # cursor.call() é o método correto para chamar procedures
        cursor.execute(f"CALL {procedure_name}()")
        
        print("Procedure executada. Realizando commit das alterações...")

        # 3. Commit
        # Essencial para salvar as alterações feitas pela procedure (TRUNCATE/INSERT)
        conn.commit()
        
        end_time = time.time()

        print("\n" + "="*50)
        print(f"✅ SUCESSO! Procedure '{procedure_name}' concluída.")
        print(f"   Tempo total de execução: {end_time - start_time:.2f} segundos.")
        print("="*50)

    except mariadb.Error as e:
        print(f"❌ Erro de Banco de Dados: {e}")
        if conn:
            print("Executando rollback...")
            conn.rollback() # Desfaz alterações em caso de erro
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")
    finally:
        # 4. Fechar tudo
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Conexão com o banco fechada.")

# --- Ponto de entrada do script ---
if __name__ == "__main__":
    rodar_procedure_silver()