# init_db.py
import mariadb as mdb
from config import DB_CONFIG

def inicializar_banco():
    """
    Conecta ao banco de dados e garante que as tabelas de 
    destino do Menor Preço existam.
    """
    try:
        # Conecta ao banco de dados principal
        conn = mdb.connect(**DB_CONFIG, database="dbDrogamais")
        cursor = conn.cursor()
        
        print("Verificando tabela 'bronze_menorPreco_lojas'...")
        cursor.execute(v1_01_bronze_menorPreco_lojas.sql)
        
        print("Verificando tabela 'bronze_menorPreco_produtos'...")
        cursor.execute(v1_02_bronze_menorPreco_produtos.sql)
        
        print("Verificando tabela 'bronze_menorPreco_notas'...")
        cursor.execute(v1_03_bronze_menorPreco_notas.sql)
        
        print("Verificando tabela 'silver_menorPreco_notas'...")
        cursor.execute(v1_04_silver_menorPreco_notas.sql)

        print("Verificando tabela 'proc_atualiza_silver_menorPreco_notas.sql'...")
        cursor.execute(proc_atualiza_silver_menorPreco_notas.sql)
        
        conn.commit()
        print("✅ Verificação do esquema do banco (Bronze e Silver) concluída com sucesso!")
    
    except mdb.Error as e:
        print(f"❌ Erro ao inicializar o banco de dados: {e}")
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    print("Iniciando setup do banco de dados...")
    inicializar_banco()