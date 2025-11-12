# init_db.py
import mariadb as mdb
from config import DB_CONFIG
from migrations.v1_create_all_tables import SQL_CREATE_LOJAS, SQL_CREATE_PRODUTOS, SQL_CREATE_NOTAS, SQL_CREATE_SILVER_NOTAS

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
        cursor.execute(SQL_CREATE_LOJAS)
        
        print("Verificando tabela 'bronze_menorPreco_produtos'...")
        cursor.execute(SQL_CREATE_PRODUTOS)
        
        print("Verificando tabela 'bronze_menorPreco_notas'...")
        cursor.execute(SQL_CREATE_NOTAS)
        
        print("Verificando tabela 'silver_menorPreco_notas'...")
        cursor.execute(SQL_CREATE_SILVER_NOTAS)

        print("Verificando procedure 'proc_atualiza_silver_menorPreco_notas'...")
        cursor.execute(migrations.v2_atualizar_silver_notas.sql)
        
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