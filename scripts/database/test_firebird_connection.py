import fdb
import logging
import sys
import os
from dotenv import load_dotenv

# Adicionado: Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configurações da Conexão Firebird (Lidas do Ambiente) ---
FIREBIRD_HOST = os.getenv("FIREBIRD_HOST", "localhost")
FIREBIRD_PORT = int(os.getenv("FIREBIRD_PORT", "3050"))
FIREBIRD_DB_PATH = os.getenv("FIREBIRD_DB_PATH")
FIREBIRD_USER = os.getenv("FIREBIRD_USER", "SYSDBA")
FIREBIRD_PASSWORD = os.getenv("FIREBIRD_PASSWORD")
FIREBIRD_CHARSET = os.getenv("FIREBIRD_CHARSET", "WIN1252")

# Verifica se as variáveis essenciais foram carregadas
if not FIREBIRD_DB_PATH or not FIREBIRD_PASSWORD:
    logging.error("Erro: Variáveis FIREBIRD_DB_PATH ou FIREBIRD_PASSWORD não definidas no .env ou ambiente.")
    sys.exit(1)

def test_connection():
    """Tenta conectar ao banco de dados Firebird e executa uma query simples."""
    conn = None
    try:
        logging.info(f"Tentando conectar a {FIREBIRD_HOST}:{FIREBIRD_DB_PATH}...")
        conn = fdb.connect(
            host=FIREBIRD_HOST,
            port=FIREBIRD_PORT,
            database=FIREBIRD_DB_PATH,
            user=FIREBIRD_USER,
            password=FIREBIRD_PASSWORD,
            charset=FIREBIRD_CHARSET
        )
        logging.info("Conexão com Firebird estabelecida com sucesso!")

        # Opcional: Executar uma query de teste simples
        # (Exemplo: selecionando a data/hora atual do servidor Firebird)
        cur = conn.cursor()
        # Query simplificada para buscar nome de uma tabela de usuário
        test_query = "SELECT rdb$relation_name FROM rdb$relations WHERE rdb$system_flag = 0 FETCH FIRST 1 ROW ONLY;"
        logging.info(f"Executando query de teste: {test_query}")
        cur.execute(test_query)
        result = cur.fetchone()
        if result:
            logging.info(f"Resultado da query de teste (nome da tabela): {result[0]}")
        else:
            logging.info("Nenhuma tabela de usuário encontrada (banco vazio?) ou erro na query.")
        cur.close()

        return True

    except fdb.Error as e:
        logging.error(f"Erro ao conectar ou interagir com o Firebird: {e}", exc_info=True)
        # Você pode querer verificar códigos de erro específicos aqui se necessário
        # ex: if e.args[0] == '...some error code...':
        return False

    finally:
        if conn:
            logging.info("Fechando conexão com Firebird.")
            conn.close()

if __name__ == "__main__":
    if test_connection():
        print("\nTeste de conexão com Firebird bem-sucedido.")
        sys.exit(0)
    else:
        print("\nFalha no teste de conexão com Firebird. Verifique os logs e as configurações.")
        sys.exit(1) 