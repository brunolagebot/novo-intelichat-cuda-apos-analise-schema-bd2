import fdb
import json
import os
import logging
import datetime
import time
import streamlit as st
import argparse

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constantes de Arquivo e Conexão ---
TECHNICAL_SCHEMA_FILE = 'data/combined_schema_details.json' # Usar o combinado para garantir que temos object_type
OUTPUT_COUNTS_FILE = 'data/overview_counts.json'

# --- Funções Auxiliares ---
def load_technical_schema(file_path):
    """Carrega o schema técnico do arquivo JSON."""
    if not os.path.exists(file_path):
        logger.error(f"Erro: Arquivo de schema técnico não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Erro ao carregar schema técnico {file_path}: {e}", exc_info=True)
        return None

def fetch_row_count(conn, table_name):
    """Busca a contagem de linhas usando uma conexão existente."""
    cur = None
    try:
        cur = conn.cursor()
        sql = f'SELECT COUNT(*) FROM "{table_name}"' # Aspas duplas para nomes de tabela
        cur.execute(sql)
        count = cur.fetchone()[0]
        cur.close()
        logger.debug(f"Contagem para {table_name}: {count}")
        return count
    except fdb.Error as e:
        # Log detalhado do erro do banco
        error_code = e.sqlcode if hasattr(e, 'sqlcode') else 'N/A'
        error_msg = e.fb_message if hasattr(e, 'fb_message') else str(e)
        logger.error(f"Erro Firebird ao contar {table_name} (Code: {error_code}): {error_msg}")
        # Fecha o cursor se ainda estiver aberto após erro
        if cur:
             try: 
                 cur.close()
                 logger.debug(f"Cursor para {table_name} fechado após erro DB.")
             except Exception as close_err:
                 logger.warning(f"Erro ao tentar fechar cursor para {table_name} após erro DB: {close_err}")
        return f"Erro DB: {error_code}" # Retorna só o código para simplificar
    except Exception as e:
        logger.exception(f"Erro inesperado ao contar {table_name}:")
        if cur:
             try: 
                 cur.close()
                 logger.debug(f"Cursor para {table_name} fechado após erro App.")
             except Exception as close_err:
                 logger.warning(f"Erro ao tentar fechar cursor para {table_name} após erro App: {close_err}")
        return "Erro App"
    # Não fecha a conexão aqui, ela é gerenciada externamente

def save_counts(counts_data, file_path):
    """Salva o dicionário de contagens no arquivo JSON."""
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(counts_data, f, indent=4)
        logger.info(f"Contagens salvas com sucesso em {file_path}")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo de contagens {file_path}: {e}", exc_info=True)
        return False

# --- Execução Principal ---
if __name__ == "__main__":
    # --- NOVO: Parseamento de Argumentos de Linha de Comando ---
    parser = argparse.ArgumentParser(description="Calcula contagem de linhas para tabelas/views e salva em JSON.")
    parser.add_argument("--db-path", required=True, help="Caminho para o arquivo do banco de dados Firebird (.fdb)")
    parser.add_argument("--db-user", required=True, help="Usuário do banco de dados Firebird")
    parser.add_argument("--db-password", required=True, help="Senha do banco de dados Firebird")
    parser.add_argument("--db-charset", default="WIN1252", help="Charset de conexão do Firebird (padrão: WIN1252)")
    args = parser.parse_args()

    # Usa os argumentos parseados
    DB_PATH = args.db_path
    DB_USER = args.db_user
    DB_PASSWORD = args.db_password
    DB_CHARSET = args.db_charset
    # --- FIM: Parseamento ---

    logger.info(f"Iniciando cálculo de contagem de linhas. Schema: {TECHNICAL_SCHEMA_FILE}")
    start_time = time.time()

    schema = load_technical_schema(TECHNICAL_SCHEMA_FILE)
    if not schema:
        logger.error("Não foi possível carregar o schema técnico. Abortando.")
        exit(1)

    # Validar conexão antes de começar
    conn_main = None
    try:
        logger.info(f"Conectando ao banco de dados: {DB_PATH}")
        conn_main = fdb.connect(
            dsn=DB_PATH,
            user=DB_USER,
            password=DB_PASSWORD,
            charset=DB_CHARSET
        )
        logger.info("Conexão com o banco estabelecida.")
    except fdb.Error as e:
        logger.error(f"Falha ao conectar ao banco de dados: {e}", exc_info=True)
        print(f"ERRO: Não foi possível conectar ao banco: {e.fb_message if hasattr(e, 'fb_message') else e}")
        exit(1)
    except Exception as e:
        logger.error(f"Erro inesperado na conexão: {e}", exc_info=True)
        print(f"ERRO: Inesperado na conexão: {e}")
        exit(1)

    # Dicionário para armazenar resultados
    overview_counts = {}
    objects_to_count = []

    # Filtrar apenas TABLES e VIEWS
    for name, data in schema.items():
        if data.get('object_type') in ["TABLE", "VIEW"]:
            objects_to_count.append(name)
    
    total_objects = len(objects_to_count)
    logger.info(f"Total de {total_objects} tabelas/views a serem contadas.")
    processed_count = 0
    error_count = 0

    # Loop principal de contagem
    for object_name in objects_to_count:
        processed_count += 1
        progress_message = f"PROGRESS:{processed_count}/{total_objects}:{object_name}"
        print(progress_message, flush=True) # Envia imediatamente para stdout
        logger.debug(f"Reportando progresso: {progress_message}") # Log para depuração
        
        count_result = fetch_row_count(conn_main, object_name)
        current_timestamp = datetime.datetime.now().isoformat()
        
        overview_counts[object_name] = {
            "count": count_result,
            "timestamp": current_timestamp
        }
        
        if isinstance(count_result, str) and count_result.startswith("Erro"):
            error_count += 1
            logger.warning(f"Falha ao contar {object_name}: {count_result}")

    # Fechar conexão principal após o loop
    if conn_main and not conn_main.closed:
        try: 
            conn_main.close()
            logger.info("Conexão principal com o banco fechada.")
        except Exception as e:
            logger.error(f"Erro ao fechar conexão principal: {e}")

    # Salvar resultados
    if save_counts(overview_counts, OUTPUT_COUNTS_FILE):
        logger.info(f"Contagens salvas em {OUTPUT_COUNTS_FILE}") # Log em vez de print
    else:
        logger.error(f"ERRO ao salvar contagens em {OUTPUT_COUNTS_FILE}") # Log em vez de print

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"Processo de contagem concluído em {duration:.2f} segundos.")
    logger.info(f"Total processado: {processed_count}. Erros: {error_count}.")
    # Mensagem final para stdout, indicando conclusão
    print(f"DONE:{processed_count}/{error_count}", flush=True) 