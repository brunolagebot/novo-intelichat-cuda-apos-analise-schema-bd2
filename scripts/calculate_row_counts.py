import json
import os
import sys
import logging
import time
import argparse
import fdb
from datetime import datetime

# Adiciona o diretório raiz ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importações do projeto
from src.core.logging_config import setup_logging
from src.core.config import OVERVIEW_COUNTS_FILE, DEFAULT_DB_PATH, DEFAULT_DB_USER, DEFAULT_DB_CHARSET

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

def connect_to_db(db_path, user, password, charset):
    """Estabelece conexão com o banco de dados Firebird."""
    try:
        conn = fdb.connect(
            database=db_path,
            user=user,
            password=password,
            charset=charset
        )
        logger.info("Conexão com o banco estabelecida.")
        return conn
    except fdb.Error as e:
        logger.error(f"Erro ao conectar ao banco: {e}")
        return None

def get_database_schema(conn):
    """Obtém o schema do banco de dados (tabelas e views)."""
    schema_objects = []
    cur = None
    try:
        cur = conn.cursor()
        
        # Consulta para obter todas as tabelas
        tables_sql = """
        SELECT rdb$relation_name as name
        FROM rdb$relations
        WHERE rdb$view_blr IS NULL
        AND rdb$system_flag = 0
        ORDER BY rdb$relation_name
        """
        cur.execute(tables_sql)
        tables = cur.fetchall()
        for table in tables:
            table_name = table[0].strip()  # Remove espaços em branco
            schema_objects.append({
                "name": table_name,
                "type": "table"
            })
        
        # Consulta para obter todas as views
        views_sql = """
        SELECT rdb$relation_name as name
        FROM rdb$relations
        WHERE rdb$view_blr IS NOT NULL
        AND rdb$system_flag = 0
        ORDER BY rdb$relation_name
        """
        cur.execute(views_sql)
        views = cur.fetchall()
        for view in views:
            view_name = view[0].strip()  # Remove espaços em branco
            schema_objects.append({
                "name": view_name,
                "type": "view"
            })
        
        logger.info(f"Schema obtido com sucesso: {len(schema_objects)} objetos encontrados.")
        return schema_objects
    except fdb.Error as e:
        error_code = e.sqlcode if hasattr(e, 'sqlcode') else 'N/A'
        error_msg = e.fb_message if hasattr(e, 'fb_message') else str(e)
        logger.error(f"Erro ao obter schema (Code: {error_code}): {error_msg}")
        return []
    except Exception as e:
        logger.exception(f"Erro inesperado ao obter schema:")
        return []
    finally:
        if cur:
            try:
                cur.close()
            except Exception as close_err:
                logger.warning(f"Erro ao tentar fechar cursor: {close_err}")

def fetch_row_count(conn, table_name):
    """Busca a contagem de linhas usando uma conexão existente."""
    cur = None
    try:
        cur = conn.cursor()
        sql = f'SELECT COUNT(*) FROM "{table_name}"'  # Aspas duplas para nomes de tabela
        cur.execute(sql)
        count = cur.fetchone()[0]
        cur.close()
        logger.debug(f"Contagem para {table_name}: {count}")
        return count
    except fdb.Error as e:
        error_code = e.sqlcode if hasattr(e, 'sqlcode') else 'N/A'
        error_msg = e.fb_message if hasattr(e, 'fb_message') else str(e)
        logger.error(f"Erro Firebird ao contar {table_name} (Code: {error_code}): {error_msg}")
        if cur:
            try:
                cur.close()
            except Exception as close_err:
                logger.warning(f"Erro ao tentar fechar cursor para {table_name}: {close_err}")
        return None
    except Exception as e:
        logger.exception(f"Erro inesperado ao contar {table_name}:")
        if cur:
            try:
                cur.close()
            except Exception as close_err:
                logger.warning(f"Erro ao tentar fechar cursor para {table_name}: {close_err}")
        return None

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

def main(args):
    start_time = time.time()
    logger.info("Iniciando cálculo de contagem de linhas...")

    # Conectar ao banco
    db_password = os.getenv('FIREBIRD_PASSWORD') or args.db_password
    if not db_password:
        logger.critical("Senha do banco não fornecida via argumento --db_password ou variável de ambiente FIREBIRD_PASSWORD.")
        return

    logger.info(f"Conectando ao banco de dados: {args.db_path}")
    conn = connect_to_db(args.db_path, args.db_user, db_password, args.db_charset)
    if not conn:
        logger.error("Não foi possível estabelecer conexão com o banco.")
        return

    try:
        # Gerar schema do banco de dados
        logger.info("Obtendo schema diretamente do banco de dados...")
        schema_objects = get_database_schema(conn)
        if not schema_objects:
            logger.error("Não foi possível obter o schema do banco de dados.")
            return
        
        # Criar objeto de schema completo
        schema = {
            "schema_objects": schema_objects,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "database": args.db_path
        }
        
        # Salvar schema gerado se solicitado
        if args.save_schema:
            try:
                schema_file_path = args.save_schema
                os.makedirs(os.path.dirname(schema_file_path), exist_ok=True)
                with open(schema_file_path, 'w', encoding='utf-8') as f:
                    json.dump(schema, f, indent=4)
                logger.info(f"Schema salvo com sucesso em {schema_file_path}")
            except Exception as e:
                logger.error(f"Erro ao salvar arquivo de schema {schema_file_path}: {e}", exc_info=True)
        
        row_counts = {}
        total_objects = len(schema_objects)
        processed_count = 0
        progress_interval = max(1, total_objects // 10)

        for obj in schema_objects:
            obj_name = obj.get('name')
            obj_type = obj.get('type', '').lower()
            
            if not obj_name or not obj_type:
                logger.warning(f"Objeto sem nome ou tipo encontrado no schema: {obj}")
                continue

            processed_count += 1
            logger.debug(f"Consultando contagem para {obj_type} '{obj_name}'")
            
            count = fetch_row_count(conn, obj_name)
            if count is not None:
                row_counts[f"{obj_type}:{obj_name}"] = count
            else:
                logger.warning(f"Não foi possível obter a contagem para {obj_type} '{obj_name}'")

            if processed_count % progress_interval == 0 or processed_count == total_objects:
                logger.info(f"Progresso: {processed_count}/{total_objects} objetos consultados ({processed_count/total_objects*100:.1f}%)...")

        # Adicionar timestamp à saída
        output_data = {
            "counts": row_counts,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        # Salvar contagens
        logger.info(f"Salvando contagens em {args.output_file}")
        if save_counts(output_data, args.output_file):
            logger.info("Contagens salvas com sucesso.")
        else:
            logger.error("Falha ao salvar contagens.")

    except Exception as e:
        logger.critical(f"Erro durante a execução: {e}", exc_info=True)
    finally:
        if conn:
            try:
                conn.close()
                logger.info("Conexão com o banco fechada.")
            except Exception as e:
                logger.error(f"Erro ao fechar conexão com o banco: {e}")

    end_time = time.time()
    logger.info(f"Cálculo de contagem de linhas concluído em {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calcula e salva a contagem de linhas para tabelas e views do schema, obtendo o schema diretamente do banco de dados.")

    # Argumentos para caminhos
    parser.add_argument("--output_file", default=OVERVIEW_COUNTS_FILE, help="Caminho para salvar o arquivo JSON de contagens.")
    parser.add_argument("--save_schema", default=None, help="Se especificado, salva o schema gerado no caminho fornecido.")

    # Argumentos para conexão com banco (padrões de config.py)
    parser.add_argument("--db_path", default=DEFAULT_DB_PATH, help="Caminho para o arquivo do banco Firebird (.fdb).")
    parser.add_argument("--db_user", default=DEFAULT_DB_USER, help="Usuário do banco de dados.")
    parser.add_argument("--db_password", default=None, help="Senha do banco (ou usar variável FIREBIRD_PASSWORD).")
    parser.add_argument("--db_charset", default=DEFAULT_DB_CHARSET, help="Charset da conexão.")

    args = parser.parse_args()
    main(args) 