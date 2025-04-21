#!/usr/bin/env python
# coding: utf-8

"""
Script dedicado a extrair amostras de dados de colunas específicas do banco Firebird,
baseado em um schema técnico previamente extraído. Salva as amostras em um JSON separado.
"""

import os
import sys
import json
import logging
import time
import datetime
import decimal
import re
from collections import defaultdict
import fdb
import concurrent.futures
from dotenv import load_dotenv

# --- Adiciona o diretório raiz ao sys.path --- #
script_dir = os.path.dirname(os.path.abspath(__file__))
# Ajusta para subir dois níveis (scripts/data_preparation -> Novo)
project_root = os.path.dirname(os.path.dirname(script_dir)) 
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM Adição ao sys.path --- #

# Usa helpers e config do projeto
from src.core.logging_config import setup_logging
from src.utils.json_helpers import load_json, save_json
import src.core.config as config # Pode ser usado para caminhos padrão

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Constantes ---
# Arquivo de entrada com a estrutura (para saber o que amostrar)
INPUT_SCHEMA_FILE = config.TECHNICAL_SCHEMA_FILE # Usa config.py -> "data/metadata/technical_schema_from_db.json"
# Arquivo de saída para as amostras
OUTPUT_SAMPLE_JSON_FILE = "data/processed/sample_data.json" # Novo arquivo na pasta processed
SAMPLE_SIZE = config.SAMPLE_SIZE # Usa config.py -> 50
MAX_WORKERS_FOR_SAMPLING = config.MAX_WORKERS_FOR_SAMPLING # Usa config.py -> 50

# --- Funções Auxiliares (Copiadas/Adaptadas de extract_technical_schema.py) ---

def convert_to_json_serializable(value):
    """Converte tipos de dados não serializáveis para JSON."""
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return str(value) # Usar string para preservar precisão decimal
    if isinstance(value, bytes): # Pode vir de BLOBs se buscados ou certos CHARSETS
        try:
            # Tenta decodificar como UTF-8 primeiro, depois fallback para replace
            return value.decode('utf-8')
        except UnicodeDecodeError:
            try:
                # Tenta Latin-1 como fallback comum
                return value.decode('latin-1')
            except UnicodeDecodeError:
                 return value.decode('utf-8', errors='replace') # Último recurso
        except Exception:
            return "<binary_data_error>"
    # Adicionar outros tipos conforme necessário
    return value

def fetch_column_samples_worker(db_params, table_name, column_name, column_type):
    """Busca amostras de dados para uma coluna (THREAD-SAFE). Abre e fecha conexão própria."""
    # Extrai parâmetros de conexão
    db_path, db_user, db_password, db_charset = db_params

    # Não busca amostras para BLOBs/TEXT (redundante se filtrado antes, mas seguro)
    if column_type in ["BLOB", "TEXT"]:
        return table_name, column_name, [] # Retorna identificadores + resultado vazio

    samples = []
    conn = None
    cursor = None
    # Formata a chave para o dicionário de resultados
    result_key = f"{table_name.strip()}:{column_name.strip()}"

    try:
        # Conecta ao DB DENTRO da thread
        conn = fdb.connect(database=db_path, user=db_user, password=db_password, charset=db_charset)
        cursor = conn.cursor()
        # Adiciona aspas duplas aos nomes de tabela/coluna para segurança
        sql = f'SELECT FIRST {SAMPLE_SIZE} DISTINCT "{column_name}" FROM "{table_name}" WHERE "{column_name}" IS NOT NULL'
        cursor.execute(sql)
        results = cursor.fetchall()
        if results:
            samples = [convert_to_json_serializable(row[0]) for row in results]
        else:
            samples = []

    except fdb.OperationalError as e:
        logger.warning(f"[Thread Sample OpError] {result_key}: {e}")
        samples = None # Indica erro
    except fdb.Error as e:
        logger.warning(f"[Thread Sample DBError] {result_key}: {e.sqlcode if hasattr(e, 'sqlcode') else e}")
        samples = None
    except Exception as e:
        logger.warning(f"[Thread Sample OtherError] {result_key}: {e}")
        samples = None
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

    # Retorna a chave e o resultado
    return result_key, samples

# --- Função Principal ---

def main():
    """Função principal para extrair amostras e salvar em JSON."""
    load_dotenv()
    logger.info("--- Iniciando Script: Extração de Amostras de Dados --- ")
    start_time = time.time()

    # 1. Carregar Schema Técnico Base
    logger.info(f"Carregando schema base de {INPUT_SCHEMA_FILE}...")
    try:
        technical_schema = load_json(INPUT_SCHEMA_FILE)
        if not technical_schema:
            logger.error(f"Arquivo de schema base {INPUT_SCHEMA_FILE} está vazio ou não pôde ser carregado.")
            return
    except FileNotFoundError:
        logger.error(f"Arquivo de schema base {INPUT_SCHEMA_FILE} não encontrado. Execute o script de extração de schema primeiro.")
        return
    except Exception as e:
        logger.error(f"Erro ao carregar o arquivo de schema base {INPUT_SCHEMA_FILE}: {e}", exc_info=True)
        return

    # 2. Obter Parâmetros do Banco de Dados
    logger.info("Obtendo parâmetros de conexão do banco de dados...")
    try:
        db_host = os.getenv("FIREBIRD_HOST", "localhost")
        db_port = int(os.getenv("FIREBIRD_PORT", "3050"))
        db_path = os.getenv("FIREBIRD_DB_PATH")
        db_user = os.getenv("FIREBIRD_USER", "SYSDBA")
        db_password = os.getenv("FIREBIRD_PASSWORD")
        db_charset = os.getenv("FIREBIRD_CHARSET", "WIN1252")

        if not db_path or not db_password:
            logger.error("Erro: Variáveis FIREBIRD_DB_PATH ou FIREBIRD_PASSWORD não definidas no .env ou ambiente.")
            return
        db_params = (db_path, db_user, db_password, db_charset)
        logger.info("Parâmetros de conexão obtidos.")
    except Exception as e:
        logger.error(f"Erro ao obter parâmetros de conexão: {e}", exc_info=True)
        return

    # 3. Preparar Tarefas de Amostragem
    logger.info("Preparando tarefas de amostragem...")
    tasks = []
    for object_name, object_data in technical_schema.items():
        # Pular a chave '_analysis' se existir
        if object_name == "_analysis":
            continue
            
        if isinstance(object_data, dict) and "columns" in object_data:
            for col_data in object_data.get("columns", []):
                col_type = col_data.get("type")
                col_name = col_data.get("name")
                if col_type and col_name and col_type not in ["BLOB", "TEXT"]:
                    tasks.append((db_params, object_name.strip(), col_name.strip(), col_type))
        else:
             logger.warning(f"Estrutura inesperada para o objeto '{object_name}' no schema base. Pulando.")


    total_tasks = len(tasks)
    if total_tasks == 0:
        logger.warning("Nenhuma coluna válida encontrada para amostragem no schema base.")
        return
        
    logger.info(f"Total de {total_tasks} colunas para buscar amostras.")

    # 4. Executar Amostragem em Paralelo
    logger.info(f"Iniciando busca PARALELA por amostras ({MAX_WORKERS_FOR_SAMPLING} workers)... (Isso pode levar tempo)")
    start_sample_time = time.time()
    processed_count = 0
    sample_results = {} # Dicionário para armazenar resultados: {"TABLE:COLUMN": [samples]}
    error_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_FOR_SAMPLING) as executor:
        future_to_key = {executor.submit(fetch_column_samples_worker, *task): f"{task[1]}:{task[2]}" for task in tasks}

        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            try:
                result_key, samples = future.result()
                # Verifica se houve erro na thread (samples == None)
                if samples is not None:
                    sample_results[result_key] = samples
                else:
                    error_count += 1
                    sample_results[result_key] = None # Marca erro explicitamente
            except Exception as exc:
                logger.error(f'[Thread Pool Error] Task para {key} gerou exceção: {exc}', exc_info=True)
                error_count += 1
                sample_results[key] = None # Marca erro

            processed_count += 1
            if processed_count % 100 == 0 or processed_count == total_tasks:
                 elapsed = time.time() - start_sample_time
                 logger.info(f"  ... {processed_count}/{total_tasks} amostras processadas ({elapsed:.1f}s). Erros até agora: {error_count}")

    end_sample_time = time.time()
    successful_samples = total_tasks - error_count
    logger.info(f"Busca paralela por amostras concluída em {end_sample_time - start_sample_time:.2f}s.")
    logger.info(f"Amostras obtidas com sucesso para {successful_samples} colunas. Falhas/Erros em {error_count} colunas.")

    # 5. Salvar Resultados
    logger.info(f"Salvando amostras coletadas em {OUTPUT_SAMPLE_JSON_FILE}...")
    try:
        save_json(sample_results, OUTPUT_SAMPLE_JSON_FILE)
        logger.info("Amostras salvas com sucesso.")
    except Exception as e:
         logger.error(f"Erro ao salvar as amostras no arquivo JSON '{OUTPUT_SAMPLE_JSON_FILE}': {e}", exc_info=True)

    end_time = time.time()
    logger.info(f"--- Script Concluído em {end_time - start_time:.2f}s --- ")

if __name__ == "__main__":
    main() 