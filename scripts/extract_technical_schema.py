#!/usr/bin/env python
# coding: utf-8

"""
Script unificado para extrair o schema técnico completo do banco de dados Firebird,
incluindo estrutura, constraints, defaults e amostras de dados, salvando em JSON.
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
import streamlit as st
import concurrent.futures # << NOVO

# --- Adiciona o diretório raiz ao sys.path --- #
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM Adição ao sys.path --- #

import src.core.config as config
from src.core.logging_config import setup_logging
from src.utils.json_helpers import save_json # Usaremos para salvar o resultado

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Constantes ---
OUTPUT_JSON_FILE = "data/processed/technical_schema_from_db.json" # Nome do arquivo de saída
SAMPLE_SIZE = 50 # Número de amostras distintas a buscar por coluna
MAX_WORKERS_FOR_SAMPLING = 10 # << NOVO: Número de threads para busca de amostras

# --- Mapeamento de Tipos Firebird (Simplificado) ---
# Baseado em https://firebirdsql.org/refdocs/langrefupd25-system-tables-rdb-fields.html#langrefupd25-systables-rdb-flds-type
# Isso pode precisar de ajustes finos dependendo da versão e tipos exatos usados
FB_TYPE_MAP = {
    7: "SMALLINT",
    8: "INTEGER",
    10: "FLOAT",
    11: "DOUBLE PRECISION", # D_FLOAT - obsoleto?
    12: "DATE",
    13: "TIME",
    14: "CHAR",
    16: "BIGINT", # DIALECT 3
    27: "DOUBLE PRECISION",
    35: "TIMESTAMP",
    37: "VARCHAR",
    261: "BLOB",
    # Adicionar outros tipos se necessário (ex: NUMERIC/DECIMAL mapeados de 7, 8, 16 com scale > 0)
}

# --- Consultas SQL para Metadados --- 
# Query mais completa para infos básicas + constraints + descriptions
SQL_GET_BASE_SCHEMA_INFO = """
    SELECT
        rf.RDB$RELATION_NAME        AS TABLE_OR_VIEW_NAME,
        r.RDB$RELATION_TYPE         AS OBJECT_TYPE_CODE, -- 0=Table, 1=View
        r.RDB$DESCRIPTION           AS OBJECT_DESCRIPTION, -- << NOVO
        rf.RDB$FIELD_NAME           AS COLUMN_NAME,
        rf.RDB$DESCRIPTION          AS COLUMN_DESCRIPTION, -- << NOVO
        f.RDB$FIELD_TYPE            AS FIELD_TYPE_CODE,
        f.RDB$FIELD_SUB_TYPE        AS FIELD_SUB_TYPE,
        f.RDB$FIELD_LENGTH          AS FIELD_LENGTH,
        f.RDB$FIELD_PRECISION       AS FIELD_PRECISION,
        f.RDB$FIELD_SCALE           AS FIELD_SCALE,
        rf.RDB$NULL_FLAG            AS NOT_NULL_FLAG, -- 1 = NOT NULL
        rf.RDB$DEFAULT_SOURCE       AS DEFAULT_SOURCE -- Ex: 'DEFAULT 0', \"DEFAULT 'A'\", NULL
    FROM RDB$RELATION_FIELDS rf
    JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
    JOIN RDB$RELATIONS r ON rf.RDB$RELATION_NAME = r.RDB$RELATION_NAME
    WHERE r.RDB$SYSTEM_FLAG = 0 -- Exclui tabelas/views do sistema
      -- REMOVIDO: AND r.RDB$VIEW_BLR IS NULL -- Inclui tabelas E views agora
      AND rf.RDB$SYSTEM_FLAG = 0
    ORDER BY rf.RDB$RELATION_NAME, rf.RDB$FIELD_POSITION;
"""

# Queries de PK/FK (mantidas do script anterior)
SQL_GET_PKS = """
    SELECT
        rc.RDB$RELATION_NAME AS TABLE_NAME,
        isc.RDB$FIELD_NAME AS COLUMN_NAME
    FROM RDB$RELATION_CONSTRAINTS rc
    JOIN RDB$INDEX_SEGMENTS isc ON rc.RDB$INDEX_NAME = isc.RDB$INDEX_NAME
    WHERE rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
    ORDER BY rc.RDB$RELATION_NAME, isc.RDB$FIELD_POSITION;
"""

SQL_GET_FKS = """
    SELECT
        rc.RDB$RELATION_NAME AS FK_TABLE,
        isc.RDB$FIELD_NAME AS FK_COLUMN,
        rc_pk.RDB$RELATION_NAME AS PK_TABLE,
        isc_pk.RDB$FIELD_NAME AS PK_COLUMN,
        rc.RDB$CONSTRAINT_NAME AS FK_NAME -- Adicionado para referência
    FROM RDB$RELATION_CONSTRAINTS rc
    JOIN RDB$INDEX_SEGMENTS isc ON rc.RDB$INDEX_NAME = isc.RDB$INDEX_NAME
    JOIN RDB$REF_CONSTRAINTS refc ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
    JOIN RDB$RELATION_CONSTRAINTS rc_pk ON refc.RDB$CONST_NAME_UQ = rc_pk.RDB$CONSTRAINT_NAME
    JOIN RDB$INDEX_SEGMENTS isc_pk ON rc_pk.RDB$INDEX_NAME = isc_pk.RDB$INDEX_NAME AND isc.RDB$FIELD_POSITION = isc_pk.RDB$FIELD_POSITION
    WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
    ORDER BY rc.RDB$RELATION_NAME, rc.RDB$CONSTRAINT_NAME, isc.RDB$FIELD_POSITION;
"""

# --- Helper Functions ---

def format_key(table_name, column_name):
    """Formata a chave de coluna consistentemente como string."""
    # Garante que não haja espaços extras que causem problemas
    return f"{table_name.strip()}:{column_name.strip()}"

def parse_default_value(default_source):
    """Tenta extrair o valor default da string RDB$DEFAULT_SOURCE."""
    if default_source is None:
        return None
    default_source = default_source.strip().upper()
    if default_source.startswith("DEFAULT "):
        val_str = default_source[len("DEFAULT "):].strip()
        # Tenta identificar tipos comuns
        if val_str == 'NULL':
            return None
        if val_str.startswith("'") and val_str.endswith("'"):
            return val_str[1:-1] # Remove aspas simples
        if val_str.startswith("\"") and val_str.endswith("\""):
            return val_str[1:-1] # Remove aspas duplas (menos comum)
        if re.match(r'^-?\d+$', val_str): # Inteiro
            try: return int(val_str)
            except ValueError: pass
        if re.match(r'^-?\d*\.\d+$', val_str): # Decimal/Float
            try: return float(val_str) # Ou decimal.Decimal(val_str)
            except ValueError: pass
        # Outros casos (ex: CURRENT_TIMESTAMP, etc.) podem precisar de tratamento
        return val_str # Retorna a string original se não reconhecer
    return None # Não começa com 'DEFAULT '

def map_fb_type(type_code, sub_type, scale):
    """Mapeia códigos de tipo Firebird para nomes legíveis."""
    if type_code == 261: # BLOB
        # Poderia detalhar subtipo (0=Segmented, 1=Text, etc.) se necessário
        return "BLOB"
    if type_code in (7, 8, 16) and scale is not None and scale < 0: 
        # Escala negativa indica NUMERIC/DECIMAL
        # A precisão está em FIELD_PRECISION (não incluído aqui ainda)
        return "NUMERIC" # Ou DECIMAL
    return FB_TYPE_MAP.get(type_code, f"UNKNOWN_TYPE_{type_code}")

def convert_to_json_serializable(value):
    """Converte tipos de dados não serializáveis para JSON."""
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value) # Ou str(value) para precisão exata
    if isinstance(value, bytes): # Pode vir de BLOBs se buscados ou certos CHARSETS
        try:
            return value.decode('utf-8', errors='replace') # Tenta decodificar como texto
        except: 
            return "<binary_data>"
    # Adicionar outros tipos conforme necessário
    return value

def fetch_column_samples(db_params, table_name, column_name, column_type):
    """Busca amostras de dados para uma coluna (THREAD-SAFE). Abre e fecha conexão própria."""
    # Extrai parâmetros de conexão
    db_path, db_user, db_password, db_charset = db_params
    
    # Não busca amostras para BLOBs
    if column_type == "BLOB":
        # logger.debug não é thread-safe por padrão, evitar logging intensivo dentro da thread
        # print(f"DEBUG [Thread]: Pulando BLOB {table_name}.{column_name}") # Usar print para debug rápido em threads
        return table_name, column_name, [] # Retorna identificadores + resultado
        
    # print(f"DEBUG [Thread]: Buscando amostra para {table_name}.{column_name}")
    samples = []
    conn = None
    cursor = None
    try:
        # Conecta ao DB DENTRO da thread
        conn = fdb.connect(dsn=db_path, user=db_user, password=db_password, charset=db_charset)
        cursor = conn.cursor()
        sql = f'SELECT FIRST {SAMPLE_SIZE} DISTINCT "{column_name}" FROM "{table_name}" WHERE "{column_name}" IS NOT NULL'
        cursor.execute(sql)
        results = cursor.fetchall()
        if results:
            samples = [convert_to_json_serializable(row[0]) for row in results]
        else:
            samples = []
            
    except fdb.Error as e:
        # Logar warnings ainda é útil, mas ser conciso
        logger.warning(f"[Thread Sample Error] DB Error {table_name}.{column_name}: {e.sqlcode if hasattr(e, 'sqlcode') else e}")
        samples = None 
    except Exception as e:
        logger.warning(f"[Thread Sample Error] Other Error {table_name}.{column_name}: {e}")
        samples = None
    finally:
        if cursor: 
            try: cursor.close()
            except: pass
        if conn: 
            try: conn.close()
            except: pass
            
    # Retorna identificadores para facilitar a correspondência no final
    return table_name, column_name, samples

def extract_full_schema_from_db(conn, db_params):
    """Extrai o schema técnico completo, incluindo amostras (paralelizadas) e descrições, do DB."""
    # Recebe db_params para passar para as threads
    logger.info("Iniciando extração completa do schema técnico (paralelizando amostras)...")
    # Agora a chave principal é o nome da tabela/view
    schema_details = defaultdict(lambda: {"columns": [], "object_type": None, "description": None}) # Adiciona description no nível do objeto
    analysis_results = {
        "composite_pk_tables": set(),
        "junction_tables": set(),
        "fk_definitions": {}, # Usará assinatura como chave
        "column_roles": defaultdict(set) # tabela:coluna -> {PK, FK}
    }
    pk_columns = set() # tabela:coluna
    pks_by_table = defaultdict(list)
    fk_columns = set() # tabela:coluna
    fks_references = defaultdict(list) # fk_col_key -> [{pk_table, pk_col}, ...]
    fks_by_constraint = defaultdict(lambda: {"fk_cols": [], "pk_table": None, "pk_cols": []})
    cursor = None

    try:
        cursor = conn.cursor()

        # 1. Obter informações básicas de tabelas/views, colunas e descrições
        logger.info("Extraindo informações básicas (incluindo descrições)...")
        cursor.execute(SQL_GET_BASE_SCHEMA_INFO)
        basic_info = cursor.fetchall()
        logger.info(f"Encontradas {len(basic_info)} definições de colunas em tabelas/views.")

        for row in basic_info:
            # Desempacotar as novas colunas OBJECT_DESCRIPTION e COLUMN_DESCRIPTION
            (obj_name, obj_type_code, obj_desc, 
             c_name, c_desc, 
             c_type, c_subtype, c_len, c_prec, c_scale, 
             not_null_flag, def_src) = row
             
            object_name = obj_name.strip()
            column_name = c_name.strip()
            object_type = "VIEW" if obj_type_code == 1 else "TABLE"
            # Assume que fdb decodifica BLOB TEXT corretamente com base no charset
            object_description = obj_desc # Pode ser None
            column_description = c_desc   # Pode ser None
            column_type = map_fb_type(c_type, c_subtype, c_scale)
            nullable = not bool(not_null_flag) 
            default_value = parse_default_value(def_src) 
            
            # Armazena a descrição do objeto (apenas uma vez por objeto)
            if schema_details[object_name]["description"] is None and object_description is not None:
                 schema_details[object_name]["description"] = object_description
            # Atribui o tipo do objeto
            schema_details[object_name]["object_type"] = object_type

            col_data = {
                "name": column_name,
                "type": column_type,
                "nullable": nullable,
                "default_value": default_value,
                "description": column_description, # Armazena descrição da coluna
                "is_pk": False, # Será atualizado depois
                "is_fk": False, # Será atualizado depois
                "fk_references": None, # Será atualizado depois
                "sample_values": None, # Será preenchido depois
                # Campos para compatibilidade/enriquecimento futuro (inicializados)
                "business_description": None,
                "value_mapping_notes": None,
                "ai_generated_description": None,
                "ai_model_used": None,
                "ai_generation_timestamp": None,
                "text_for_embedding": None, # Será gerado depois, se necessário
            }
            schema_details[object_name]["columns"].append(col_data)

        # 2. Obter e processar PKs (Só se aplica a TABELAS)
        logger.info("Processando Chaves Primárias (PKs) - Apenas para Tabelas...")
        cursor.execute(SQL_GET_PKS)
        raw_pks = cursor.fetchall()
        for row in raw_pks:
            table_name, column_name = map(str.strip, row)
            col_key = format_key(table_name, column_name)
            pk_columns.add(col_key)
            analysis_results["column_roles"][col_key].add("PK")
            pks_by_table[table_name].append(column_name)
        # Marcar tabelas com PKs compostas
        for table, columns in pks_by_table.items():
            if len(columns) > 1:
                analysis_results["composite_pk_tables"].add(table)

        # 3. Obter e processar FKs (Só se aplica a TABELAS)
        logger.info("Processando Chaves Estrangeiras (FKs) - Apenas para Tabelas...")
        cursor.execute(SQL_GET_FKS)
        raw_fks = cursor.fetchall()
        for row in raw_fks:
            fk_table, fk_column, pk_table, pk_column, fk_name = map(str.strip, row)
            fk_col_key = format_key(fk_table, fk_column)
            pk_col_key = format_key(pk_table, pk_column)
            fk_columns.add(fk_col_key)
            analysis_results["column_roles"][fk_col_key].add("FK")
            fks_references[fk_col_key].append({"references_table": pk_table, "references_column": pk_column})
            
            # Agrupar por constraint para análise de FKs compostas e definição
            constraint_key = (fk_table, fk_name)
            fks_by_constraint[constraint_key]["fk_cols"].append(fk_column)
            fks_by_constraint[constraint_key]["pk_table"] = pk_table
            fks_by_constraint[constraint_key]["pk_cols"].append(pk_column)

        # Normalizar e armazenar definições de FK
        for (fk_table, fk_name), details in fks_by_constraint.items():
            fk_cols_sorted = tuple(sorted(details['fk_cols']))
            pk_cols_sorted = tuple(sorted(details['pk_cols']))
            pk_table = details['pk_table']
            fk_signature = f"{fk_table}{fk_cols_sorted} -> {pk_table}{pk_cols_sorted}"
            if fk_signature not in analysis_results["fk_definitions"]:
                 analysis_results["fk_definitions"][fk_signature] = {
                     "fk_table": fk_table,
                     "fk_cols": list(fk_cols_sorted),
                     "pk_table": pk_table,
                     "pk_cols": list(pk_cols_sorted),
                     "constraint_names": [fk_name]
                 }
            else:
                 if fk_name not in analysis_results["fk_definitions"][fk_signature]["constraint_names"]:
                      analysis_results["fk_definitions"][fk_signature]["constraint_names"].append(fk_name)
                      
        # 4. Identificar Tabelas de Junção (Só se aplica a TABELAS)
        logger.info("Identificando tabelas de junção - Apenas para Tabelas...")
        for table_name in pks_by_table:
             pk_cols_for_table = set(pks_by_table[table_name])
             if len(pk_cols_for_table) > 1: # Só pode ser junção se PK for composta
                 is_junction = True
                 fk_cols_in_pk = set()
                 for pk_col in pk_cols_for_table:
                     col_key = format_key(table_name, pk_col)
                     # Verifica se CADA coluna da PK é também uma FK
                     if "FK" not in analysis_results["column_roles"].get(col_key, set()):
                         is_junction = False
                         break
                     fk_cols_in_pk.add(pk_col)
                 # Verifica se o conjunto de colunas PK é exatamente o mesmo conjunto de colunas FK que compõem a PK
                 if is_junction and pk_cols_for_table == fk_cols_in_pk:
                     analysis_results["junction_tables"].add(table_name)
                     logger.debug(f"Tabela '{table_name}' identificada como tabela de junção.")
        logger.info(f"Identificadas {len(analysis_results['junction_tables'])} tabelas de junção.")

        # 5. Atualizar schema_details com infos de PK/FK (Só para colunas de TABELAS)
        logger.info("Atualizando detalhes das colunas com informações de PK/FK (Apenas Tabelas)...")
        for object_name, object_data in schema_details.items():
             # Pula a atualização se for uma VIEW
             if object_data["object_type"] == "VIEW":
                  continue 
             for col_data in object_data["columns"]:
                 col_key = format_key(object_name, col_data["name"]) # Usa object_name agora
                 if col_key in pk_columns:
                     col_data["is_pk"] = True
                 if col_key in fk_columns:
                     col_data["is_fk"] = True
                     # Simplificado: Pega a primeira referência encontrada. Poderia ser lista.
                     col_data["fk_references"] = fks_references.get(col_key, [])[0] if fks_references.get(col_key) else None

        # Fechar cursor principal antes de iniciar threads (boa prática)
        if cursor: 
            cursor.close()
            cursor = None 
            
        # 6. Buscar Amostras de Dados em Paralelo
        logger.info(f"Iniciando busca PARALELA por amostras ({MAX_WORKERS_FOR_SAMPLING} workers)... (Isso pode levar tempo)")
        start_sample_time = time.time()
        
        tasks = []
        columns_to_update = [] # Guardar referência para atualizar depois
        for object_name, object_data in schema_details.items():
            for col_data in object_data["columns"]:
                if col_data["type"] != "BLOB": # Só busca amostra se não for BLOB
                    tasks.append((db_params, object_name, col_data["name"], col_data["type"]))
                    columns_to_update.append(col_data) # Guarda a referência do dict da coluna
                else:
                    col_data["sample_values"] = [] # Define como vazio para BLOBs

        total_tasks = len(tasks)
        logger.info(f"Total de {total_tasks} colunas para buscar amostras.")
        processed_count = 0
        results_map = {} # Mapear (table, column) -> samples

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_FOR_SAMPLING) as executor:
            # Usar map para submeter todas as tarefas e obter resultados na ordem
            # Nota: A função fetch_column_samples agora retorna (table, column, samples)
            future_to_task = {executor.submit(fetch_column_samples, *task): task for task in tasks}
            
            for future in concurrent.futures.as_completed(future_to_task):
                try:
                    table_name, column_name, samples = future.result()
                    results_map[(table_name, column_name)] = samples
                except Exception as exc:
                    # Logar erro se a própria thread falhou catastroficamente (raro)
                    task_info = future_to_task[future]
                    logger.error(f'[Thread Pool Error] Task {task_info[1]}.{task_info[2]} gerou exceção: {exc}')
                    # Marcar como erro no mapa para não tentar usar depois
                    results_map[(task_info[1], task_info[2])] = None 
                
                processed_count += 1
                if processed_count % 100 == 0 or processed_count == total_tasks:
                     elapsed = time.time() - start_sample_time
                     logger.info(f"  ... {processed_count}/{total_tasks} amostras processadas ({elapsed:.1f}s).")
                     
        end_sample_time = time.time()
        logger.info(f"Busca paralela por amostras concluída em {end_sample_time - start_sample_time:.2f}s.")
        
        # 7. Atualizar schema_details com amostras coletadas
        logger.info("Atualizando schema com as amostras coletadas...")
        updated_count = 0
        error_count = 0
        for object_name, object_data in schema_details.items():
             for col_data in object_data["columns"]:
                  if col_data["type"] != "BLOB": # Só atualiza se não for BLOB
                       key = (object_name, col_data["name"])
                       if key in results_map:
                            col_data["sample_values"] = results_map[key]
                            if results_map[key] is not None:
                                 updated_count += 1
                            else: 
                                error_count +=1 # Contabiliza erros marcados como None
                       else:
                            # Isso não deveria acontecer se a lógica estiver correta
                            logger.warning(f"Resultado da amostra não encontrado para {object_name}.{col_data['name']}!")
                            col_data["sample_values"] = None 
        logger.info(f"Schema atualizado. Amostras preenchidas para {updated_count} colunas. Falhas/Erros em {error_count} colunas.")
                            
        # 8. Montar resultado final
        logger.info("Montando estrutura final do schema...")
        final_schema = dict(schema_details)
        final_schema["_analysis"] = {
            "composite_pk_tables": sorted(list(analysis_results["composite_pk_tables"])),
            "junction_tables": sorted(list(analysis_results["junction_tables"])),
            "fk_definitions": analysis_results["fk_definitions"],
        }

        logger.info("Extração completa do schema técnico concluída.")
        return final_schema

    except Exception as e:
        logger.error(f"Erro fatal durante a extração do schema: {e}", exc_info=True)
        return None

def main():
    """Função principal para extrair schema completo e salvar."""
    logger.info(f"--- Iniciando Script de Extração de Schema Técnico Completo (DB -> {OUTPUT_JSON_FILE}) ---")
    start_time = time.time()
    conn = None
    full_schema = None
    db_params = None # << NOVO: Guardar params para threads

    # 1. Conectar ao DB e guardar params
    try:
        db_path = config.DEFAULT_DB_PATH
        db_user = config.DEFAULT_DB_USER
        db_charset = config.DEFAULT_DB_CHARSET
        try:
            db_password = st.secrets["database"]["password"]
            # Guarda os parâmetros para passar às threads
            db_params = (db_path, db_user, db_password, db_charset) 
        except KeyError:
             logger.error("Senha não encontrada em st.secrets['database']['password'].")
             return
        except Exception as e:
            logger.error(f"Erro ao acessar st.secrets: {e}.")
            return

        logger.info(f"Tentando conectar ao DB (conexão principal): {db_path}...")
        conn = fdb.connect(dsn=db_path, user=db_user, password=db_password, charset=db_charset)
        logger.info("Conexão principal com o banco de dados estabelecida.")

        # 2. Extrair Schema Completo (passando db_params)
        if conn and db_params:
            full_schema = extract_full_schema_from_db(conn, db_params)
        else:
             logger.error("Falha na obtenção da conexão principal ou parâmetros do DB.")

    except fdb.Error as e:
        logger.error(f"Erro do Firebird durante conexão/extração: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Erro inesperado durante conexão/extração: {e}", exc_info=True)
    finally:
        # Fecha APENAS a conexão principal
        if conn:
            try:
                if not conn.closed: conn.close(); logger.info("Conexão principal DB fechada.")
            except Exception as e: logger.error(f"Erro ao fechar conexão principal DB: {e}")

    # 3. Salvar Schema se extraído com sucesso
    if full_schema:
        logger.info(f"Salvando schema técnico completo em {OUTPUT_JSON_FILE}...")
        # Garante que o diretório de saída exista
        output_dir = os.path.dirname(OUTPUT_JSON_FILE)
        if output_dir: os.makedirs(output_dir, exist_ok=True)
            
        save_start_time = time.time()
        # Usar a função save_json que lida com erros
        if save_json(full_schema, OUTPUT_JSON_FILE):
            save_end_time = time.time()
            logger.info(f"Schema técnico salvo com sucesso em {save_end_time - save_start_time:.2f}s.")
        else:
            logger.error(f"Falha ao salvar schema técnico em {OUTPUT_JSON_FILE}.")
    else:
        logger.error("Não foi possível extrair o schema técnico do banco de dados. Nenhum arquivo foi salvo.")

    end_time = time.time()
    logger.info(f"--- Script Concluído em {end_time - start_time:.2f} segundos ---")

if __name__ == "__main__":
    main() 