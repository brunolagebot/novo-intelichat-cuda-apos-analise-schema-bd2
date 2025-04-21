#!/usr/bin/env python
# coding: utf-8

"""
Script unificado para extrair o schema técnico completo do banco de dados Firebird,
incluindo estrutura, constraints, defaults e amostras de dados, salvando em JSON.

**IMPORTANTE:**
- Execute este script como um módulo a partir da raiz do projeto:
  `python -m scripts.data_preparation.extract_technical_schema`
- A execução, especialmente a etapa de amostragem, pode ser demorada (horas).
- Verifique os logs para erros de amostragem (DBError, charmap, etc.) que podem ocorrer
  em colunas/views específicas, mas o script tentará continuar.
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
from dotenv import load_dotenv # << NOVO

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
OUTPUT_JSON_FILE = "data/metadata/technical_schema_from_db.json" # <-- CORRIGIDO para /metadata/
SAMPLE_SIZE = 50 # Número de amostras distintas a buscar por coluna
MAX_WORKERS_FOR_SAMPLING = 50 # << ALTERADO: Ajustado para 50 workers

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
    # Removido strip().upper() para preservar case original se necessário
    default_source_clean = default_source.strip()
    # Melhorar a regex para capturar o valor após DEFAULT,
    # independentemente de espaços extras ou casing
    match = re.match(r"^DEFAULT\s+(.*)", default_source_clean, re.IGNORECASE)
    if match:
        val_str = match.group(1).strip()
        # Tenta identificar tipos comuns
        if val_str.upper() == 'NULL':
            return None
        # Lida com aspas simples e duplas
        if (val_str.startswith("'") and val_str.endswith("'")) or \
           (val_str.startswith('"') and val_str.endswith('"')):
            return val_str[1:-1] # Remove aspas
        # Verifica se é um número (inteiro, decimal, com sinal)
        if re.match(r'^-?(\d+\.?\d*|\.\d+)$', val_str):
            try: # Tenta converter para int se não tiver ponto decimal
                if '.' not in val_str:
                    return int(val_str)
                else:
                    # Mantém como string para decimais para evitar perda de precisão com float
                    # Poderia usar decimal.Decimal(val_str) se precisão exata for crucial
                    return val_str 
            except ValueError:
                pass # Se falhar, retorna a string original
        # Outros casos (ex: CURRENT_TIMESTAMP, etc.) podem precisar de tratamento
        return val_str # Retorna a string original se não reconhecer
    return None # Não encontrou padrão 'DEFAULT ...'

def map_fb_type(type_code, sub_type, scale):
    """Mapeia códigos de tipo Firebird para nomes legíveis."""
    if type_code == 261: # BLOB
        # Poderia detalhar subtipo (0=Segmented, 1=Text, etc.) se necessário
        # Mapeia BLOB SUB_TYPE 1 para TEXT para clareza
        return "TEXT" if sub_type == 1 else "BLOB"
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

def fetch_column_samples(db_params, table_name, column_name, column_type):
    """Busca amostras de dados para uma coluna (THREAD-SAFE). Abre e fecha conexão própria."""
    # Extrai parâmetros de conexão
    db_path, db_user, db_password, db_charset = db_params

    # Não busca amostras para BLOBs/TEXT
    if column_type in ["BLOB", "TEXT"]:
        return table_name, column_name, [] # Retorna identificadores + resultado vazio

    samples = []
    conn = None
    cursor = None
    try:
        # Conecta ao DB DENTRO da thread
        conn = fdb.connect(database=db_path, user=db_user, password=db_password, charset=db_charset)
        cursor = conn.cursor()
        # -- Correção: Adicionar aspas duplas ao nome da coluna também --
        sql = f'SELECT FIRST {SAMPLE_SIZE} DISTINCT "{column_name}" FROM "{table_name}" WHERE "{column_name}" IS NOT NULL'
        cursor.execute(sql)
        results = cursor.fetchall()
        if results:
            samples = [convert_to_json_serializable(row[0]) for row in results]
        else:
            samples = []

    except fdb.OperationalError as e:
        # Erros operacionais comuns (ex: tabela não existe, coluna não existe)
        logger.warning(f"[Thread Sample OpError] {table_name}.{column_name}: {e}")
        samples = None # Indica erro
    except fdb.Error as e:
        # Outros erros de DB
        logger.warning(f"[Thread Sample DBError] {table_name}.{column_name}: {e.sqlcode if hasattr(e, 'sqlcode') else e}")
        samples = None
    except Exception as e:
        logger.warning(f"[Thread Sample OtherError] {table_name}.{column_name}: {e}")
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
    logger.info("Iniciando extração completa do schema técnico...") # Atualizado log
    # Agora a chave principal é o nome da tabela/view
    schema_details = defaultdict(lambda: {
        "columns": [], 
        "object_type": None, 
        "description": None, # Descrição técnica do DB
        "object_business_description": None, # Descrição manual do objeto
        "object_ai_generated_description": None, # Descrição IA do objeto
        "object_ai_model_used": None, # Modelo IA do objeto
        "object_ai_generation_timestamp": None # Timestamp IA do objeto
    })
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

        # Fechar cursor principal (boa prática)
        if cursor: 
            cursor.close()
            cursor = None 
            
        logger.info("Pulando etapa de busca de amostras.") # <<< NOVO Log
                            
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
    """Função principal para executar a extração do schema e salvar o resultado."""
    load_dotenv() # Carrega variáveis de .env se existir
    logger.info("--- Iniciando Script: Extração de Schema Técnico Detalhado --- ")
    start_time = time.time()
    conn = None
    full_schema = None
    db_params = None # << NOVO: Guardar params para threads

    # 1. Conectar ao DB e guardar params
    try:
        # --- ATUALIZADO: Ler tudo do ENV --- #
        db_host = os.getenv("FIREBIRD_HOST", "localhost")
        db_port = int(os.getenv("FIREBIRD_PORT", "3050"))
        db_path = os.getenv("FIREBIRD_DB_PATH")
        db_user = os.getenv("FIREBIRD_USER", "SYSDBA")
        db_password = os.getenv("FIREBIRD_PASSWORD")
        db_charset = os.getenv("FIREBIRD_CHARSET", "WIN1252")
        
        if not db_path or not db_password:
            logger.error("Erro: Variáveis FIREBIRD_DB_PATH ou FIREBIRD_PASSWORD não definidas no .env ou ambiente.")
            return
        
        # Guarda os parâmetros para passar às threads
        db_params = (db_path, db_user, db_password, db_charset)
        # --- FIM ATUALIZAÇÃO ---

        logger.info(f"Tentando conectar ao DB (conexão principal): {db_path} (Host: {db_host}:{db_port})...") # Log atualizado
        # --- ATUALIZADO: Usar variáveis lidas do ENV na conexão --- #
        conn = fdb.connect(
            host=db_host,
            port=db_port,
            database=db_path,
            user=db_user,
            password=db_password,
            charset=db_charset
        )
        # --- FIM ATUALIZAÇÃO ---
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
                conn.close()
            except Exception as e:
                logger.error(f"Erro ao fechar conexão principal: {e}")

    end_time = time.time()
    logger.info(f"Extração completa do schema (incluindo amostras) concluída em {end_time - start_time:.2f}s")
    return full_schema

if __name__ == "__main__":
    # <<< CORREÇÃO: Capturar o resultado e salvar em JSON >>>
    final_schema_data = main()
    if final_schema_data:
        logger.info(f"Salvando schema técnico completo em {OUTPUT_JSON_FILE}...")
        try:
            save_json(final_schema_data, OUTPUT_JSON_FILE)
            logger.info("Schema salvo com sucesso.")
        except Exception as e:
             logger.error(f"Erro ao salvar o schema no arquivo JSON '{OUTPUT_JSON_FILE}': {e}", exc_info=True)
    else:
        logger.error("Não foi possível gerar o schema técnico (main retornou None). Verifique os logs.")
    # <<< FIM CORREÇÃO >>> 