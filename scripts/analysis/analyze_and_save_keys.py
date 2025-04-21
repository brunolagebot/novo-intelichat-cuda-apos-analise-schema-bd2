#!/usr/bin/env python
# coding: utf-8

"""Script para analisar a estrutura de chaves DIRETAMENTE DO BANCO DE DADOS e salvar os resultados."""

import os
import sys
import json
import logging
import time
from collections import defaultdict
import fdb # Para conexão direta
import streamlit as st # Para st.secrets

# Adiciona o diretório raiz ao sys.path para permitir importações de src
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(ROOT_DIR)

# Importar o módulo de configuração completo
import src.core.config as config

from src.core.config import (
    TECHNICAL_SCHEMA_FILE,
    KEY_ANALYSIS_RESULTS_FILE
)
# REMOVIDO: from src.core.analysis import analyze_key_structure # Movido
# from src.analysis.analysis import analyze_key_structure # Atualizado (ainda comentado)
from src.utils.json_helpers import load_json, save_json
from src.core.log_utils import setup_logging

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Consultas SQL (copiadas de compare_db_json_key_structure.py) ---

SQL_GET_PKS = """
    SELECT
        rc.RDB$RELATION_NAME AS TABLE_NAME,
        isc.RDB$FIELD_NAME AS COLUMN_NAME,
        isc.RDB$FIELD_POSITION AS COL_POSITION
    FROM RDB$RELATION_CONSTRAINTS rc
    JOIN RDB$INDEX_SEGMENTS isc ON rc.RDB$INDEX_NAME = isc.RDB$INDEX_NAME
    WHERE rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
    ORDER BY rc.RDB$RELATION_NAME, isc.RDB$FIELD_POSITION;
"""

SQL_GET_FKS = """
    SELECT
        rc.RDB$RELATION_NAME AS FK_TABLE,
        rc.RDB$CONSTRAINT_NAME AS FK_NAME,
        isc.RDB$FIELD_NAME AS FK_COLUMN,
        isc.RDB$FIELD_POSITION AS FK_COL_POS,
        rc_pk.RDB$RELATION_NAME AS PK_TABLE,
        isc_pk.RDB$FIELD_NAME AS PK_COLUMN
    FROM RDB$RELATION_CONSTRAINTS rc
    JOIN RDB$INDEX_SEGMENTS isc ON rc.RDB$INDEX_NAME = isc.RDB$INDEX_NAME
    JOIN RDB$REF_CONSTRAINTS refc ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
    JOIN RDB$RELATION_CONSTRAINTS rc_pk ON refc.RDB$CONST_NAME_UQ = rc_pk.RDB$CONSTRAINT_NAME
    JOIN RDB$INDEX_SEGMENTS isc_pk ON rc_pk.RDB$INDEX_NAME = isc_pk.RDB$INDEX_NAME AND isc.RDB$FIELD_POSITION = isc_pk.RDB$FIELD_POSITION
    WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
    ORDER BY rc.RDB$RELATION_NAME, rc.RDB$CONSTRAINT_NAME, isc.RDB$FIELD_POSITION;
"""

def format_key(table_name, column_name):
    """Formata a chave de coluna consistentemente como string."""
    # Garante que não haja espaços extras que causem problemas
    return f"{table_name.strip()}:{column_name.strip()}"

def get_db_key_structure(conn):
    """Extrai e processa a estrutura de chaves do banco de dados conectado."""
    # (Função adaptada de compare_db_json_key_structure.py)
    logger.info("Extraindo estrutura de chaves (PKs e FKs) do banco de dados...")
    db_struct = {
        "pk_columns": set(),
        "composite_pk_tables": set(),
        "fk_definitions": {}, # Usaremos a assinatura como chave para evitar duplicatas lógicas
        "fk_columns": set(),
        "column_roles": defaultdict(set),
        "junction_tables": set() # NOVO: Para identificar tabelas de junção
    }
    cursor = None

    try:
        cursor = conn.cursor()

        # --- Processar PKs ---
        logger.debug("Executando query para PKs...")
        cursor.execute(SQL_GET_PKS)
        raw_pks = cursor.fetchall()
        logger.info(f"Encontrados {len(raw_pks)} segmentos de PKs.")

        pks_by_table = defaultdict(list)
        for row in raw_pks:
            table_name, column_name, _ = row
            table_name = table_name.strip()
            column_name = column_name.strip()
            pks_by_table[table_name].append(column_name)
            col_key = format_key(table_name, column_name)
            db_struct["pk_columns"].add(col_key)
            db_struct["column_roles"][col_key].add("PK")

        for table, columns in pks_by_table.items():
            if len(columns) > 1:
                db_struct["composite_pk_tables"].add(table)

        # --- Processar FKs ---
        logger.debug("Executando query para FKs...")
        cursor.execute(SQL_GET_FKS)
        raw_fks = cursor.fetchall()
        logger.info(f"Encontrados {len(raw_fks)} segmentos de FKs.")

        fks_by_constraint = defaultdict(lambda: {"fk_cols": [], "pk_table": None, "pk_cols": []})
        for row in raw_fks:
            fk_table, fk_name, fk_column, _, pk_table, pk_column = row
            fk_table = fk_table.strip()
            fk_name = fk_name.strip() # Guardar nome da constraint pode ser útil
            fk_column = fk_column.strip()
            pk_table = pk_table.strip()
            pk_column = pk_column.strip()

            constraint_key = (fk_table, fk_name)
            fks_by_constraint[constraint_key]["fk_cols"].append(fk_column)
            fks_by_constraint[constraint_key]["pk_table"] = pk_table
            fks_by_constraint[constraint_key]["pk_cols"].append(pk_column)

            col_key = format_key(fk_table, fk_column)
            db_struct["fk_columns"].add(col_key)
            db_struct["column_roles"][col_key].add("FK")

        # Normalizar e armazenar FKs (criar assinatura única para a relação lógica)
        for (fk_table, fk_name), details in fks_by_constraint.items():
            fk_cols_sorted = tuple(sorted(details['fk_cols']))
            pk_cols_sorted = tuple(sorted(details['pk_cols']))
            pk_table = details['pk_table']
            # Assinatura: fk_table(col1,col2...) -> pk_table(colA,colB...)
            # Usamos a assinatura lógica como chave principal para fk_definitions
            fk_signature = f"{fk_table}{fk_cols_sorted} -> {pk_table}{pk_cols_sorted}"

            # --- Adicionar marcador para FKs Compostas --- 
            is_composite_fk = len(fk_cols_sorted) > 1
            if is_composite_fk:
                for fk_col in details['fk_cols']:
                    col_key_comp = format_key(fk_table, fk_col)
                    # Adiciona o marcador sem remover outros papéis como "FK"
                    db_struct["column_roles"][col_key_comp].add("FK_COMP_PART") 
            # ------------------------------------------- 

            if fk_signature not in db_struct["fk_definitions"]:
                 db_struct["fk_definitions"][fk_signature] = {
                     "fk_table": fk_table,
                     "fk_cols": list(fk_cols_sorted), # Salvar como lista
                     "pk_table": pk_table,
                     "pk_cols": list(pk_cols_sorted), # Salvar como lista
                     "constraint_names": [fk_name] # Guarda a primeira constraint que define esta relação
                 }
            else:
                # Se a mesma relação lógica for definida por outra constraint, apenas adiciona o nome
                 if fk_name not in db_struct["fk_definitions"][fk_signature]["constraint_names"]:
                      db_struct["fk_definitions"][fk_signature]["constraint_names"].append(fk_name)

        # --- NOVO: Identificar Tabelas de Junção --- 
        logger.info("Identificando tabelas de junção...")
        all_tables_with_pks = set(pks_by_table.keys())
        for table_name in all_tables_with_pks:
            pk_cols_for_table = set(pks_by_table[table_name])
            
            # Otimização: Pular se PK não for composta
            if len(pk_cols_for_table) <= 1:
                continue 
                
            fk_cols_in_table = set()
            for col_key, roles in db_struct["column_roles"].items():
                t, c = col_key.split(':', 1)
                if t == table_name and "FK" in roles:
                    fk_cols_in_table.add(c)
            
            # Condição: PK é composta E todas as colunas da PK são também FKs E 
            #           o conjunto de colunas PK é igual ao conjunto de colunas FK da tabela
            if pk_cols_for_table == fk_cols_in_table:
                logger.debug(f"Tabela '{table_name}' identificada como possível tabela de junção.")
                db_struct["junction_tables"].add(table_name)

        logger.info(f"Identificadas {len(db_struct['junction_tables'])} potenciais tabelas de junção.")
        logger.info("Extração da estrutura do DB concluída.")
        return db_struct

    except Exception as e:
        logger.error(f"Erro ao extrair estrutura do banco de dados: {e}", exc_info=True)
        return None
    finally:
        if cursor:
            cursor.close()


def convert_tuple_keys_to_str(obj):
    """Converte recursivamente chaves de tupla em strings formatadas (mantido por segurança)."""
    if isinstance(obj, dict):
        new_dict = {}
        for k, v in obj.items():
            if isinstance(k, tuple):
                new_key = ":".join(map(str, k))
            else:
                new_key = k

            if not isinstance(new_key, (str, int, float, bool, type(None))):
                 logger.warning(f"Chave não convertida para tipo válido de JSON: {new_key} ({type(new_key)}). Convertendo para string.")
                 new_key = str(new_key)
            new_dict[new_key] = convert_tuple_keys_to_str(v)
        return new_dict
    elif isinstance(obj, list):
        return [convert_tuple_keys_to_str(elem) for elem in obj]
    elif isinstance(obj, set): # NOVO: Converter sets para listas
         return sorted(list(obj)) # Ordena para consistência
    else:
        return obj

def main():
    """Função principal para extrair chaves do DB e salvar resultados."""
    logger.info("--- Iniciando Análise e Salvamento da Estrutura de Chaves (Direto do DB) --- ")
    start_time = time.time()

    conn = None
    key_analysis_results = None
    output_analysis_path = config.KEY_ANALYSIS_RESULTS_FILE

    # 1. Obter credenciais e conectar ao DB
    try:
        db_path = config.DEFAULT_DB_PATH
        db_user = config.DEFAULT_DB_USER
        db_charset = config.DEFAULT_DB_CHARSET
        try:
            db_password = st.secrets["database"]["password"]
            logger.info("Credenciais do DB obtidas (senha de st.secrets).")
        except KeyError:
             logger.error("Senha não encontrada em st.secrets['database']['password'].")
             return
        except Exception as e:
            logger.error(f"Erro ao acessar st.secrets: {e}.")
            return

        logger.info(f"Tentando conectar ao DB: {db_path}...")
        conn = fdb.connect(dsn=db_path, user=db_user, password=db_password, charset=db_charset)
        logger.info("Conexão com o banco de dados estabelecida.")

        # 2. Extrair estrutura de chaves do DB
        analysis_start_time = time.time()
        key_analysis_results = get_db_key_structure(conn)
        analysis_end_time = time.time()

        if not key_analysis_results:
            logger.error("Falha ao extrair a estrutura de chaves do banco de dados. Abortando.")
            return

        logger.info(f"Análise de chaves do DB concluída em {analysis_end_time - analysis_start_time:.2f}s.")

        # 3. Preparar e Salvar Resultados
        logger.info(f"Preparando resultados para salvamento em {output_analysis_path}...")

        # --- Adicionar Cálculo de Contagem de Referências (fk_reference_counts) ---
        fk_reference_counts = defaultdict(int)
        for fk_def in key_analysis_results["fk_definitions"].values():
            pk_table = fk_def["pk_table"]
            pk_cols = fk_def["pk_cols"]
            # Para PKs simples ou compostas, contamos cada coluna PK referenciada
            for pk_col in pk_cols:
                 col_key = f"{pk_table}.{pk_col}" # Formato esperado pela UI
                 fk_reference_counts[col_key] += 1
        logger.info(f"Calculadas contagens de referência para {len(fk_reference_counts)} colunas PK.")


        # --- Adicionar Geração de Detalhes de FKs Compostas (composite_fk_details) ---
        composite_fk_details = {}
        for fk_def in key_analysis_results["fk_definitions"].values():
            # Considera FK composta se tiver mais de 1 coluna na FK
            if len(fk_def['fk_cols']) > 1:
                 fk_table = fk_def['fk_table']
                 pk_table = fk_def['pk_table']
                 fk_constraint_names = fk_def.get('constraint_names', []) # Pode haver múltiplas constraints para mesma relação lógica
                 fk_name_display = ", ".join(fk_constraint_names) if fk_constraint_names else 'N/A'

                 # Para cada coluna na FK composta, adiciona uma entrada
                 for i, fk_col in enumerate(fk_def['fk_cols']):
                     pk_col = fk_def['pk_cols'][i] # Assume correspondência pela ordem/posição
                     # Chave no formato esperado pela UI
                     detail_key = (fk_table, fk_col)
                     composite_fk_details[detail_key] = {
                         'fk_name': fk_name_display,
                         'referenced_table': pk_table,
                         'referenced_column': pk_col
                     }
        logger.info(f"Gerados detalhes para {len(composite_fk_details)} segmentos de FKs compostas.")

        # --- Processar Roles e Níveis de Importância --- 
        processed_column_roles = {}
        raw_column_roles = key_analysis_results["column_roles"] # defaultdict(set)
        composite_pk_tables_set = key_analysis_results["composite_pk_tables"] # set

        for key_str, roles_set in raw_column_roles.items():
            try:
                table_name, _ = key_str.split(':', 1)
            except ValueError:
                logger.warning(f"Skipping role processing for invalid key: {key_str}")
                continue

            final_role = "Normal"
            importance_level = "Baixa"

            is_pk = "PK" in roles_set
            is_fk = "FK" in roles_set
            is_fk_comp_part = "FK_COMP_PART" in roles_set
            is_pk_comp_table = table_name in composite_pk_tables_set

            if is_pk and is_fk:
                final_role = "PK/FK"
                importance_level = "Máxima"
            elif is_pk:
                final_role = "PK (Comp.)" if is_pk_comp_table else "PK"
                importance_level = "Alta"
            elif is_fk_comp_part:
                final_role = "FK (Comp.)"
                importance_level = "Média"
            elif is_fk: # Apenas FK simples (não parte de composta)
                final_role = "FK"
                importance_level = "Média"
            # Se não for PK nem FK, mantém "Normal"/"Baixa"
            
            processed_column_roles[key_str] = {
                 'role': final_role,
                 'importance_level': importance_level
             }
        logger.info(f"Processados papéis e níveis de importância para {len(processed_column_roles)} colunas.")
        # ------------------------------------------------- 

        # Estrutura final para salvar (precisa converter sets e garantir chaves string)
        # A estrutura de 'fk_definitions' já tem chaves string (assinatura)
        final_output = {
            "composite_pk_tables": sorted(list(key_analysis_results["composite_pk_tables"])),
            "junction_tables": sorted(list(key_analysis_results["junction_tables"])),
            "composite_fk_details": convert_tuple_keys_to_str(composite_fk_details), # Converte chaves (table, col) para string "table:col"
            "column_roles": processed_column_roles, # USA A VERSÃO PROCESSADA!
            "fk_reference_counts": dict(fk_reference_counts), # Converte defaultdict para dict
            # Manter fk_definitions pode ser útil para depuração ou análises futuras
            "_raw_fk_definitions": key_analysis_results["fk_definitions"]
        }

        # Aplicar conversão final (redundante se get_db_key_structure for perfeito, mas seguro)
        # Note que convert_tuple_keys_to_str agora também converte sets para listas
        final_output_converted = convert_tuple_keys_to_str(final_output)

        # Garante que o diretório de saída exista
        analysis_save_dir = os.path.dirname(output_analysis_path)
        if analysis_save_dir:
             os.makedirs(analysis_save_dir, exist_ok=True)

        save_start_time = time.time()
        if save_json(final_output_converted, output_analysis_path):
             save_end_time = time.time()
             logger.info(f"Resultados da análise de chaves (do DB) salvos com sucesso em {save_end_time - save_start_time:.2f}s.")
        else:
             logger.error(f"Falha ao salvar resultados da análise de chaves em {output_analysis_path}. Verifique logs anteriores.")

    except fdb.Error as e:
        logger.error(f"Erro do Firebird durante o processo: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Erro inesperado durante a análise ou salvamento: {e}", exc_info=True)
    finally:
        if conn:
            try:
                if not conn.closed:
                    conn.close()
                    logger.info("Conexão com o banco de dados fechada.")
            except Exception as e:
                logger.error(f"Erro ao fechar a conexão com o DB: {e}", exc_info=True)

    end_time = time.time()
    logger.info(f"--- Processo de Análise e Salvamento (Direto do DB) Concluído em {end_time - start_time:.2f} segundos --- ")

if __name__ == "__main__":
    main() 