#!/usr/bin/env python
# coding: utf-8

"""
Script para extrair a estrutura de chaves (PKs, FKs) diretamente do banco de dados
Firebird e compará-la com a análise previamente salva em JSON.
"""

import os
import sys
import json
import logging
import time
from collections import defaultdict
import fdb # Importar fdb diretamente
import streamlit as st # Importar streamlit para st.secrets

# --- Adiciona o diretório raiz ao sys.path --- #
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM Adição ao sys.path --- #

import src.core.config as config
from src.core.logging_config import setup_logging
from src.utils.json_helpers import load_json # Usar a função segura de carregamento

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Consultas SQL para Metadados Firebird ---

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
    return f"{table_name.strip()}:{column_name.strip()}"

def get_db_key_structure(conn):
    """Extrai e processa a estrutura de chaves do banco de dados conectado."""
    logger.info("Extraindo estrutura de chaves (PKs e FKs) do banco de dados...")
    db_struct = {
        "pk_columns": set(), # Set de strings "tabela:coluna"
        "composite_pk_tables": set(), # Set de nomes de tabelas
        "fk_definitions": {}, # Dict[str, dict]: fk_signature -> {fk_cols: [], pk_table: str, pk_cols: []}
        "fk_columns": set(), # Set de strings "tabela:coluna" que participam de alguma FK
        "column_roles": defaultdict(set) # Dict[str, set]: "tabela:coluna" -> {'PK', 'FK'}
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
            table_name, column_name, _ = row # Ignora a posição por enquanto
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
            fk_name = fk_name.strip()
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

        # Normalizar e armazenar FKs (garantir ordem das colunas e criar assinatura)
        for (fk_table, fk_name), details in fks_by_constraint.items():
            # Assinatura única para a FK baseada nas tabelas e colunas ordenadas
            fk_cols_sorted = tuple(sorted(details['fk_cols']))
            pk_cols_sorted = tuple(sorted(details['pk_cols']))
            pk_table = details['pk_table']
            # Assinatura: fk_table(col1,col2) -> pk_table(colA,colB)
            fk_signature = f"{fk_table}{fk_cols_sorted} -> {pk_table}{pk_cols_sorted}"
            
            if fk_signature not in db_struct["fk_definitions"]:
                 db_struct["fk_definitions"][fk_signature] = {
                     "fk_table": fk_table,
                     "fk_cols": list(fk_cols_sorted),
                     "pk_table": pk_table,
                     "pk_cols": list(pk_cols_sorted)
                 }
            # Poderia adicionar fk_name aqui se necessário para depuração

        logger.info("Extração da estrutura do DB concluída.")
        return db_struct

    except Exception as e:
        logger.error(f"Erro ao extrair estrutura do banco de dados: {e}", exc_info=True)
        return None
    finally:
        if cursor:
            cursor.close()


def load_json_key_structure(file_path):
    """Carrega a estrutura de chaves do arquivo JSON."""
    logger.info(f"Carregando estrutura de chaves do arquivo JSON: {file_path}")
    if not os.path.exists(file_path):
        logger.error(f"Arquivo JSON não encontrado: {file_path}")
        return None
        
    try:
        data = load_json(file_path) # Usa helper seguro
        if not data:
             logger.error(f"Falha ao carregar ou arquivo JSON vazio: {file_path}")
             return None

        # Normalizar a estrutura JSON para comparação
        json_struct = {
            "pk_columns": set(),
            "composite_pk_tables": set(data.get("composite_pk_tables", [])),
            "fk_definitions": {}, # Recriar a partir de composite_fk_details se necessário
            "fk_columns": set(),
            "column_roles": defaultdict(set) # Dict[str, set]: "tabela:coluna" -> {'PK', 'FK'}
        }

        # Processar column_roles (chave já deve ser string "tabela:coluna")
        raw_roles = data.get("column_roles", {})
        for key, roles in raw_roles.items():
             # Garantir que 'roles' seja um conjunto de strings válidas
             valid_roles = set()
             if isinstance(roles, str): # Caso simples onde é só 'PK' ou 'FK'
                 if roles in {"PK", "FK"}: valid_roles.add(roles)
             elif isinstance(roles, (list, set, tuple)): # Caso seja uma coleção
                 for r in roles:
                      if isinstance(r, str) and r in {"PK", "FK"}: valid_roles.add(r)
             
             if valid_roles:
                 json_struct["column_roles"][key] = valid_roles
                 if "PK" in valid_roles: json_struct["pk_columns"].add(key)
                 if "FK" in valid_roles: json_struct["fk_columns"].add(key)


        # Processar composite_fk_details para gerar fk_definitions (adaptar à estrutura real do JSON)
        # Esta parte DEPENDE MUITO da estrutura exata de 'composite_fk_details' no seu JSON.
        # Assumindo que seja um dicionário onde a chave é alguma representação da FK
        # e o valor tem 'fk_table', 'fk_columns', 'references_table', 'references_columns'
        raw_comp_fks = data.get("composite_fk_details", {})
        for fk_key_json, details in raw_comp_fks.items():
             try:
                 fk_table = details.get('fk_table')
                 fk_cols = details.get('fk_columns')
                 pk_table = details.get('references_table')
                 pk_cols = details.get('references_columns')

                 if fk_table and fk_cols and pk_table and pk_cols:
                     fk_cols_sorted = tuple(sorted(fk_cols))
                     pk_cols_sorted = tuple(sorted(pk_cols))
                     fk_signature = f"{fk_table}{fk_cols_sorted} -> {pk_table}{pk_cols_sorted}"
                     
                     json_struct["fk_definitions"][fk_signature] = {
                         "fk_table": fk_table,
                         "fk_cols": list(fk_cols_sorted),
                         "pk_table": pk_table,
                         "pk_cols": list(pk_cols_sorted)
                     }
                 else:
                      logger.warning(f"Detalhe de FK composto incompleto no JSON (chave: {fk_key_json}): {details}")
             except Exception as e:
                  logger.warning(f"Erro processando FK composta do JSON (chave: {fk_key_json}): {e}")
        
        logger.info("Estrutura do JSON carregada e normalizada.")
        return json_struct
        
    except Exception as e:
        logger.error(f"Erro ao carregar ou processar o arquivo JSON: {e}", exc_info=True)
        return None


def compare_structures(db_struct, json_struct):
    """Compara as estruturas do DB e do JSON e loga as diferenças."""
    logger.info("--- Iniciando Comparação entre Estrutura do DB e JSON ---")
    
    # Comparar Tabelas com PKs Compostas
    db_comp_pks = db_struct["composite_pk_tables"]
    json_comp_pks = json_struct["composite_pk_tables"]
    logger.info(f"[PKs Compostas] DB: {len(db_comp_pks)}, JSON: {len(json_comp_pks)}")
    if db_comp_pks != json_comp_pks:
        logger.warning("Diferenças encontradas em TABELAS COM PKs COMPOSTAS:")
        logger.info(f"  >> Apenas no DB: {sorted(list(db_comp_pks - json_comp_pks))}")
        logger.info(f"  >> Apenas no JSON: {sorted(list(json_comp_pks - db_comp_pks))}")
    else:
        logger.info("[PKs Compostas] Sem diferenças encontradas.")

    # Comparar Colunas que são PK
    db_pk_cols = db_struct["pk_columns"]
    json_pk_cols = json_struct["pk_columns"]
    logger.info(f"[Colunas PK] DB: {len(db_pk_cols)}, JSON: {len(json_pk_cols)}")
    if db_pk_cols != json_pk_cols:
        logger.warning("Diferenças encontradas em COLUNAS marcadas como PK:")
        logger.info(f"  >> Apenas no DB: {sorted(list(db_pk_cols - json_pk_cols))}")
        logger.info(f"  >> Apenas no JSON: {sorted(list(json_pk_cols - db_pk_cols))}")
    else:
        logger.info("[Colunas PK] Sem diferenças encontradas.")

    # Comparar Colunas que são FK
    db_fk_cols = db_struct["fk_columns"]
    json_fk_cols = json_struct["fk_columns"]
    logger.info(f"[Colunas FK] DB: {len(db_fk_cols)}, JSON: {len(json_fk_cols)}")
    if db_fk_cols != json_fk_cols:
        logger.warning("Diferenças encontradas em COLUNAS marcadas como FK:")
        logger.info(f"  >> Apenas no DB: {sorted(list(db_fk_cols - json_fk_cols))}")
        logger.info(f"  >> Apenas no JSON: {sorted(list(json_fk_cols - db_fk_cols))}")
    else:
        logger.info("[Colunas FK] Sem diferenças encontradas.")

    # Comparar Definições de FKs (pela assinatura)
    db_fk_defs = set(db_struct["fk_definitions"].keys())
    json_fk_defs = set(json_struct["fk_definitions"].keys())
    logger.info(f"[Definições FK] DB: {len(db_fk_defs)}, JSON: {len(json_fk_defs)}")
    if db_fk_defs != json_fk_defs:
         logger.warning("Diferenças encontradas nas DEFINIÇÕES DE FK (baseado na assinatura TabelaFK(ColunasFK) -> TabelaPK(ColunasPK)):")
         only_in_db = sorted(list(db_fk_defs - json_fk_defs))
         only_in_json = sorted(list(json_fk_defs - db_fk_defs))
         logger.info(f"  >> Apenas no DB ({len(only_in_db)}):")
         for fk_sig in only_in_db[:10]: # Logar apenas as primeiras 10 para não poluir
              logger.info(f"     - {fk_sig}")
         if len(only_in_db) > 10: logger.info("     - ... (e mais)")
             
         logger.info(f"  >> Apenas no JSON ({len(only_in_json)}):")
         for fk_sig in only_in_json[:10]: # Logar apenas as primeiras 10
              logger.info(f"     - {fk_sig}")
         if len(only_in_json) > 10: logger.info("     - ... (e mais)")
    else:
         logger.info("[Definições FK] Sem diferenças encontradas.")
         
    # Comparar Papéis das Colunas (PK/FK)
    logger.info("[Papéis das Colunas (PK/FK)] Comparando...")
    all_col_keys = set(db_struct["column_roles"].keys()) | set(json_struct["column_roles"].keys())
    diff_found = False
    for col_key in sorted(list(all_col_keys)):
        db_roles = db_struct["column_roles"].get(col_key, set())
        json_roles = json_struct["column_roles"].get(col_key, set())
        if db_roles != json_roles:
            if not diff_found:
                 logger.warning("Diferenças encontradas nos PAPÉIS das colunas (PK/FK):")
                 diff_found = True
            logger.info(f"  - Coluna '{col_key}': DB={sorted(list(db_roles)) if db_roles else 'Nenhum'} | JSON={sorted(list(json_roles)) if json_roles else 'Nenhum'}")
            
    if not diff_found:
        logger.info("[Papéis das Colunas (PK/FK)] Sem diferenças encontradas.")

    logger.info("--- Fim da Comparação ---")


def main():
    """Função principal para executar a comparação."""
    logger.info("--- Iniciando Script de Comparação de Estrutura de Chaves (DB vs JSON) ---")
    start_time = time.time()

    conn = None
    db_structure = None
    json_structure = None

    # 1. Obter credenciais e conectar ao DB
    try:
        db_path = config.DEFAULT_DB_PATH
        db_user = config.DEFAULT_DB_USER
        db_charset = config.DEFAULT_DB_CHARSET
        try:
            db_password = st.secrets["database"]["password"]
            logger.info("Credenciais do DB obtidas (exceto senha de st.secrets).")
        except KeyError:
             logger.error("Senha não encontrada em st.secrets['database']['password']. Verifique .streamlit/secrets.toml")
             return # Aborta se não tem senha
        except Exception as e:
            logger.error(f"Erro ao acessar st.secrets: {e}. Certifique-se que está executando em um ambiente com Streamlit.")
            return # Aborta

        logger.info(f"Tentando conectar ao DB: {db_path}...")
        conn = fdb.connect(
            dsn=db_path,
            user=db_user,
            password=db_password,
            charset=db_charset
        )
        logger.info("Conexão com o banco de dados estabelecida.")
        
        # Extrair estrutura do DB usando a conexão ativa
        db_structure = get_db_key_structure(conn)

    except fdb.Error as e:
        logger.error(f"Erro do Firebird durante a conexão ou extração: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Erro geral durante a conexão ou extração do DB: {e}", exc_info=True)
    finally:
        if conn:
            try:
                if not conn.closed:
                    conn.close()
                    logger.info("Conexão com o banco de dados fechada.")
            except Exception as e:
                logger.error(f"Erro ao fechar a conexão com o DB: {e}", exc_info=True)

    # 2. Carregar estrutura do JSON
    json_file_path = config.KEY_ANALYSIS_RESULTS_FILE
    json_structure = load_json_key_structure(json_file_path)

    # 3. Comparar se ambas as estruturas foram obtidas
    if db_structure and json_structure:
        compare_structures(db_structure, json_structure)
    else:
        logger.error("Não foi possível obter ambas as estruturas (DB e JSON) para comparação.")

    end_time = time.time()
    logger.info(f"--- Script Concluído em {end_time - start_time:.2f} segundos ---")

if __name__ == "__main__":
    main()

"""
**Notas Importantes:**

1.  **Dependência de `db_utils`:** Este script assume que `src/core/db_utils.py` contém as funções `connect_db()` e `close_db()` que lidam com a obtenção de credenciais (via `config.py`, `st.secrets` ou variáveis de ambiente) e o estabelecimento/fechamento da conexão Firebird.
2.  **Estrutura do JSON:** A parte que processa `composite_fk_details` no JSON (`load_json_key_structure`) é uma *suposição* sobre como esses dados estão armazenados. Pode precisar de ajuste dependendo da estrutura real do seu `key_analysis_results.json`.
3.  **Normalização:** A comparação depende de normalizar as representações (ex: ordenar colunas em chaves compostas, usar strings `tabela:coluna`).
4.  **Desempenho:** Para bancos de dados muito grandes, as consultas de metadados podem levar algum tempo.
5.  **Apenas Leitura:** O script é seguro para executar, pois apenas lê metadados do banco e o arquivo JSON.
""" 