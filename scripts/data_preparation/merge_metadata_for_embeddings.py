"""Script para mesclar dados de múltiplas fontes (schema técnico base,
metadados manuais e descrições geradas por IA) em um único arquivo JSON.

Este arquivo resultante é projetado para ser a entrada para o processo de geração
de embeddings vetoriais.
"""

import json
import os
import sys
import logging
import argparse
from collections import defaultdict, OrderedDict
from pathlib import Path
import time

# Adiciona o diretório raiz ao sys.path para importações de módulos
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(ROOT_DIR))

# Importações do projeto
from src.utils.json_helpers import load_json, save_json
try:
    from src.core.log_utils import setup_logging
    setup_logging() # Configura o logger conforme definido no projeto
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    print("AVISO: src.core.log_utils.setup_logging não encontrado. Usando config básica.")

# Importar constantes de config.py
from src.core.config import (
    TECHNICAL_SCHEMA_FILE, # Schema Base
    METADATA_FILE, # Manual
    AI_DESCRIPTIONS_FILE, # Default para AI
    MERGED_SCHEMA_FOR_EMBEDDINGS_FILE # Saída
)

logger = logging.getLogger(__name__)

# --- Funções Auxiliares ---

def build_ai_lookup(ai_data):
    """Cria um dicionário de lookup para descrições de IA."""
    lookup = {}
    if not isinstance(ai_data, list):
        logger.warning("Dados de IA não estão no formato esperado (lista). Lookup ficará vazio.")
        return lookup

    for item in ai_data:
        if isinstance(item, dict) and 'object_name' in item and 'column_name' in item:
            table_name = item['object_name'].strip().upper() # Normaliza para maiúsculas
            column_name = item['column_name'].strip().upper() # Normaliza para maiúsculas
            key = (table_name, column_name)
            lookup[key] = {
                "generated_description": item.get("generated_description"),
                "model_used": item.get("model_used"),
                "generation_timestamp": item.get("generation_timestamp")
            }
        else:
            logger.warning(f"Item de descrição de IA ignorado por falta de chaves ou formato inválido: {item}")
    logger.info(f"Lookup de descrições de IA criado com {len(lookup)} entradas.")
    return lookup

def build_manual_lookup(manual_data):
    """Cria um dicionário de lookup para metadados manuais (colunas e tabelas)."""
    lookup = {"objects": {}, "columns": {}}
    if not isinstance(manual_data, dict):
        logger.warning("Dados manuais não estão no formato esperado (dicionário). Lookup ficará vazio.")
        return lookup

    processed_objects = 0
    processed_cols = 0
    for obj_type_key, objects in manual_data.items():
        if obj_type_key.startswith("_"):
             continue
             
        if not isinstance(objects, dict):
            logger.warning(f"Entrada para '{obj_type_key}' nos dados manuais não é um dicionário. Pulando.")
            continue

        for obj_name_orig, obj_data in objects.items():
            if not isinstance(obj_data, dict):
                logger.warning(f"Dados para '{obj_name_orig}' em '{obj_type_key}' não são um dicionário. Pulando.")
                continue

            obj_name = obj_name_orig.strip().upper()
            obj_desc = obj_data.get('description')
            if obj_desc is not None:
                lookup["objects"][obj_name] = {"business_description": obj_desc}
                processed_objects += 1

            columns_data = obj_data.get("COLUMNS")
            if isinstance(columns_data, dict):
                for col_name_orig, col_data in columns_data.items():
                    if isinstance(col_data, dict):
                        col_name = col_name_orig.strip().upper()
                        key = (obj_name, col_name)
                        lookup["columns"][key] = {
                            "business_description": col_data.get("business_description"),
                            "value_mapping_notes": col_data.get("value_mapping_notes")
                        }
                        processed_cols += 1
                    else:
                        logger.warning(f"Dados da coluna '{col_name_orig}' em '{obj_name_orig}' não são um dicionário. Pulando.")
            # else: Não loga erro se COLUMNS não existir ou for inválido, pode ser normal

    logger.info(f"Lookup manual criado: {processed_objects} descrições de objetos, {processed_cols} entradas de colunas.")
    return lookup

def build_enriched_text(col_data_tech, manual_col_info, ai_col_info):
    """Constrói o texto enriquecido para embedding, priorizando descrições."""

    manual_desc = manual_col_info.get("business_description", "").strip() if manual_col_info else ""
    ai_desc = ai_col_info.get("generated_description", "").strip() if ai_col_info else ""
    tech_desc_raw = col_data_tech.get("description")
    tech_desc = tech_desc_raw.strip() if tech_desc_raw else ""

    best_desc = manual_desc
    if not best_desc:
        best_desc = ai_desc
    # Considerar se deve incluir tech_desc como fallback final?
    # if not best_desc:
    #    best_desc = tech_desc

    col_name = col_data_tech.get('name', '[Coluna Desconhecida]')
    col_type = col_data_tech.get('type', '[Tipo Desconhecido]')
    table_name = col_data_tech.get('__table_name__', '[Tabela Desconhecida]') 
    notes = manual_col_info.get("value_mapping_notes", "").strip() if manual_col_info else ""

    text_parts = [
        f"Tabela: {table_name}",
        f"Coluna: {col_name}",
        f"Tipo: {col_type}",
        f"Descrição Principal: {best_desc if best_desc else 'N/A'}",
        # Opcional: Adicionar descrição técnica sempre para contexto?
        f"Descrição Técnica DB: {tech_desc if tech_desc else 'N/A'}",
    ]
    if notes:
        text_parts.append(f"Notas Adicionais/Mapeamento: {notes}")

    return "\n".join(text_parts)

# --- Função Principal ---

def main(args):
    script_start_time = time.time()
    logger.info("--- Iniciando Script: Merge Consolidado de Metadados para Embeddings --- ")

    # 1. Carregar Arquivos (Usando caminhos dos argumentos)
    logger.info(f"Carregando schema técnico base de: {args.technical_schema}")
    base_schema = load_json(args.technical_schema)
    if base_schema is None:
        logger.critical("Não foi possível carregar o schema base. Abortando.")
        sys.exit(1)

    merged_schema = json.loads(json.dumps(base_schema), object_pairs_hook=OrderedDict)

    logger.info(f"Carregando descrições de IA de: {args.ai_descriptions}")
    ai_data = load_json(args.ai_descriptions)
    ai_lookup = build_ai_lookup(ai_data if ai_data else [])

    logger.info(f"Carregando metadados manuais de: {args.manual_metadata}")
    manual_data = load_json(args.manual_metadata)
    manual_lookup = build_manual_lookup(manual_data if manual_data else {})

    # 2. Lógica de Merge
    logger.info("Iniciando processo de merge...")
    merge_count_ai = 0
    merge_count_manual_obj = 0
    merge_count_manual_col = 0
    processed_columns = 0

    if isinstance(merged_schema, dict):
        for table_name_orig, table_data in merged_schema.items():
            table_name_norm = table_name_orig.strip().upper()
            
            if not isinstance(table_data, dict):
                 logger.warning(f"Entrada para '{table_name_orig}' no schema base não é um dicionário. Pulando.")
                 continue

            manual_obj_info = manual_lookup.get("objects", {}).get(table_name_norm)
            if manual_obj_info:
                 table_data['business_description'] = manual_obj_info.get('business_description')
                 merge_count_manual_obj += 1
                 logger.debug(f"Descrição manual do objeto mesclada para {table_name_orig}")
            elif 'business_description' not in table_data:
                 table_data['business_description'] = None
                 
            if 'columns' in table_data and isinstance(table_data['columns'], list):
                for column_data in table_data['columns']:
                    processed_columns += 1
                    if isinstance(column_data, dict) and 'name' in column_data:
                        col_name_orig = column_data['name']
                        col_name_norm = col_name_orig.strip().upper()
                        key = (table_name_norm, col_name_norm)
                        
                        column_data['__table_name__'] = table_name_orig 

                        manual_col_info = manual_lookup.get("columns", {}).get(key)
                        ai_col_info = ai_lookup.get(key)

                        if manual_col_info:
                            column_data['business_description'] = manual_col_info.get('business_description')
                            column_data['value_mapping_notes'] = manual_col_info.get('value_mapping_notes')
                            merge_count_manual_col += 1
                            logger.debug(f"Dados manuais da coluna mesclados para {table_name_orig}.{col_name_orig}")
                        else:
                             if 'business_description' not in column_data: column_data['business_description'] = None
                             if 'value_mapping_notes' not in column_data: column_data['value_mapping_notes'] = None

                        if ai_col_info:
                            column_data['ai_generated_description'] = ai_col_info.get('generated_description')
                            column_data['ai_model_used'] = ai_col_info.get('model_used')
                            column_data['ai_generation_timestamp'] = ai_col_info.get('generation_timestamp')
                            merge_count_ai += 1
                            logger.debug(f"Dados IA mesclados para {table_name_orig}.{col_name_orig}")
                        else:
                             if 'ai_generated_description' not in column_data: column_data['ai_generated_description'] = None
                             if 'ai_model_used' not in column_data: column_data['ai_model_used'] = None
                             if 'ai_generation_timestamp' not in column_data: column_data['ai_generation_timestamp'] = None

                        column_data['text_for_embedding'] = build_enriched_text(column_data, manual_col_info, ai_col_info)
                        
                        del column_data['__table_name__']

                    else:
                        logger.warning(f"Coluna em '{table_name_orig}' ignorada no merge por falta de nome ou formato inválido: {column_data}")
            # else: Não loga erro se não houver 'columns', pode ser normal
    else:
        logger.error("Schema base não é um dicionário. Merge não pode ser realizado.")
        sys.exit(1)

    logger.info(f"Merge concluído. Total de colunas processadas: {processed_columns}.")
    logger.info(f"Descrições manuais de objeto mescladas: {merge_count_manual_obj}")
    logger.info(f"Metadados manuais de coluna mesclados: {merge_count_manual_col}")
    logger.info(f"Descrições de IA mescladas: {merge_count_ai}")

    # 3. Salvar Resultado
    logger.info(f"Salvando schema mesclado e enriquecido em: {args.output}")
    save_success = save_json(merged_schema, args.output)
    if not save_success:
        logger.error("Falha ao salvar o schema mesclado.")
        # Não sair imediatamente, imprimir resumo mesmo assim

    script_duration = time.time() - script_start_time
    logger.info(f"--- Script de Merge Consolidado Concluído em {script_duration:.2f} segundos --- ")

    # --- NOVO: Imprimir Resumo no Console --- #
    print("\n--- Resumo do Merge --- ")
    print(f"Schema Base Lido: {args.technical_schema}")
    print(f"Metadados Manuais Lidos: {args.manual_metadata}")
    print(f"Descrições IA Lidas: {args.ai_descriptions if args.ai_descriptions else 'Nenhuma'}")
    print(f"Arquivo de Saída: {args.output}")
    print("-" * 20)
    print(f"Colunas Processadas: {processed_columns}")
    print(f"Descrições Obj. Manuais Mescladas: {merge_count_manual_obj}")
    print(f"Metadados Col. Manuais Mesclados: {merge_count_manual_col}")
    print(f"Descrições IA Mescladas: {merge_count_ai}")
    print("-" * 20)
    print(f"Status Salvamento: {'Sucesso' if save_success else 'FALHA'}")
    print(f"Duração Total: {script_duration:.2f}s")
    print("---------------------\n")

    # Sair com erro se salvamento falhou
    if not save_success:
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combina schema técnico, metadados manuais e descrições AI em um único JSON enriquecido para embeddings.")
    parser.add_argument(
        "--technical_schema",
        default=TECHNICAL_SCHEMA_FILE,
        help=f"Caminho para o schema técnico base (JSON). Padrão: {TECHNICAL_SCHEMA_FILE}"
    )
    parser.add_argument(
        "--manual_metadata",
        default=METADATA_FILE,
        help=f"Caminho para os metadados manuais (JSON, estrutura aninhada). Padrão: {METADATA_FILE}"
    )
    parser.add_argument(
        "--ai_descriptions",
        default=AI_DESCRIPTIONS_FILE,
        help=f"Caminho para as descrições geradas por IA (JSON lista). Nenhum para ignorar. Padrão: {AI_DESCRIPTIONS_FILE}"
    )
    parser.add_argument(
        "-o", "--output",
        default=MERGED_SCHEMA_FOR_EMBEDDINGS_FILE,
        help=f"Caminho para salvar o JSON final mesclado e enriquecido. Padrão: {MERGED_SCHEMA_FOR_EMBEDDINGS_FILE}"
    )

    args = parser.parse_args()

    ai_file_path = Path(args.ai_descriptions)
    if not ai_file_path.exists():
        logger.warning(f"Arquivo de descrições AI especificado ({ai_file_path}) não encontrado. Continuando sem dados de IA.")
        args.ai_descriptions = None

    main(args) 