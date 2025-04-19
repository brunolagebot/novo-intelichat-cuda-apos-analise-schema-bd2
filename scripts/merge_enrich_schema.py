#!/usr/bin/env python
# coding: utf-8

import os
import json
import argparse
import logging
from collections import OrderedDict
import time
import sys

# Adiciona o diretório raiz ao sys.path para permitir importações de src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.json_helpers import load_json, save_json
from src.core.log_utils import setup_logging
from src.core.config import METADATA_FILE, TECHNICAL_SCHEMA_FILE # Importar caminhos

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Constantes e Configurações Padrão --- 
DEFAULT_TECH_SCHEMA_PATH = "data/enhanced_technical_schema.json"
DEFAULT_MANUAL_METADATA_PATH = "data/schema_metadata.json"
DEFAULT_AI_DESC_PATH = "data/ai_generated_descriptions_openai_35turbo.json" # Ou o arquivo AI relevante
DEFAULT_OUTPUT_ENRICHED_PATH = "data/processed/schema_enriched_for_embedding.json"

def build_enriched_text(col_data_tech, col_data_manual, col_data_ai):
    """Constrói o texto enriquecido para embedding, priorizando descrições."""
    
    # 1. Prioridade: Descrição Manual
    manual_desc = col_data_manual.get("description", "").strip()
    # 2. Fallback: Descrição IA (se fornecida e manual vazia)
    ai_desc = col_data_ai.get("generated_description", "").strip() if col_data_ai else ""
    # 3. Fallback: Descrição Técnica
    tech_desc_raw = col_data_tech.get("description") 
    tech_desc = tech_desc_raw.strip() if tech_desc_raw else ""

    best_desc = manual_desc
    if not best_desc:
        best_desc = ai_desc
    if not best_desc:
        best_desc = tech_desc

    # Combinar com outras informações relevantes
    col_name = col_data_tech.get('name', '[Coluna Desconhecida]')
    col_type = col_data_tech.get('type', '[Tipo Desconhecido]')
    table_name = col_data_tech.get('table_name', '[Tabela Desconhecida]') # Adicionar nome da tabela ao col_data_tech
    notes = col_data_manual.get("value_mapping_notes", "").strip()
    
    # Estrutura do texto (ajuste conforme necessário)
    text_parts = [
        f"Tabela: {table_name}",
        f"Coluna: {col_name}",
        f"Tipo: {col_type}",
        f"Descrição Principal: {best_desc if best_desc else 'N/A'}",
    ]
    if notes:
        text_parts.append(f"Notas Adicionais: {notes}")

    return "\n".join(text_parts)

def main(technical_schema_path=TECHNICAL_SCHEMA_FILE, manual_metadata_path=METADATA_FILE, output_path=TECHNICAL_SCHEMA_FILE): # Usar variáveis importadas como default
    script_start_time = time.time()
    logger.info("--- Iniciando Script: Merge e Enriquecimento de Schema para Embedding --- ")

    # --- Carregar Dados --- 
    logger.info(f"Carregando Schema Técnico Detalhado de: {technical_schema_path}")
    tech_schema = load_json(technical_schema_path)
    if not tech_schema:
        logger.critical("Falha ao carregar schema técnico. Abortando.")
        return

    logger.info(f"Carregando Metadados Manuais de: {manual_metadata_path}")
    manual_metadata = load_json(manual_metadata_path)
    if manual_metadata is None: # Pode ser vazio, mas não None em caso de erro
        logger.warning(f"Falha ao carregar {manual_metadata_path}. Procedendo sem metadados manuais.")
        manual_metadata = {} # Trata como vazio

    ai_descriptions_data = None
    if args.ai_descriptions:
        logger.info(f"Carregando Descrições IA de: {args.ai_descriptions}")
        ai_descriptions_list = load_json(args.ai_descriptions)
        if isinstance(ai_descriptions_list, list):
            ai_descriptions_data = {
                (item['object_name'], item['column_name']): item 
                for item in ai_descriptions_list 
                if 'object_name' in item and 'column_name' in item
            }
            logger.info(f"{len(ai_descriptions_data)} descrições IA carregadas e mapeadas.")
        else:
            logger.warning(f"Arquivo de descrições IA ({args.ai_descriptions}) não é uma lista válida. Ignorando descrições IA.")
            ai_descriptions_data = {}
    else:
        logger.info("Nenhum arquivo de descrição IA fornecido.")
        ai_descriptions_data = {}

    # --- Processar e Preparar Dados de Saída --- 

    output_data = OrderedDict() # Estrutura final para o JSON

    logger.info("Iterando sobre schema técnico e construindo dados enriquecidos...")
    total_cols = 0
    cols_processed = 0
    for obj_name, obj_data_tech in tech_schema.items():
        if not isinstance(obj_data_tech, dict) or 'columns' not in obj_data_tech:
            continue

        obj_type = obj_data_tech.get('object_type', 'UNKNOWN')
        metadata_key_type = obj_type + "S" if obj_type != 'UNKNOWN' else 'UNKNOWN'
        obj_data_manual = manual_metadata.get(metadata_key_type, {}).get(obj_name, {})

        output_data[obj_name] = OrderedDict(
            [(k, v) for k, v in obj_data_tech.items() if k != 'columns'] 
        )
        output_data[obj_name]['columns'] = []

        for col_data_tech in obj_data_tech.get('columns', []):
            total_cols += 1
            col_name = col_data_tech.get('name')
            if not col_name:
                logger.warning(f"Coluna sem nome encontrada em {obj_name}. Pulando.")
                continue

            col_data_tech['table_name'] = obj_name 
            col_data_manual = obj_data_manual.get("COLUMNS", {}).get(col_name, {})
            col_data_ai = ai_descriptions_data.get((obj_name, col_name), {})

            # Construir texto enriquecido
            enriched_text = build_enriched_text(col_data_tech, col_data_manual, col_data_ai)

            # Montar dados da coluna para o JSON de saída
            output_col_data = OrderedDict(
                [(k, v) for k, v in col_data_tech.items()] # Começa com dados técnicos
            )
            output_col_data['business_description'] = col_data_manual.get('description', None)
            output_col_data['value_mapping_notes'] = col_data_manual.get('value_mapping_notes', None)
            output_col_data['ai_generated_description'] = col_data_ai.get('generated_description', None)
            output_col_data['ai_model_used'] = col_data_ai.get('model_used', None)
            output_col_data['ai_generation_timestamp'] = col_data_ai.get('generation_timestamp', None)
            output_col_data['text_for_embedding'] = enriched_text # Guarda o texto a ser usado

            output_data[obj_name]['columns'].append(output_col_data)
            cols_processed += 1
            if cols_processed % 500 == 0:
                logger.info(f"Processadas {cols_processed}/{total_cols} colunas...")

    logger.info(f"Total de {cols_processed} colunas preparadas no arquivo de saída.")

    # --- Salvar Arquivo JSON Enriquecido --- 
    logger.info(f"Salvando JSON enriquecido em: {output_path}")
    if save_json(output_data, output_path):
        logger.info("JSON enriquecido salvo com sucesso.")
    else:
        logger.error("Falha ao salvar JSON enriquecido.")

    script_duration = time.time() - script_start_time
    logger.info(f"--- Script de Merge e Enriquecimento Concluído em {script_duration:.2f} segundos --- ")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Combina schemas, enriquece dados textuais e salva um JSON pronto para embedding.")
    parser.add_argument("--technical_schema", default=TECHNICAL_SCHEMA_FILE, help=f"Caminho para o schema técnico detalhado (JSON). Padrão: {TECHNICAL_SCHEMA_FILE}")
    parser.add_argument("--manual_metadata", default=METADATA_FILE, help=f"Caminho para os metadados manuais (JSON). Padrão: {METADATA_FILE}")
    parser.add_argument("--ai_descriptions", default=DEFAULT_AI_DESC_PATH, help=f"Caminho para as descrições geradas por IA (JSON lista). Opcional. Padrão: {DEFAULT_AI_DESC_PATH}")
    parser.add_argument("--output_enriched_json", default=TECHNICAL_SCHEMA_FILE, help=f"Caminho para salvar o JSON final enriquecido. Padrão: {TECHNICAL_SCHEMA_FILE}")

    args = parser.parse_args()

    # Cria diretório de saída JSON se não existir
    output_dir_json = os.path.dirname(args.output_enriched_json)
    if output_dir_json:
        os.makedirs(output_dir_json, exist_ok=True)

    main(args.technical_schema, args.manual_metadata, args.output_enriched_json) 