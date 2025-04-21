import os
import sys
import json
import logging
import argparse
from pathlib import Path
from collections import defaultdict

# --- Adiciona o diretório raiz ao sys.path --- #
script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- FIM Adição ao sys.path --- #

# Importar configurações e helpers necessários
from src.core.logging_config import setup_logging
from src.utils.json_helpers import load_json, save_json
from src.core.config import (
    MERGED_SCHEMA_WITH_AI_FILE, # Entrada Base (Técnico + AI)
    MERGED_SCHEMA_FOR_EMBEDDINGS_FILE, # Entrada Fonte (Manual - geralmente é este)
    MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE # Saída (Técnico + AI + Manual + Amostras)
)

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Função Principal --- #

def merge_manual_descriptions(base_schema_path, manual_source_path, output_path):
    """
    Mescla descrições manuais (business_description, value_mapping_notes)
    de um arquivo fonte em um arquivo de schema base.

    Args:
        base_schema_path (str ou Path): Caminho para o JSON do schema base (já com dados técnicos, AI, etc.).
        manual_source_path (str ou Path): Caminho para o JSON contendo as descrições manuais a serem mescladas.
        output_path (str ou Path): Caminho para salvar o novo JSON com todos os dados mesclados.
    """
    logger.info("--- Iniciando Mesclagem de Descrições Manuais no Schema Base ---")
    logger.info(f"Schema Base de Entrada: {base_schema_path}")
    logger.info(f"Fonte Manual de Entrada: {manual_source_path}")
    logger.info(f"Arquivo de Saída Final: {output_path}")

    # 1. Carregar Schema Base
    logger.info("Carregando schema base...")
    base_schema = load_json(base_schema_path)
    if not base_schema or not isinstance(base_schema, dict):
        logger.critical(f"Falha ao carregar ou schema base inválido em: {base_schema_path}")
        return False
    logger.info(f"Schema base carregado.")

    # 2. Carregar Schema Fonte (com dados manuais)
    logger.info("Carregando schema fonte (manual)...")
    manual_source_schema = load_json(manual_source_path)
    if not manual_source_schema or not isinstance(manual_source_schema, dict):
        logger.critical(f"Falha ao carregar ou schema fonte inválido em: {manual_source_path}")
        return False
    logger.info(f"Schema fonte carregado.")

    # 3. Criar um lookup para acesso rápido aos dados manuais
    manual_lookup = {}
    skipped_objects = 0
    processed_cols_count = 0
    logger.info("Criando lookup para dados manuais...")
    for obj_name, obj_data in manual_source_schema.items():
        if obj_name == '_analysis' or 'columns' not in obj_data or not isinstance(obj_data['columns'], list):
            skipped_objects += 1
            continue
        obj_name_strip = obj_name.strip()
        for col_data in obj_data['columns']:
            try:
                if 'name' in col_data:
                    col_name_strip = col_data['name'].strip()
                    lookup_key = (obj_name_strip, col_name_strip)
                    # Armazena apenas os campos necessários para evitar memória excessiva
                    manual_lookup[lookup_key] = {
                        'business_description': col_data.get('business_description'),
                        'value_mapping_notes': col_data.get('value_mapping_notes'),
                        'sample_values': col_data.get('sample_values')
                    }
                    processed_cols_count += 1
            except Exception as e:
                logger.warning(f"Erro ao processar coluna {obj_name_strip}.{col_data.get('name', 'N/A')} no schema fonte: {e}")

    logger.info(f"Lookup criado para {processed_cols_count} colunas de {len(manual_source_schema) - skipped_objects - 1} objetos (ignorando '_analysis' e {skipped_objects} objetos malformados/sem colunas).")

    # 4. Iterar sobre o schema base e mesclar dados manuais
    merged_count = 0
    not_found_count = 0
    logger.info("Iniciando processo de mesclagem dos dados manuais...")
    for obj_name, obj_data in base_schema.items():
        if obj_name == '_analysis' or 'columns' not in obj_data or not isinstance(obj_data['columns'], list):
            continue

        obj_name_strip = obj_name.strip()
        for col_data in obj_data['columns']:
            try:
                if 'name' not in col_data:
                    continue

                col_name_strip = col_data['name'].strip()
                lookup_key = (obj_name_strip, col_name_strip)

                if lookup_key in manual_lookup:
                    manual_data = manual_lookup[lookup_key]
                    # Atualiza os campos no schema base
                    # Só atualiza se o valor no manual_lookup não for None (para não sobrescrever com None por acidente)
                    if manual_data.get('business_description') is not None:
                         col_data['business_description'] = manual_data['business_description']
                    if manual_data.get('value_mapping_notes') is not None:
                         col_data['value_mapping_notes'] = manual_data['value_mapping_notes']
                    if manual_data.get('sample_values') is not None:
                         col_data['sample_values'] = manual_data['sample_values']
                    merged_count += 1
                else:
                    # Não encontrou correspondência no schema fonte manual
                    not_found_count += 1
                    # Garante que os campos existam como None se já não existirem (embora devam do schema técnico)
                    if 'business_description' not in col_data:
                         col_data['business_description'] = None
                    if 'value_mapping_notes' not in col_data:
                         col_data['value_mapping_notes'] = None
                    if 'sample_values' not in col_data:
                        col_data['sample_values'] = None

            except Exception as e:
                logger.error(f"Erro ao mesclar dados manuais para coluna '{obj_name_strip}.{col_data.get('name', 'N/A')}': {e}", exc_info=True)

    logger.info(f"Mesclagem manual concluída. {merged_count} colunas tiveram dados manuais encontrados e potencialmente atualizados.")
    if not_found_count > 0:
        logger.warning(f"{not_found_count} colunas do schema base não encontraram correspondência no schema fonte manual.")

    # 5. Salvar o novo schema final mesclado
    logger.info(f"Salvando schema final mesclado em {output_path}...")
    success = save_json(base_schema, output_path)
    if success:
        logger.info("Schema final salvo com sucesso.")
    else:
        logger.error("Falha ao salvar o schema final.")
        return False

    logger.info("--- Mesclagem Final Concluída ---")
    return True

# --- Ponto de Entrada do Script --- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mescla descrições manuais (business_description, value_mapping_notes) de um arquivo fonte para um schema base.")

    parser.add_argument(
        "--base-schema",
        default=MERGED_SCHEMA_WITH_AI_FILE,
        help=f"Caminho para o arquivo JSON do schema base (com dados técnicos, AI, etc.). Padrão: {MERGED_SCHEMA_WITH_AI_FILE}"
    )
    parser.add_argument(
        "--manual-source",
        default=MERGED_SCHEMA_FOR_EMBEDDINGS_FILE, # Assumindo que este é o arquivo com os dados manuais mais recentes
        help=f"Caminho para o arquivo JSON fonte das descrições manuais. Padrão: {MERGED_SCHEMA_FOR_EMBEDDINGS_FILE}"
    )
    parser.add_argument(
        "-o", "--output",
        default=MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE,
        help=f"Caminho para o novo arquivo JSON de saída final mesclado. Padrão: {MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE}"
    )

    args = parser.parse_args()

    # Executa a função principal
    merge_manual_descriptions(
        base_schema_path=args.base_schema,
        manual_source_path=args.manual_source,
        output_path=args.output
    ) 