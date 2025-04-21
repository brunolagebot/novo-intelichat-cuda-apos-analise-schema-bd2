import os
import sys
import json
import logging
import argparse
from pathlib import Path
from collections import defaultdict

# --- Adiciona o diretório raiz ao sys.path --- #
# Assume que este script está em project_root/scripts/recovery/
script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parents[1] # Sobe dois níveis (recovery -> scripts -> project_root)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- FIM Adição ao sys.path --- #

# Importar configurações e helpers necessários
from src.core.logging_config import setup_logging
from src.utils.json_helpers import load_json, save_json
# Nota: Não usamos config.py diretamente aqui para manter o script de recuperação independente,
# mas definimos os caminhos padrão via argparse.

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Constantes Padrão para este Script ---
# Arquivo fonte onde os dados manuais (provavelmente) estão
DEFAULT_SOURCE_FILE = "data/processed/schema_enriched_for_embedding.json"
# Novo arquivo mestre que será criado APENAS com dados manuais
DEFAULT_OUTPUT_MASTER_FILE = "data/metadata/manual_metadata_master.json"


def extract_manual_data(source_path, output_path):
    """
    Extrai dados manuais de coluna (business_description, value_mapping_notes)
    de um arquivo JSON fonte e salva em um novo arquivo mestre.

    IMPORTANTE: Este script cria a estrutura do arquivo mestre, mas preenche
    APENAS os dados manuais de COLUNA existentes na fonte. Os dados manuais
    de OBJETO (tabela/view) precisarão ser adicionados manualmente depois.

    Args:
        source_path (str ou Path): Caminho para o arquivo JSON fonte.
        output_path (str ou Path): Caminho para o novo arquivo JSON mestre a ser criado.
    """
    logger.info("--- Iniciando Extração de Dados Manuais (Recuperação) ---")
    logger.info(f"Arquivo Fonte: {source_path}")
    logger.info(f"Arquivo Mestre de Saída: {output_path}")

    # 1. Carregar Schema Fonte
    logger.info("Carregando schema fonte...")
    source_schema = load_json(source_path)
    if not source_schema or not isinstance(source_schema, dict):
        logger.critical(f"Falha ao carregar ou schema fonte inválido em: {source_path}")
        return False
    logger.info("Schema fonte carregado.")

    # 2. Preparar a estrutura do novo arquivo mestre
    manual_master_data = {}
    extracted_col_count = 0
    processed_object_count = 0

    logger.info("Processando schema fonte e extraindo dados manuais de colunas...")
    for obj_name, obj_data in source_schema.items():
        # Ignora chaves especiais como '_analysis'
        if obj_name.startswith('_') or not isinstance(obj_data, dict):
            continue

        processed_object_count += 1
        obj_name_strip = obj_name.strip()
        manual_master_data[obj_name_strip] = {
            # Inicializa campos de objeto como None - PREENCHER MANUALMENTE DEPOIS
            "object_business_description": None,
            "object_value_mapping_notes": None,
            "columns": {} # Dicionário para as colunas deste objeto
        }

        if 'columns' in obj_data and isinstance(obj_data['columns'], list):
            for col_data in obj_data['columns']:
                try:
                    if 'name' in col_data:
                        col_name_strip = col_data['name'].strip()
                        col_manual_info = {}
                        has_manual_info = False

                        # Extrai apenas os campos manuais relevantes se existirem
                        if col_data.get('business_description') is not None:
                            col_manual_info['business_description'] = col_data['business_description']
                            has_manual_info = True
                        if col_data.get('value_mapping_notes') is not None:
                            col_manual_info['value_mapping_notes'] = col_data['value_mapping_notes']
                            has_manual_info = True

                        # Adiciona ao dicionário 'columns' do objeto mestre apenas se houver info manual
                        if has_manual_info:
                            manual_master_data[obj_name_strip]['columns'][col_name_strip] = col_manual_info
                            extracted_col_count += 1

                except Exception as e:
                    logger.warning(f"Erro ao processar coluna {obj_name_strip}.{col_data.get('name', 'N/A')} no schema fonte: {e}")
        else:
             logger.warning(f"Objeto '{obj_name_strip}' no schema fonte não tem uma lista de 'columns' ou formato inválido.")


    logger.info(f"Extração concluída. {extracted_col_count} colunas com dados manuais encontradas em {processed_object_count} objetos.")
    if extracted_col_count == 0:
         logger.warning("Nenhuma informação manual de coluna foi encontrada no arquivo fonte.")


    # 3. Salvar o novo arquivo mestre
    logger.info(f"Salvando novo arquivo mestre manual em {output_path}...")
    success = save_json(manual_master_data, output_path)
    if success:
        logger.info("Arquivo mestre manual salvo com sucesso.")
        logger.warning("Lembre-se: Edite este arquivo para adicionar descrições/notas de nível de objeto (tabela/view) manualmente.")
    else:
        logger.error("Falha ao salvar o arquivo mestre manual.")
        return False

    logger.info("--- Extração de Recuperação Concluída ---")
    return True


# --- Ponto de Entrada do Script --- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrai dados manuais de COLUNA (business_description, value_mapping_notes) de um JSON fonte para criar um novo arquivo mestre manual.")

    parser.add_argument(
        "--source",
        default=DEFAULT_SOURCE_FILE,
        help=f"Caminho para o arquivo JSON fonte contendo os dados manuais (coluna). Padrão: {DEFAULT_SOURCE_FILE}"
    )
    parser.add_argument(
        "-o", "--output",
        default=DEFAULT_OUTPUT_MASTER_FILE,
        help=f"Caminho para o novo arquivo JSON mestre manual a ser criado. Padrão: {DEFAULT_OUTPUT_MASTER_FILE}"
    )

    args = parser.parse_args()

    # Executa a função principal
    extract_manual_data(
        source_path=args.source,
        output_path=args.output
    ) 