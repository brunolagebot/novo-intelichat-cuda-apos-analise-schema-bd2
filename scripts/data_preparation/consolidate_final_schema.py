import os
import sys
import json
import logging
import argparse
from pathlib import Path
from collections import defaultdict

# --- Adiciona o diretório raiz ao sys.path --- #
# Assume que este script está em project_root/scripts/data_preparation/
script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- FIM Adição ao sys.path --- #

# Importar configurações e helpers necessários
from src.core.logging_config import setup_logging
from src.utils.json_helpers import load_json, save_json
from src.core.config import (
    MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE, # Input Base (Técnico + AI Col + Manual + Samples)
    AI_OBJECT_DESCRIPTIONS_FILE,           # Input Descrições AI de Objetos
    FINAL_CONSOLIDATED_SCHEMA_FILE         # Output Final
)

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Função Principal --- #

def consolidate_schema(base_schema_path, object_ai_path, output_path):
    """
    Consolida as descrições AI de objetos no schema base quase final.

    Args:
        base_schema_path (str ou Path): Caminho para o JSON do schema base
                                         (com dados técnicos, AI colunas, manual, samples).
        object_ai_path (str ou Path): Caminho para o JSON das descrições AI de objetos.
        output_path (str ou Path): Caminho para salvar o JSON final consolidado.
    """
    logger.info("--- Iniciando Consolidação Final do Schema --- ")
    logger.info(f"Schema Base de Entrada: {base_schema_path}")
    logger.info(f"Descrições AI Objeto de Entrada: {object_ai_path}")
    logger.info(f"Arquivo de Saída Final: {output_path}")

    # 1. Carregar Schema Base
    logger.info("Carregando schema base...")
    base_schema = load_json(base_schema_path)
    if not base_schema or not isinstance(base_schema, dict):
        logger.critical(f"Falha ao carregar ou schema base inválido em: {base_schema_path}")
        return False
    logger.info("Schema base carregado.")

    # 2. Carregar Descrições AI de Objetos
    logger.info("Carregando descrições AI de objetos...")
    object_ai_descriptions = load_json(object_ai_path, default_value=[]) # Espera uma lista
    if not isinstance(object_ai_descriptions, list):
        logger.error(f"Arquivo de descrições AI de objetos não contém uma lista válida: {object_ai_path}")
        # Continuar mesmo assim, mas logar aviso
        object_ai_descriptions = []
    logger.info(f"{len(object_ai_descriptions)} descrições AI de objeto carregadas.")

    # 3. Criar um lookup para acesso rápido às descrições AI de objetos
    ai_object_lookup = {}
    for item in object_ai_descriptions:
        try:
            # Verifica se é uma descrição de objeto e tem nome
            if item.get('object_name') and item.get('column_name') is None:
                obj_name = item['object_name'].strip()
                # Armazena os dados relevantes (descrição, modelo, timestamp)
                ai_object_lookup[obj_name] = {
                    'generated_description': item.get('generated_description'),
                    'model_used': item.get('model_used'),
                    'generation_timestamp': item.get('generation_timestamp')
                }
        except Exception as e:
             logger.warning(f"Erro processando item de descrição AI de objeto: {item} - {e}")
    logger.info(f"Criado lookup para {len(ai_object_lookup)} descrições AI de objeto válidas.")

    # 4. Iterar sobre o schema base e mesclar descrições AI de objeto
    merged_count = 0
    logger.info("Iniciando processo de mesclagem das descrições AI de objeto...")

    for obj_name, obj_data in base_schema.items():
        # Ignorar chaves especiais como '_analysis'
        if obj_name.startswith('_') or not isinstance(obj_data, dict):
            continue

        # Buscar descrição AI para este objeto no lookup
        ai_desc_data = ai_object_lookup.get(obj_name.strip())

        if ai_desc_data:
            # Adiciona/Atualiza os campos no nível do objeto
            obj_data['object_ai_generated_description'] = ai_desc_data.get('generated_description')
            obj_data['object_ai_model_used'] = ai_desc_data.get('model_used')
            obj_data['object_ai_generation_timestamp'] = ai_desc_data.get('generation_timestamp')
            merged_count += 1
        else:
            # Garante que os campos existam como None se não foram encontrados
            if 'object_ai_generated_description' not in obj_data:
                 obj_data['object_ai_generated_description'] = None
            if 'object_ai_model_used' not in obj_data:
                 obj_data['object_ai_model_used'] = None
            if 'object_ai_generation_timestamp' not in obj_data:
                 obj_data['object_ai_generation_timestamp'] = None

    logger.info(f"Consolidação concluída. {merged_count} objetos tiveram descrições AI incorporadas.")
    if merged_count < len(ai_object_lookup):
        # Encontra quais descrições AI não foram usadas (pode indicar nomes inconsistentes)
        unmerged_objects = set(ai_object_lookup.keys()) - set(k.strip() for k in base_schema.keys() if not k.startswith('_'))
        if unmerged_objects:
            logger.warning(f"Atenção: {len(unmerged_objects)} descrições AI de objeto não encontraram correspondência no schema base. Exemplos: {list(unmerged_objects)[:5]}")


    # 5. Salvar o schema final consolidado
    logger.info(f"Salvando schema final consolidado em {output_path}...")
    success = save_json(base_schema, output_path)
    if success:
        logger.info("Schema final salvo com sucesso.")
    else:
        logger.error("Falha ao salvar o schema final consolidado.")
        return False

    logger.info("--- Consolidação Final Concluída ---")
    return True

# --- Ponto de Entrada do Script --- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consolida descrições AI de objetos em um arquivo de schema base.")

    parser.add_argument(
        "--base-schema",
        default=MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE,
        help=f"Caminho para o arquivo JSON do schema base (com técnico, AI col, manual, samples). Padrão: {MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE}"
    )
    parser.add_argument(
        "--object-ai-descriptions",
        default=AI_OBJECT_DESCRIPTIONS_FILE,
        help=f"Caminho para o arquivo JSON das descrições AI de objetos. Padrão: {AI_OBJECT_DESCRIPTIONS_FILE}"
    )
    parser.add_argument(
        "-o", "--output",
        default=FINAL_CONSOLIDATED_SCHEMA_FILE,
        help=f"Caminho para o novo arquivo JSON de saída final consolidado. Padrão: {FINAL_CONSOLIDATED_SCHEMA_FILE}"
    )

    args = parser.parse_args()

    # Executa a função principal
    consolidate_schema(
        base_schema_path=args.base_schema,
        object_ai_path=args.object_ai_descriptions,
        output_path=args.output
    ) 