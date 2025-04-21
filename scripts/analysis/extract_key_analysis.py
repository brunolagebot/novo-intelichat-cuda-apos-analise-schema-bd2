import os
import sys
import json
import logging
import argparse
from pathlib import Path

# --- Adiciona o diretório raiz ao sys.path --- #
# Assume que este script está em project_root/scripts/analysis/
script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parents[1] # Sobe dois níveis (analysis -> scripts -> project_root)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- FIM Adição ao sys.path --- #

# Importar configurações e helpers necessários
from src.core.logging_config import setup_logging
from src.utils.json_helpers import load_json, save_json
from src.core.config import (
    TECHNICAL_SCHEMA_FILE,         # Arquivo CORE de entrada
    KEY_ANALYSIS_RESULTS_FILE      # Arquivo de saída específico para análise de chaves
)

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)


def extract_key_analysis(source_schema_path, output_analysis_path):
    """
    Extrai a seção de análise de chaves ('_analysis') de um arquivo de
    schema técnico completo e a salva em um arquivo separado.

    Args:
        source_schema_path (str ou Path): Caminho para o JSON do schema técnico completo.
        output_analysis_path (str ou Path): Caminho para salvar o JSON contendo apenas a análise de chaves.
    """
    logger.info("--- Iniciando Extração da Análise de Chaves do Schema Técnico ---")
    logger.info(f"Schema Técnico Fonte (CORE): {source_schema_path}")
    logger.info(f"Arquivo de Saída (Análise de Chaves): {output_analysis_path}")

    # 1. Carregar Schema Técnico Completo
    logger.info("Carregando schema técnico completo...")
    technical_schema = load_json(source_schema_path)
    if not technical_schema or not isinstance(technical_schema, dict):
        logger.critical(f"Falha ao carregar ou schema técnico inválido em: {source_schema_path}")
        return False
    logger.info("Schema técnico carregado.")

    # 2. Extrair a seção '_analysis'
    logger.info("Extraindo a seção '_analysis'...")
    key_analysis_data = technical_schema.get('_analysis')

    if key_analysis_data is None:
        logger.error(f"A chave '_analysis' não foi encontrada no arquivo: {source_schema_path}")
        logger.error("Verifique se o script 'extract_technical_schema.py' foi executado corretamente.")
        return False
    elif not isinstance(key_analysis_data, dict):
         logger.error(f"O conteúdo da chave '_analysis' não é um dicionário válido em: {source_schema_path}")
         return False

    logger.info("Seção '_analysis' extraída com sucesso.")

    # 3. Salvar os dados extraídos
    logger.info(f"Salvando dados da análise de chaves em {output_analysis_path}...")
    success = save_json(key_analysis_data, output_analysis_path)
    if success:
        logger.info("Dados da análise de chaves salvos com sucesso.")
    else:
        logger.error("Falha ao salvar os dados da análise de chaves.")
        return False

    logger.info("--- Extração da Análise de Chaves Concluída ---")
    return True

# --- Ponto de Entrada do Script --- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extrai a seção '_analysis' (chaves, constraints) de um arquivo de schema técnico completo.")

    parser.add_argument(
        "--source-schema",
        default=TECHNICAL_SCHEMA_FILE,
        help=f"Caminho para o arquivo JSON do schema técnico completo (CORE). Padrão: {TECHNICAL_SCHEMA_FILE}"
    )
    parser.add_argument(
        "-o", "--output",
        default=KEY_ANALYSIS_RESULTS_FILE,
        help=f"Caminho para o novo arquivo JSON de saída contendo apenas a análise de chaves. Padrão: {KEY_ANALYSIS_RESULTS_FILE}"
    )

    args = parser.parse_args()

    # Executa a função principal
    extract_key_analysis(
        source_schema_path=args.source_schema,
        output_analysis_path=args.output
    ) 