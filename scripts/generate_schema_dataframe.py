import pandas as pd
import json
import os
import argparse
import logging
import sys

# Adiciona o diretório raiz ao sys.path para encontrar o módulo core
script_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(script_dir, '..'))
sys.path.insert(0, project_root)

from core.dataframe_generator import generate_schema_dataframe
from core.utils import load_json_safe # Reutilizando load_json_safe se existir em utils
                                       # Se não existir, precisaria ser copiado/adaptado aqui ou no generator

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_INPUT_FILE = os.path.join(project_root, 'data', 'combined_schema_details.json')
DEFAULT_OUTPUT_FILE = os.path.join(project_root, 'data', 'schema_dataframe.csv')

def main():
    # Usa aspas simples para a descrição e formata argumentos
    parser = argparse.ArgumentParser(
        description='Gera um DataFrame CSV achatado a partir do schema JSON combinado.'
    )
    parser.add_argument(
        "-i", "--input",
        default=DEFAULT_INPUT_FILE,
        help=f"Caminho para o arquivo JSON de entrada (padrão: {DEFAULT_INPUT_FILE})"
    )
    parser.add_argument(
        "-o", "--output",
        default=DEFAULT_OUTPUT_FILE,
        help=f"Caminho para o arquivo CSV de saída (padrão: {DEFAULT_OUTPUT_FILE})"
    )
    # Poderia adicionar argumento para formato de saída (csv, pkl, etc.)
    args = parser.parse_args()
    
    input_file = args.input
    output_file = args.output

    logger.info(f"Carregando schema combinado de: {input_file}")
    # Usando load_json_safe (assumindo que está em core.utils)
    # Se não, adaptar:
    # schema_data = None
    # try:
    #     with open(input_file, 'r', encoding='utf-8') as f:
    #         schema_data = json.load(f)
    # except FileNotFoundError:
    #     logger.error(f"Erro: Arquivo de entrada não encontrado: {input_file}")
    #     sys.exit(1)
    # except json.JSONDecodeError as e:
    #     logger.error(f"Erro ao decodificar JSON do arquivo {input_file}: {e}")
    #     sys.exit(1)
    # except Exception as e:
    #     logger.error(f"Erro inesperado ao carregar {input_file}: {e}")
    #     sys.exit(1)
    
    # Assumindo que core.utils.load_json_safe existe:
    schema_data = load_json_safe(input_file)
    if schema_data is None:
        logger.error("Falha ao carregar o arquivo de schema. Abortando.")
        sys.exit(1)

    logger.info("Gerando DataFrame achatado do schema...")
    try:
        df_schema = generate_schema_dataframe(schema_data)
    except Exception as e:
        logger.error(f"Erro durante a geração do DataFrame: {e}", exc_info=True)
        sys.exit(1)

    if df_schema.empty:
        logger.warning("O DataFrame gerado está vazio. Verifique o arquivo de entrada ou os logs.")
        # Decide se quer salvar um CSV vazio ou não.
        # Vamos salvar um vazio por enquanto, mas alertando.
        # sys.exit(1) # Opcional: Sair se vazio

    logger.info(f"Salvando DataFrame com {len(df_schema)} linhas em: {output_file}")
    try:
        # Garante que o diretório de saída exista
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        # Salva como CSV com UTF-8
        df_schema.to_csv(output_file, index=False, encoding='utf-8-sig') # utf-8-sig para compatibilidade Excel
        logger.info("DataFrame salvo com sucesso!")
    except Exception as e:
        logger.error(f"Erro ao salvar o DataFrame no arquivo {output_file}: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 