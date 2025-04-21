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
from src.utils.json_helpers import load_json
# Nota: Não usamos config.py diretamente aqui para manter o script de análise independente,
# mas definimos o caminho padrão via argparse.

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Constantes Padrão para este Script ---
# Arquivo mestre manual a ser analisado
DEFAULT_INPUT_FILE = "data/metadata/manual_metadata_master.json"

def analyze_manual_metadata(input_path):
    """
    Analisa o arquivo mestre de metadados manuais e conta as descrições e notas
    existentes nos níveis de objeto (tabela/view) e coluna.

    Args:
        input_path (str ou Path): Caminho para o arquivo JSON mestre manual.
    """
    logger.info("--- Iniciando Análise do Arquivo Mestre Manual ---")
    logger.info(f"Arquivo de Entrada: {input_path}")

    # 1. Carregar o arquivo mestre
    logger.info("Carregando arquivo mestre manual...")
    manual_data = load_json(input_path)
    if not manual_data or not isinstance(manual_data, dict):
        logger.critical(f"Falha ao carregar ou formato inválido no arquivo mestre: {input_path}")
        print(f"Erro: Não foi possível carregar ou analisar o arquivo {input_path}")
        return
    logger.info("Arquivo mestre carregado.")

    # 2. Inicializar contadores
    total_objects = 0
    object_desc_count = 0
    object_notes_count = 0
    total_columns_processed = 0 # Contará colunas dentro dos objetos válidos
    column_desc_count = 0
    column_notes_count = 0

    # 3. Iterar e contar
    logger.info("Analisando o conteúdo...")
    for obj_name, obj_data in manual_data.items():
        if not isinstance(obj_data, dict):
            logger.warning(f"Entrada inválida para o objeto '{obj_name}'. Pulando.")
            continue
        total_objects += 1

        # Contar dados de nível de objeto (verificando se não é None e não é string vazia)
        if obj_data.get("object_business_description"):
            object_desc_count += 1
        if obj_data.get("object_value_mapping_notes"):
            object_notes_count += 1

        # Contar dados de nível de coluna
        if "columns" in obj_data and isinstance(obj_data["columns"], dict):
            for col_name, col_data in obj_data["columns"].items():
                if not isinstance(col_data, dict):
                     logger.warning(f"Entrada inválida para a coluna '{obj_name}.{col_name}'. Pulando.")
                     continue
                total_columns_processed += 1
                # Conta apenas se a chave existir e o valor não for None/vazio
                if col_data.get("business_description"):
                    column_desc_count += 1
                if col_data.get("value_mapping_notes"):
                    column_notes_count += 1

    # 4. Imprimir os resultados
    print("\n--- Resultados da Análise do Arquivo Mestre Manual ---")
    print(f"Arquivo Analisado: {input_path}")
    print("-" * 40)
    print(f"Total de Objetos (Tabelas/Views) Encontrados: {total_objects}")
    print(f"  - Descrições de Negócio (Nível Objeto): {object_desc_count}")
    print(f"  - Notas de Mapeamento (Nível Objeto):  {object_notes_count}")
    print("-" * 40)
    print(f"Total de Colunas Processadas (com dados manuais): {total_columns_processed}")
    print(f"  - Descrições de Negócio (Nível Coluna): {column_desc_count}")
    print(f"  - Notas de Mapeamento (Nível Coluna):  {column_notes_count}")
    print("-" * 40)
    logger.info("Análise concluída.")

# --- Ponto de Entrada do Script --- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analisa um arquivo JSON mestre manual e conta descrições/notas.")

    parser.add_argument(
        "-i", "--input",
        default=DEFAULT_INPUT_FILE,
        help=f"Caminho para o arquivo JSON mestre manual a ser analisado. Padrão: {DEFAULT_INPUT_FILE}"
    )

    args = parser.parse_args()

    # Executa a função principal
    analyze_manual_metadata(input_path=args.input) 