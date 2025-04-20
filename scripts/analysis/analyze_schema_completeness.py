#!/usr/bin/env python
# coding: utf-8

import json
import argparse
import os
import logging
import sys

# Adiciona o diretório raiz ao sys.path para permitir importações de src
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(ROOT_DIR)

from src.core.config import OUTPUT_COMBINED_FILE
# from src.utils.json_helpers import load_json # Usar load_json padrão por enquanto

# Configurar logging básico
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_INPUT_JSON = OUTPUT_COMBINED_FILE # Usar a constante importada

def load_json_safe(filepath):
    """Carrega um arquivo JSON com tratamento de erro básico."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Erro: Arquivo não encontrado em '{filepath}'")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON em '{filepath}': {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao ler '{filepath}': {e}")
        return None

def analyze_completeness(data):
    """Analisa a completude das chaves especificadas no dicionário de schema."""
    if not isinstance(data, dict):
        logger.error("Formato de dados inválido. Esperado um dicionário no nível raiz.")
        return None

    total_columns = 0
    empty_sample_values_count = 0
    non_empty_sample_values_count = 0
    non_empty_with_any_description_count = 0
    # Contadores individuais para cada chave de descrição
    has_business_desc_count = 0
    has_value_notes_count = 0
    has_ai_desc_count = 0

    # Contadores para colunas com sample_values não vazio
    non_empty_sv_with_business_desc_count = 0
    non_empty_sv_with_value_notes_count = 0
    non_empty_sv_with_ai_desc_count = 0

    for obj_name, obj_data in data.items():
        if not isinstance(obj_data, dict) or 'columns' not in obj_data or not isinstance(obj_data['columns'], list):
            logger.warning(f"Estrutura inesperada para o objeto '{obj_name}'. Pulando.")
            continue

        for col_data in obj_data['columns']:
            if not isinstance(col_data, dict):
                logger.warning(f"Item inválido na lista de colunas para '{obj_name}'. Pulando.")
                continue

            total_columns += 1

            # Contagem das descrições (independentemente de sample_values)
            if bool(col_data.get("business_description")):
                has_business_desc_count += 1
            if bool(col_data.get("value_mapping_notes")):
                has_value_notes_count += 1
            if bool(col_data.get("ai_generated_description")):
                has_ai_desc_count += 1

            sample_values = col_data.get("sample_values")

            # Contagem 1: sample_values vazio?
            if isinstance(sample_values, list) and not sample_values:
                empty_sample_values_count += 1
            # Contagem 2: sample_values não vazio?
            elif isinstance(sample_values, list) and sample_values:
                non_empty_sample_values_count += 1

                # Verifica presença de cada descrição INDIVIDUALMENTE para este grupo
                has_business_desc = bool(col_data.get("business_description"))
                has_value_notes = bool(col_data.get("value_mapping_notes"))
                has_ai_desc = bool(col_data.get("ai_generated_description"))

                if has_business_desc:
                    non_empty_sv_with_business_desc_count += 1
                if has_value_notes:
                    non_empty_sv_with_value_notes_count += 1
                if has_ai_desc:
                    non_empty_sv_with_ai_desc_count += 1

                # Contagem 3: Se não vazio, tem ALGUMA descrição preenchida?
                if has_business_desc or has_value_notes or has_ai_desc:
                    non_empty_with_any_description_count += 1
            # Caso onde sample_values não existe ou não é lista (tratar como não tendo amostras)
            # else:
                # Poderia contar aqui se necessário, mas a lógica atual cobre os casos pedidos

    return {
        "total_columns": total_columns,
        "empty_sample_values_count": empty_sample_values_count,
        "non_empty_sample_values_count": non_empty_sample_values_count,
        "non_empty_with_any_description_count": non_empty_with_any_description_count,
        "has_business_desc_count": has_business_desc_count,
        "has_value_notes_count": has_value_notes_count,
        "has_ai_desc_count": has_ai_desc_count,
        # Novos contadores detalhados
        "non_empty_sv_with_business_desc_count": non_empty_sv_with_business_desc_count,
        "non_empty_sv_with_value_notes_count": non_empty_sv_with_value_notes_count,
        "non_empty_sv_with_ai_desc_count": non_empty_sv_with_ai_desc_count,
    }

def main():
    parser = argparse.ArgumentParser(description="Analisa a completude de chaves em um arquivo JSON de schema.")
    parser.add_argument(
        "--input_json",
        default=DEFAULT_INPUT_JSON,
        help=f"Caminho para o arquivo JSON de schema a ser analisado. Padrão: {DEFAULT_INPUT_JSON}"
    )
    args = parser.parse_args()

    logger.info(f"Iniciando análise de completude para: {args.input_json}")

    schema_data = load_json_safe(args.input_json)
    if schema_data is None:
        return # Erro já logado em load_json_safe

    analysis_results = analyze_completeness(schema_data)

    if analysis_results:
        total_cols = analysis_results['total_columns']
        print("\n--- Resultados da Análise de Completude ---")
        print(f"Total de colunas analisadas: {total_cols:,}")

        print("\n# Contagem de Descrições (Geral):")
        bus_desc_count = analysis_results['has_business_desc_count']
        val_notes_count = analysis_results['has_value_notes_count']
        ai_desc_count = analysis_results['has_ai_desc_count']
        print(f"  - Colunas com 'business_description': {bus_desc_count:,} ({bus_desc_count/total_cols:.1%} do total)")
        print(f"  - Colunas com 'value_mapping_notes': {val_notes_count:,} ({val_notes_count/total_cols:.1%} do total)")
        print(f"  - Colunas com 'ai_generated_description': {ai_desc_count:,} ({ai_desc_count/total_cols:.1%} do total)")

        print("\n# Contagem relacionada a 'sample_values':")
        empty_sv_count = analysis_results['empty_sample_values_count']
        non_empty_sv_count = analysis_results['non_empty_sample_values_count']
        non_empty_with_desc_count = analysis_results['non_empty_with_any_description_count']

        print(f"  - Colunas com 'sample_values' vazio ([]): {empty_sv_count:,}")
        print(f"  - Colunas com 'sample_values' não vazio: {non_empty_sv_count:,}")
        if non_empty_sv_count > 0:
            # Mantém a contagem geral
            print(f"    -> Destas, com alguma descrição preenchida: {non_empty_with_desc_count:,} ({non_empty_with_desc_count/non_empty_sv_count:.1%})")
            # Adiciona contagens detalhadas
            print(f"    -> Detalhe:")
            ne_bus_desc = analysis_results['non_empty_sv_with_business_desc_count']
            ne_val_notes = analysis_results['non_empty_sv_with_value_notes_count']
            ne_ai_desc = analysis_results['non_empty_sv_with_ai_desc_count']
            print(f"       - Com 'business_description': {ne_bus_desc:,} ({ne_bus_desc/non_empty_sv_count:.1%})")
            print(f"       - Com 'value_mapping_notes': {ne_val_notes:,} ({ne_val_notes/non_empty_sv_count:.1%})")
            print(f"       - Com 'ai_generated_description': {ne_ai_desc:,} ({ne_ai_desc/non_empty_sv_count:.1%})")
        else:
            print("    -> Nenhuma coluna com 'sample_values' não vazio encontrada.")

        print("----------------------------------------")
    else:
        logger.error("Análise não pôde ser concluída devido a erros anteriores.")

    logger.info("Análise concluída.")

if __name__ == "__main__":
    main() 