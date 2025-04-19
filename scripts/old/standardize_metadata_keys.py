#!/usr/bin/env python
# coding: utf-8

"""
Script para padronizar as chaves no arquivo de metadados manuais.
FORÇA a substituição de todas as ocorrências da chave "description": 
para "business_description": usando substituição de string.
"""

import os
import sys
import json
import logging
import time
import re # Importar re para substituição

# --- Adiciona o diretório raiz ao sys.path --- #
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM Adição ao sys.path --- #

import src.core.config as config
from src.core.logging_config import setup_logging
from src.utils.json_helpers import save_json # Apenas para salvar

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Constantes ---
METADATA_FILE_PATH = config.METADATA_FILE
OLD_KEY_PATTERN = r'"description"\s*:' # Regex para encontrar "description": (case-insensitive)
NEW_KEY_STRING = '"business_description":' # String de substituição

# REMOVIDA: Função find_and_standardize_keys - usaremos substituição direta

def main():
    """Função principal para carregar, substituir string e salvar."""
    logger.info(f"--- Iniciando Script de Substituição FORÇADA de Chave em {METADATA_FILE_PATH} ---")
    start_time = time.time()

    # 1. Carregar arquivo como TEXTO
    logger.info(f"Lendo o conteúdo bruto do arquivo: {METADATA_FILE_PATH}...")
    try:
        with open(METADATA_FILE_PATH, 'r', encoding='utf-8') as f:
            raw_content = f.read()
        logger.info(f"Arquivo lido com sucesso ({len(raw_content)} caracteres).")
    except FileNotFoundError:
        logger.error(f"Arquivo não encontrado: {METADATA_FILE_PATH}. Abortando.")
        return
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo: {e}", exc_info=True)
        return

    # 2. Realizar a substituição de string (case-insensitive)
    logger.info(f"Realizando substituição de 'description': por '{NEW_KEY_STRING}' (case-insensitive)...")
    try:
        # re.IGNORECASE garante que "description":, "Description":, "DESCRIPTION": etc. sejam pegos
        # O padrão busca a chave entre aspas, seguida por espaços opcionais e dois pontos.
        modified_content, num_subs = re.subn(OLD_KEY_PATTERN, NEW_KEY_STRING, raw_content, flags=re.IGNORECASE)
        
        logger.info(f"Substituição concluída. {num_subs} ocorrências substituídas.")
        
    except Exception as e:
        logger.error(f"Erro durante a substituição de string: {e}", exc_info=True)
        return

    # 3. Validar se o resultado ainda é um JSON válido (opcional, mas recomendado)
    logger.info("Validando se o conteúdo modificado ainda é um JSON válido...")
    try:
        # Tenta carregar o conteúdo modificado como JSON
        json.loads(modified_content) 
        logger.info("Validação JSON bem-sucedida.")
    except json.JSONDecodeError as e:
        logger.error(f"ERRO CRÍTICO: O conteúdo modificado NÃO é um JSON válido! {e}")
        logger.error("NÃO FOI POSSÍVEL SALVAR O ARQUIVO. Verifique o padrão de substituição ou o arquivo original.")
        return
    except Exception as e:
        logger.error(f"Erro inesperado durante a validação JSON: {e}", exc_info=True)
        return

    # 4. Salvar o conteúdo modificado de volta no arquivo (sobrescrevendo)
    if num_subs > 0:
        logger.info(f"Salvando conteúdo modificado de volta em {METADATA_FILE_PATH}...")
        save_start_time = time.time()
        try:
            with open(METADATA_FILE_PATH, 'w', encoding='utf-8') as f:
                f.write(modified_content)
            save_end_time = time.time()
            logger.info(f"Arquivo atualizado com sucesso em {save_end_time - save_start_time:.2f}s.")
        except Exception as e:
            logger.error(f"Falha ao salvar o arquivo modificado: {e}", exc_info=True)
    else:
        logger.info("Nenhuma ocorrência encontrada para substituir. O arquivo não foi modificado.")

    end_time = time.time()
    logger.info(f"--- Script Concluído em {end_time - start_time:.2f} segundos ---")

if __name__ == "__main__":
    main() 