#!/usr/bin/env python
# coding: utf-8

"""
Script para ler um arquivo JSON e salvá-lo de volta com formatação padrão,
corrigindo potenciais problemas de indentação ou estrutura inválida.
"""

import os
import sys
import logging
import time

# --- Adiciona o diretório raiz ao sys.path --- #
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM Adição ao sys.path --- #

import src.core.config as config
from src.core.logging_config import setup_logging
from src.utils.json_helpers import load_json, save_json

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Arquivo Alvo --- 
# Pode ser alterado ou passado como argumento se necessário
TARGET_JSON_FILE = config.METADATA_FILE 

def main():
    """Função principal para carregar e salvar o JSON."""
    logger.info(f"--- Iniciando Script de Formatação para: {TARGET_JSON_FILE} ---")
    start_time = time.time()

    # 1. Carregar arquivo JSON
    logger.info(f"Carregando arquivo: {TARGET_JSON_FILE}...")
    try:
        data = load_json(TARGET_JSON_FILE)
        if data is None:
            # load_json já loga o erro específico
            logger.error("Falha ao carregar o arquivo JSON. Abortando.")
            return
        logger.info("Arquivo JSON carregado com sucesso.")
        
    except Exception as e:
         logger.error(f"Erro inesperado durante o carregamento: {e}", exc_info=True)
         return

    # 2. Salvar o arquivo de volta (sobrescrevendo com formatação padrão)
    logger.info(f"Salvando arquivo de volta em {TARGET_JSON_FILE} com formatação padrão...")
    save_start_time = time.time()
    try:
        if save_json(data, TARGET_JSON_FILE):
            save_end_time = time.time()
            logger.info(f"Arquivo JSON formatado e salvo com sucesso em {save_end_time - save_start_time:.2f}s.")
        else:
            # save_json já loga o erro específico
            logger.error(f"Falha ao salvar o arquivo JSON formatado em {TARGET_JSON_FILE}.")
            
    except Exception as e:
         logger.error(f"Erro inesperado durante o salvamento: {e}", exc_info=True)
         
    end_time = time.time()
    logger.info(f"--- Script Concluído em {end_time - start_time:.2f} segundos ---")

if __name__ == "__main__":
    main() 