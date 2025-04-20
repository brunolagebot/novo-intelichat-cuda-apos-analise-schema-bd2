# scripts/extract_metadata_keys.py
"""Script para extrair conjuntos específicos de chaves de metadados
(descrições AI e metadados manuais) de seus respectivos arquivos JSON
e combiná-los em um único arquivo JSON de saída.

Útil para criar um snapshot focado de certos metadados.
"""

import json
import os
import sys
import logging
from copy import deepcopy

# Adiciona o diretório raiz ao sys.path para encontrar 'src'
# __file__ é o path do script atual (extract_metadata_keys.py)
# os.path.dirname(__file__) é o diretório 'scripts'
# os.path.join(..., '..') volta um nível para o diretório raiz 'Novo'
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(ROOT_DIR)

try:
    # Tenta importar as configurações e utilitários de log
    from src.core.log_utils import setup_logging
    # Nota: Não precisamos de config aqui, definiremos os paths diretamente
except ImportError as e:
    # Fallback se a estrutura 'src' não for encontrada ou houver erro
    print(f"Erro ao importar módulos de 'src'. Verifique a estrutura do projeto e PYTHONPATH: {e}", file=sys.stderr)
    # Configuração de logging básica como fallback
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    setup_logging = lambda: None # No-op function

# Configurar logging
setup_logging() # Chamar a função de configuração (real ou no-op)
logger = logging.getLogger(__name__)

# Caminhos relativos à raiz do projeto (ROOT_DIR)
AI_DESC_FILE = os.path.join(ROOT_DIR, 'data', 'metadata', 'ai_generated_descriptions_openai_35turbo.json')
MANUAL_META_FILE = os.path.join(ROOT_DIR, 'data', 'metadata', 'metadata_schema_manual.json')
OUTPUT_FILE = os.path.join(ROOT_DIR, 'data', 'metadata', 'extracted_metadata.json')

def load_json_safe(filename):
    """Carrega um arquivo JSON com tratamento de erros."""
    if not os.path.exists(filename):
        logger.error(f"Arquivo não encontrado: {filename}")
        return None
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON do arquivo {filename}: {e}")
        # Tenta fornecer mais detalhes sobre o erro
        try:
            with open(filename, 'r', encoding='utf-8') as f_err:
                content_preview = f_err.read(500) # Lê os primeiros 500 caracteres
            logger.error(f"Prévia do conteúdo perto do erro (pode não ser exato): {content_preview}")
        except Exception as read_err:
            logger.error(f"Não foi possível ler o arquivo para prévia do erro: {read_err}")
        return None
    except IOError as e:
        logger.error(f"Erro de I/O ao ler o arquivo {filename}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar {filename}: {type(e).__name__} - {e}")
        return None

def find_keys_recursive(data, keys_to_find):
    """
    Busca recursivamente em uma estrutura de dados (listas/dicionários)
    por dicionários que contenham todas as chaves em 'keys_to_find'.
    Retorna uma lista de cópias desses dicionários encontrados.
    """
    found_items = []
    if isinstance(data, dict):
        # Verifica se o dicionário atual contém todas as chaves necessárias
        if all(key in data for key in keys_to_find):
            # Extrai apenas as chaves desejadas
            item = {key: data.get(key) for key in keys_to_find}
            found_items.append(deepcopy(item)) # Adiciona uma cópia

        # Continua a busca nos valores do dicionário
        for key, value in data.items():
            found_items.extend(find_keys_recursive(value, keys_to_find))
    elif isinstance(data, list):
        # Continua a busca nos itens da lista
        for item in data:
            found_items.extend(find_keys_recursive(item, keys_to_find))
    # Ignora outros tipos de dados (int, str, bool, None)

    return found_items

def main():
    """Função principal para carregar, extrair e salvar os dados."""
    logger.info("Iniciando extração de metadados...")

    # --- Carregar e Extrair Dados do AI Descriptions ---
    logger.info(f"Carregando descrições AI de: {AI_DESC_FILE}")
    ai_data = load_json_safe(AI_DESC_FILE)
    extracted_ai_data = []
    if ai_data:
        keys_ai = ["generated_description", "model_used", "generation_timestamp"]
        logger.info(f"Extraindo chaves {keys_ai} do arquivo AI...")
        extracted_ai_data = find_keys_recursive(ai_data, keys_ai)
        logger.info(f"Encontrados {len(extracted_ai_data)} registros com as chaves AI.")
    else:
        logger.warning("Não foi possível carregar os dados AI. A extração para este arquivo será pulada.")

    # --- Carregar e Extrair Dados do Manual Metadata ---
    logger.info(f"Carregando metadados manuais de: {MANUAL_META_FILE}")
    manual_data = load_json_safe(MANUAL_META_FILE)
    extracted_manual_data = []
    if manual_data:
        keys_manual = ["business_description", "value_mapping_notes"]
        logger.info(f"Extraindo chaves {keys_manual} do arquivo manual...")
        extracted_manual_data = find_keys_recursive(manual_data, keys_manual)
        logger.info(f"Encontrados {len(extracted_manual_data)} registros com as chaves manuais.")
    else:
        logger.warning("Não foi possível carregar os metadados manuais. A extração para este arquivo será pulada.")

    # --- Combinar e Salvar Resultados ---
    output_data = {
        "extracted_ai_descriptions": extracted_ai_data,
        "extracted_manual_metadata": extracted_manual_data
    }

    logger.info(f"Salvando dados extraídos em: {OUTPUT_FILE}")
    try:
        # Cria o diretório de saída se não existir
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f_out:
            json.dump(output_data, f_out, ensure_ascii=False, indent=4)
        logger.info("Dados extraídos salvos com sucesso!")
    except IOError as e:
        logger.error(f"Erro de I/O ao salvar o arquivo {OUTPUT_FILE}: {e}")
    except Exception as e:
        logger.error(f"Erro inesperado ao salvar {OUTPUT_FILE}: {type(e).__name__} - {e}")

if __name__ == "__main__":
    main()
