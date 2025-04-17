import json
import os
import logging

logger = logging.getLogger(__name__) # Usa um logger

def load_json(file_path, default_value=None):
    """Carrega dados de um arquivo JSON, tratando erros comuns."""
    if default_value is None:
        default_value = [] # Default para listas (histórico, feedback)
    if not os.path.exists(file_path):
        logger.info(f"Arquivo JSON não encontrado: {file_path}. Retornando valor padrão.")
        return default_value
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError:
        logger.error(f"Erro ao decodificar JSON de: {file_path}. Retornando valor padrão.")
        return default_value
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar JSON de {file_path}: {e}")
        return default_value

def save_json(data, file_path):
    """Salva dados em um arquivo JSON, tratando erros."""
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Dados JSON salvos em: {file_path}")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar JSON em {file_path}: {e}")
        return False 