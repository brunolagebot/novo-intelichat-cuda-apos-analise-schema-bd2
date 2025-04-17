import os
import json
import logging

logger = logging.getLogger(__name__)

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
        return None
    except IOError as e:
        logger.error(f"Erro ao ler o arquivo {filename}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar {filename}: {e}", exc_info=True)
        return None

# Outras funções utilitárias podem ser adicionadas aqui no futuro. 