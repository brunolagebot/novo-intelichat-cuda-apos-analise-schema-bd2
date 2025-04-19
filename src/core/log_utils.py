import logging
import os
from datetime import datetime
import sys

def setup_logging(log_file='data/logs/app.log', level=logging.INFO):
    """
    Configura o sistema de logging para a aplicação.
    
    Args:
        log_file (str): Caminho para o arquivo de log
        level (int): Nível de logging (default: logging.INFO)
    """
    # Garantir que o diretório de logs existe
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configurar o formato do log
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Configurar o logger raiz
    logging.basicConfig(
        level=level,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Log inicial
    logging.info(f"Logging inicializado em {datetime.now().strftime(date_format)}")
    
    return logging.getLogger(__name__)
