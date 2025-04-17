import logging
import logging.config
import os
import sys

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        'detailed': {
            'format': '%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
            'stream': sys.stdout, # Envia para a saída padrão (console)
        },
        'file': {
            'level': 'DEBUG', # Captura mais detalhes no arquivo
            'formatter': 'detailed',
            'class': 'logging.FileHandler',
            'filename': 'data/app.log', # Salva os logs na pasta data
            'mode': 'a', # 'a' para append, 'w' para overwrite a cada execução
            'encoding': 'utf-8',
        },
    },
    'loggers': {
        '': {  # Logger raiz
            'handlers': ['console', 'file'],
            'level': 'DEBUG', # Nível mais baixo para capturar tudo nos handlers
            'propagate': True,
        },
        'fdb': { # Configuração específica para logs da biblioteca fdb (se necessário)
            'handlers': ['console', 'file'],
            'level': 'INFO', # Pode ajustar para WARNING ou ERROR se for muito verboso
            'propagate': False, # Não propaga para o logger raiz para evitar duplicidade
        },
         'streamlit': { # Configuração específica para logs do Streamlit
            'handlers': ['console', 'file'],
            'level': 'INFO',
            'propagate': False,
        },
         'watchdog': { # Watchdog pode ser verboso, ajustar nível
            'handlers': ['console', 'file'],
            'level': 'WARNING',
            'propagate': False,
        }
        # Adicione outros loggers específicos de bibliotecas se necessário
    }
}

def setup_logging():
    """Configura o logging usando o dicionário LOGGING_CONFIG."""
    # Garante que o diretório data exista
    log_dir = os.path.dirname(LOGGING_CONFIG['handlers']['file']['filename'])
    os.makedirs(log_dir, exist_ok=True)
    
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger(__name__)
    logger.info("Logging configurado com sucesso.") 