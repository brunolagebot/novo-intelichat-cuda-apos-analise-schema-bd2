import logging
import sys
import os
import pytest

# Adiciona o diretório raiz ao sys.path para permitir importações de src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.core.log_utils import setup_logging
from src.core import config

def test_setup_logging_configures_and_logs(caplog, tmp_path):
    """Verifica se setup_logging configura o logger e registra a mensagem inicial.
    
    Também garante que o diretório de log seja criado.
    """
    # Temporariamente sobrescreve o caminho do log para um diretório temporário
    # para não interferir com o log real ou outros testes.
    original_log_file = config.LOG_FILE
    temp_log_dir = tmp_path / "logs"
    temp_log_file = temp_log_dir / "test_app.log"
    config.LOG_FILE = str(temp_log_file)
    
    # Reinicia o estado do logging (importante se outros testes já configuraram)
    logging.shutdown()
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        handler.close()
    # Limpa o cache de loggers para garantir nova configuração
    logging.Logger.manager.loggerDict.clear()

    try:
        with caplog.at_level(logging.INFO):
            setup_logging()

            # 1. Verifica se a mensagem de configuração foi logada
            assert "Logging configurado com sucesso." in caplog.text
            found_log = False
            for record in caplog.records:
                if record.message == "Logging configurado com sucesso." and record.levelno == logging.INFO:
                    found_log = True
                    break
            assert found_log, "A mensagem de configuração INFO não foi encontrada nos logs capturados."

            # 2. Verifica se o logger raiz está configurado
            root_logger = logging.getLogger()
            assert root_logger.hasHandlers(), "O logger raiz não tem handlers configurados após setup_logging."
            assert root_logger.level <= logging.DEBUG, "O nível do logger raiz não está configurado para DEBUG ou inferior."

            # 3. Verifica se o diretório de log foi criado
            assert temp_log_dir.is_dir(), f"O diretório de log {temp_log_dir} não foi criado."
            
            # 4. Tenta logar algo para garantir que funciona
            test_logger = logging.getLogger("test_setup")
            test_logger.info("Log de teste após setup.")
            assert "Log de teste após setup." in caplog.text

    finally:
        # Restaura o caminho original do log em config para não afetar outros testes
        config.LOG_FILE = original_log_file
        # Limpa a configuração novamente para isolamento
        logging.shutdown()
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
            handler.close()
        logging.Logger.manager.loggerDict.clear() 