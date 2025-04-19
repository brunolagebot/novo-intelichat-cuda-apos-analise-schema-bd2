import sys
import os
import pytest

# Adiciona o diretório raiz ao sys.path para permitir importações de src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Importa as configurações que queremos testar
from src.core import config

def test_config_variables_exist_and_typed(caplog): # Adiciona o fixture caplog
    """Verifica se variáveis de configuração essenciais existem e têm o tipo esperado."""
    
    # Exemplo: Verificar se LOG_FILE existe e é uma string
    assert hasattr(config, 'LOG_FILE'), "A variável LOG_FILE deve existir em config.py"
    assert isinstance(config.LOG_FILE, str), "LOG_FILE deve ser uma string"
    assert config.LOG_FILE == 'data/logs/app.log', "Valor de LOG_FILE não corresponde ao esperado"

    # Exemplo: Verificar se METADATA_FILE existe e é uma string
    assert hasattr(config, 'METADATA_FILE'), "A variável METADATA_FILE deve existir em config.py"
    assert isinstance(config.METADATA_FILE, str), "METADATA_FILE deve ser uma string"
    assert config.METADATA_FILE == 'data/metadata/schema_metadata.json', "Valor de METADATA_FILE não corresponde ao esperado"
    
    # Exemplo: Verificar DEFAULT_DB_USER
    assert hasattr(config, 'DEFAULT_DB_USER'), "DEFAULT_DB_USER deve existir"
    assert isinstance(config.DEFAULT_DB_USER, str), "DEFAULT_DB_USER deve ser string"
    assert config.DEFAULT_DB_USER == "SYSDBA", "Valor de DEFAULT_DB_USER incorreto"

    # Exemplo: Verificar TYPE_EXPLANATIONS (dicionário)
    assert hasattr(config, 'TYPE_EXPLANATIONS'), "TYPE_EXPLANATIONS deve existir"
    assert isinstance(config.TYPE_EXPLANATIONS, dict), "TYPE_EXPLANATIONS deve ser um dicionário"
    assert "VARCHAR" in config.TYPE_EXPLANATIONS, "Chave VARCHAR deve existir em TYPE_EXPLANATIONS"

    # Exemplo de uso do caplog (não terá saída aqui, pois config não loga)
    # Em outros testes, você pode verificar os logs gerados:
    # logger.info("Teste de log") # Supondo que um logger foi configurado
    # assert "Teste de log" in caplog.text
    # assert "INFO" in caplog.text
    # Ou verificar registros específicos:
    # assert len(caplog.records) == 1
    # assert caplog.records[0].levelname == "INFO"
    # assert caplog.records[0].message == "Teste de log"
    
    # Linha para demonstrar que o teste passou sem logs inesperados neste caso
    assert len(caplog.records) == 0, "Nenhum log deveria ser gerado ao importar config.py"

# Adicionar mais testes conforme necessário para outras variáveis ou lógicas em config.py 