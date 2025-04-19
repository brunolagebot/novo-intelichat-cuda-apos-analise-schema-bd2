import json
import sys
import os
import pytest
import logging

# Adiciona o diretório raiz ao sys.path para permitir importações de src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Importa as funções a serem testadas
from src.utils.json_helpers import load_json, save_json
# Importa setup_logging para garantir que os logs sejam capturáveis
from src.core.log_utils import setup_logging

# Configura o logging para os testes deste módulo
setup_logging()

# --- Testes para save_json --- 

def test_save_json_success(tmp_path):
    """Testa salvar um JSON válido com sucesso."""
    data_to_save = {"key": "value", "number": 123, "list": [1, 2, 3]}
    test_file = tmp_path / "valid.json"
    
    success = save_json(data_to_save, str(test_file))
    
    assert success is True, "save_json deveria retornar True em caso de sucesso."
    assert test_file.is_file(), "O arquivo JSON não foi criado."
    
    # Verifica o conteúdo
    with open(test_file, 'r', encoding='utf-8') as f:
        loaded_data = json.load(f)
    assert loaded_data == data_to_save, "O conteúdo do arquivo salvo não corresponde ao original."

def test_save_json_creates_dir(tmp_path):
    """Testa se save_json cria diretórios intermediários."""
    data_to_save = {"a": 1}
    nested_dir = tmp_path / "nested" / "dir"
    test_file = nested_dir / "test.json"
    
    assert not nested_dir.exists(), "O diretório aninhado não deveria existir antes do teste."
    
    success = save_json(data_to_save, str(test_file))
    
    assert success is True
    assert nested_dir.is_dir(), "O diretório aninhado não foi criado."
    assert test_file.is_file(), "O arquivo JSON não foi criado no diretório aninhado."

def test_save_json_failure_io_error(tmp_path, caplog):
    """Testa falha ao salvar JSON devido a erro de IO (simulado por permissão)."""
    # Simular erro de IO tornando o diretório não gravável (pode não funcionar em todos os OS)
    # Alternativa: passar um caminho inválido/protegido se soubermos um
    # Por simplicidade, vamos usar um caminho que provavelmente falha
    invalid_path = "/hopefully/non/existent/or/unwritable/path/fail.json"
    data_to_save = {"fail": True}
    
    with caplog.at_level(logging.ERROR):
        success = save_json(data_to_save, invalid_path)
        
    assert success is False, "save_json deveria retornar False em caso de erro."
    assert "Erro ao salvar JSON" in caplog.text, "Mensagem de erro esperada não encontrada no log."

# --- Testes para load_json --- 

def test_load_json_success(tmp_path):
    """Testa carregar um JSON válido com sucesso."""
    valid_data = {"key": "value", "nested": {"num": 42}}
    test_file = tmp_path / "valid_load.json"
    with open(test_file, 'w', encoding='utf-8') as f:
        json.dump(valid_data, f)
        
    loaded_data = load_json(str(test_file))
    
    assert loaded_data == valid_data, "Os dados carregados não correspondem aos salvos."

def test_load_json_file_not_found(tmp_path, caplog):
    """Testa carregar um arquivo JSON inexistente."""
    non_existent_file = tmp_path / "not_found.json"
    
    with caplog.at_level(logging.ERROR):
        loaded_data = load_json(str(non_existent_file))
        
    assert loaded_data is None, "load_json deveria retornar None se o arquivo não for encontrado."
    assert "Arquivo não encontrado" in caplog.text, "Mensagem de erro 'Arquivo não encontrado' esperada no log."

def test_load_json_invalid_json(tmp_path, caplog):
    """Testa carregar um arquivo com conteúdo JSON inválido."""
    invalid_content = '{"key": "value", "unterminated string"}'
    test_file = tmp_path / "invalid.json"
    test_file.write_text(invalid_content, encoding='utf-8')
    
    with caplog.at_level(logging.ERROR):
        loaded_data = load_json(str(test_file))
        
    assert loaded_data is None, "load_json deveria retornar None para JSON inválido."
    assert "Erro ao decodificar JSON" in caplog.text, "Mensagem de erro 'Erro ao decodificar JSON' esperada no log."

def test_load_json_empty_file(tmp_path, caplog):
    """Testa carregar um arquivo JSON vazio."""
    test_file = tmp_path / "empty.json"
    test_file.touch()
    
    with caplog.at_level(logging.ERROR):
        loaded_data = load_json(str(test_file))
        
    assert loaded_data is None, "load_json deveria retornar None para arquivo vazio (que não é JSON válido)."
    assert "Erro ao decodificar JSON" in caplog.text, "Mensagem de erro de decodificação esperada para arquivo vazio." 