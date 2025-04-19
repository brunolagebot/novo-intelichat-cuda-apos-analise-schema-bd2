import sys
import os
import pytest
import json
import logging
import copy

# Adiciona o diretório raiz ao sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Importa funções e config
from src.core.data_loader import load_metadata, load_technical_schema, load_overview_counts
from src.core import config
from src.core.log_utils import setup_logging

# Setup logging para testes
setup_logging()

# --- Fixtures --- #

@pytest.fixture
def mock_metadata_file(tmp_path):
    """Cria um arquivo schema_metadata.json falso."""
    metadata_content = {
        "_global_context": "Contexto de teste",
        "TABLES": {
            "CLIENTES": {
                "description": "Tabela de clientes de teste",
                "COLUMNS": {
                    "ID": {"description": "ID único do cliente"},
                    "NOME": {"description": "Nome do cliente", "value_mapping_notes": "Verificar maiúsculas"}
                }
            }
        }
    }
    metadata_dir = tmp_path / "data" / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    file_path = metadata_dir / "schema_metadata.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(metadata_content, f)
    return str(file_path), metadata_content

@pytest.fixture
def mock_technical_schema_file(tmp_path):
    """Cria um arquivo technical_schema falso."""
    tech_content = {
        "CLIENTES": {
            "object_type": "TABLE",
            "columns": [
                {"name": "ID", "type": "INTEGER"},
                {"name": "NOME", "type": "VARCHAR(100)"}
            ]
        },
        "PEDIDOS": {
            "object_type": "TABLE",
            "columns": [
                {"name": "PEDIDO_ID", "type": "INTEGER"},
                {"name": "CLIENTE_ID", "type": "INTEGER"}
            ]
        }
    }
    tech_dir = tmp_path / "data" / "processed" # Ou onde quer que ele esteja
    tech_dir.mkdir(parents=True, exist_ok=True)
    file_path = tech_dir / "schema_enriched_for_embedding.json" # Nome do config
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(tech_content, f)
    return str(file_path), tech_content

@pytest.fixture
def mock_overview_counts_file(tmp_path):
    """Cria um arquivo de contagens falso."""
    counts_content = {
        "TABLES:CLIENTES": 150,
        "TABLES:PEDIDOS": 5000
    }
    counts_dir = tmp_path / "data" / "metadata"
    counts_dir.mkdir(parents=True, exist_ok=True)
    file_path = counts_dir / "overview_counts.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(counts_content, f)
    return str(file_path), counts_content

# --- Testes --- #

def test_load_metadata_success(mock_metadata_file, monkeypatch):
    """Testa carregar metadados com sucesso."""
    mock_path, expected_content = mock_metadata_file
    # Sobrescreve o caminho no config para usar o mock
    monkeypatch.setattr(config, 'METADATA_FILE', mock_path)
    
    # Limpa o cache da função se ela usar @st.cache_data ou similar
    load_metadata.clear()
    
    loaded_data = load_metadata()
    assert loaded_data == expected_content

def test_load_metadata_file_not_found(monkeypatch, caplog):
    """Testa carregar metadados quando o arquivo não existe."""
    non_existent_path = "/path/to/non/existent/metadata.json"
    monkeypatch.setattr(config, 'METADATA_FILE', non_existent_path)
    load_metadata.clear()
    
    with caplog.at_level(logging.WARNING):
        loaded_data = load_metadata()
        
    assert loaded_data == {}, "Deveria retornar um dict vazio se o arquivo não for encontrado."
    assert f"Arquivo de metadados {non_existent_path} não encontrado" in caplog.text

def test_load_technical_schema_success(mock_technical_schema_file, monkeypatch):
    """Testa carregar schema técnico com sucesso."""
    mock_path, expected_content = mock_technical_schema_file
    monkeypatch.setattr(config, 'TECHNICAL_SCHEMA_FILE', mock_path)
    load_technical_schema.clear()
    
    loaded_data = load_technical_schema()
    assert loaded_data == expected_content

def test_load_technical_schema_file_not_found(monkeypatch, caplog):
    """Testa carregar schema técnico quando o arquivo não existe."""
    non_existent_path = "/path/to/non/existent/tech_schema.json"
    monkeypatch.setattr(config, 'TECHNICAL_SCHEMA_FILE', non_existent_path)
    load_technical_schema.clear()
    
    with caplog.at_level(logging.ERROR):
        loaded_data = load_technical_schema()
        
    assert loaded_data is None, "Deveria retornar None se o arquivo não for encontrado."
    assert f"Erro ao carregar schema técnico: Arquivo não encontrado em '{non_existent_path}'" in caplog.text

def test_load_overview_counts_success(mock_overview_counts_file, monkeypatch):
    """Testa carregar contagens com sucesso."""
    mock_path, expected_content = mock_overview_counts_file
    monkeypatch.setattr(config, 'OVERVIEW_COUNTS_FILE', mock_path)
    load_overview_counts.clear()
    
    loaded_data = load_overview_counts()
    assert loaded_data == expected_content

def test_load_overview_counts_file_not_found(monkeypatch, caplog):
    """Testa carregar contagens quando o arquivo não existe."""
    non_existent_path = "/path/to/non/existent/counts.json"
    monkeypatch.setattr(config, 'OVERVIEW_COUNTS_FILE', non_existent_path)
    load_overview_counts.clear()
    
    with caplog.at_level(logging.WARNING):
        loaded_data = load_overview_counts()
        
    assert loaded_data == {}, "Deveria retornar um dict vazio se o arquivo não for encontrado."
    assert f"Arquivo de contagens {non_existent_path} não encontrado" in caplog.text

# TODO: Adicionar testes para load_and_process_data (mais complexo, requer mocking de st.session_state e talvez outras funções) 