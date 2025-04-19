import sys
import os
import pytest
import json
import logging
from collections import Counter

# Adiciona o diretório raiz ao sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Importa a função a ser testada (assumindo que a lógica principal foi refatorada)
# Se a lógica ainda estiver em main(), precisaríamos refatorar ou testar de forma diferente
from scripts.analyze_schema import analyze_schema # Ajuste se o nome da função for diferente
from src.core.log_utils import setup_logging

# Setup logging
setup_logging()

# --- Fixture com Dados de Schema de Exemplo --- #

@pytest.fixture
def sample_schema_data():
    return {
        "TABLES": {
            "CLIENTES": {
                "object_type": "TABLE",
                "business_description": "Dados dos clientes",
                "columns": [
                    {"name": "ID", "type": "INTEGER", "is_pk": True, "business_description": "ID"},
                    {"name": "NOME", "type": "VARCHAR(100)"},
                    {"name": "EMAIL", "type": "VARCHAR(100)", "description_manual": "Email principal"} # Description manual
                ],
                 "constraints": {"primary_key": [{"columns": ["ID"]}]}
            },
            "PEDIDOS": {
                "object_type": "TABLE",
                 "business_description": None, # Sem descrição do objeto
                "columns": [
                    {"name": "PEDIDO_ID", "type": "INTEGER", "is_pk": True},
                    {"name": "CLIENTE_ID", "type": "INTEGER", "is_fk": True, "fk_references": {"references_table": "CLIENTES", "references_columns": ["ID"]}, "description_ai": "ID do cliente"}, # Description AI
                    {"name": "DATA_PEDIDO", "type": "TIMESTAMP"}
                ],
                "constraints": {
                    "primary_key": [{"columns": ["PEDIDO_ID"]}],
                    "foreign_keys": [{"columns": ["CLIENTE_ID"], "references_table": "CLIENTES", "references_columns": ["ID"]}]
                }
            }
        },
        "VIEWS": {
            "VIEW_CLIENTES_ATIVOS": {
                "object_type": "VIEW",
                "business_description": "Visão de clientes ativos",
                "columns": [
                    {"name": "ID_CLIENTE", "type": "INTEGER"},
                    {"name": "NOME_CLIENTE", "type": "VARCHAR(100)"}
                ]
            }
        }
    }

# --- Testes --- #

def test_analyze_schema_counts(sample_schema_data):
    """Testa as contagens básicas geradas pela análise."""
    results = analyze_schema(sample_schema_data)
    
    assert isinstance(results, dict)
    assert results['object_types'] == {'TABLES', 'VIEWS'}
    assert results['object_counts'] == {'TABLES': 2, 'VIEWS': 1}
    assert results['total_columns'] == 5 # 3 em CLIENTES, 2 em PEDIDOS, 2 em VIEW

def test_analyze_schema_column_types(sample_schema_data):
    """Testa a distribuição de tipos de coluna."""
    results = analyze_schema(sample_schema_data)
    
    assert isinstance(results['column_type_distribution'], Counter)
    assert results['column_type_distribution']["INTEGER"] == 3
    assert results['column_type_distribution']["VARCHAR(100)"] == 2
    assert results['column_type_distribution']["TIMESTAMP"] == 1

def test_analyze_schema_descriptions(sample_schema_data):
    """Testa a contagem de colunas com/sem descrições."""
    results = analyze_schema(sample_schema_data)
    
    # Nota: A lógica atual pode precisar de ajuste para diferenciar desc manual/AI
    # Assumindo que description_manual conta como manual e description_ai como AI
    assert results['columns_with_manual_description'] == 1 # EMAIL
    assert results['columns_without_manual_description'] == 6 # Total 7 colunas - 1 manual
    assert results['columns_with_ai_description'] == 1 # CLIENTE_ID
    assert results['columns_without_ai_description'] == 6 # Total 7 colunas - 1 AI
    
    # Idealmente, a função analyze_schema deveria retornar nomes das colunas também

def test_analyze_schema_keys(sample_schema_data):
    """Testa a contagem de colunas PK/FK."""
    results = analyze_schema(sample_schema_data)
    
    assert results['primary_key_columns'] == 2 # ID, PEDIDO_ID
    assert results['foreign_key_columns'] == 1 # CLIENTE_ID

# Adicione mais testes para verificar outras lógicas da função analyze_schema 