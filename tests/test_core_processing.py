# Testes para src/core/processing.py

import pytest
from src.core.processing import preprocess_user_input

# Casos de teste parametrizados
@pytest.mark.parametrize(
    "input_string, expected_output",
    [
        # Caso normal, sem alterações
        ("Olá mundo", "Olá mundo"),
        # Espaços no início e fim
        ("  Olá mundo  ", "Olá mundo"),
        # Múltiplos espaços entre palavras
        ("Olá   mundo  com   espaços", "Olá mundo com espaços"),
        # Combinação de todos os espaços extras
        ("   Olá   mundo   final   ", "Olá mundo final"),
        # String vazia
        ("", ""),
        # String apenas com espaços
        ("    ", ""),
        # Tabulações e novas linhas (também são espaços em branco)
        ("\tOlá\nmundo\t", "Olá mundo"),
        # Nenhum espaço
        ("SemEspacos", "SemEspacos"),
    ]
)
def test_preprocess_user_input_spacing(input_string, expected_output):
    """Testa a limpeza de espaços da função preprocess_user_input."""
    assert preprocess_user_input(input_string) == expected_output

def test_preprocess_user_input_non_string():
    """Testa o comportamento com input que não é string."""
    assert preprocess_user_input(None) is None
    assert preprocess_user_input(123) == 123
    assert preprocess_user_input(["lista"]) == ["lista"] 