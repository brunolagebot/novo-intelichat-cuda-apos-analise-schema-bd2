import pytest
import requests # Necessário para simular exceções
import json
from unittest.mock import MagicMock # Usado para criar mocks mais flexíveis
from src.ollama_integration.client import chat_completion

# Marca para pular este teste por padrão, pois ele faria uma chamada real à API
# Para executar, use: pytest -m "not slow"
# Ou remova a marca @pytest.mark.slow
@pytest.mark.slow
def test_chat_completion_integration():
    """Teste de integração básico para chat_completion (requer Ollama rodando)."""
    messages = [
        {"role": "user", "content": "Explique o que é um teste de integração em uma frase."}
    ]
    
    # Testa sem streaming
    response_full = chat_completion(messages=messages, stream=False)
    assert response_full is not None, "Falha ao obter resposta (não stream). Ollama está rodando?"
    assert isinstance(response_full, str)
    assert len(response_full) > 5 # Ajuste pequeno na verificação
    print(f"\n[Integração] Resposta (não stream): {response_full}")

    # Testa com streaming
    response_stream = chat_completion(messages=messages, stream=True)
    assert response_stream is not None, "Falha ao obter resposta (stream). Ollama está rodando?"
    full_stream_response = ""
    chunk_count = 0
    print("\n[Integração] Resposta (stream): ", end="")
    for chunk in response_stream:
        assert isinstance(chunk, str)
        print(chunk, end="", flush=True) # Imprime o stream
        full_stream_response += chunk
        chunk_count += 1
    print() # Nova linha no final
    
    assert chunk_count > 0, "Gerador de stream não produziu chunks."
    assert len(full_stream_response) > 5 # Ajuste pequeno

# --- Fixtures e Mocks (Fixture removida) --- 

# @pytest.fixture
# def mock_requests_post(mocker):
#     return mocker.patch('src.ollama_integration.client.requests.post')

# Exemplo de mensagens para os testes
MESSAGES_EXAMPLE = [{"role": "user", "content": "Olá"}]

# Função auxiliar para criar respostas de stream simuladas
def create_mock_stream_response(lines):
    mock_resp = MagicMock(spec=requests.Response)
    mock_resp.status_code = 200
    mock_resp.iter_lines.return_value = iter(lines)
    mock_resp.raise_for_status = MagicMock()
    mock_resp.close = MagicMock()
    return mock_resp

# --- Testes Unitários (usando mocker diretamente) --- 

def test_chat_completion_success_no_stream(mocker):
    """Testa o sucesso da chamada sem streaming."""
    # Aplica o mock dentro do teste
    mock_post = mocker.patch('src.ollama_integration.client.requests.post')
    
    # Configura o mock para retornar uma resposta JSON válida
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    expected_content = "Resposta simulada sem stream."
    mock_response.json.return_value = {
        "model": "llama3",
        "created_at": "2023-10-26T18:25:43.511Z",
        "message": {"role": "assistant", "content": expected_content},
        "done": True
    }
    mock_response.raise_for_status = MagicMock()
    mock_post.return_value = mock_response

    result = chat_completion(messages=MESSAGES_EXAMPLE, stream=False)

    assert result == expected_content
    mock_post.assert_called_once()

def test_chat_completion_success_with_stream(mocker):
    """Testa o sucesso da chamada com streaming."""
    mock_post = mocker.patch('src.ollama_integration.client.requests.post')
    
    # Cria linhas de stream simuladas
    stream_lines = [
        json.dumps({"message": {"content": "Olá "}, "done": False}).encode('utf-8'),
        json.dumps({"message": {"content": "Mundo"}, "done": False}).encode('utf-8'),
        json.dumps({"message": {"content": "!"}, "done": True}).encode('utf-8'),
    ]
    mock_response = create_mock_stream_response(stream_lines)
    mock_post.return_value = mock_response

    generator = chat_completion(messages=MESSAGES_EXAMPLE, stream=True)
    result_list = list(generator)

    assert result_list == ["Olá ", "Mundo", "!"]
    mock_post.assert_called_once()
    mock_response.close.assert_called_once()

def test_chat_completion_http_error(mocker, caplog):
    """Testa o tratamento de erro HTTP (ex: 404 Not Found)."""
    mock_post = mocker.patch('src.ollama_integration.client.requests.post')
    
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 404
    mock_response.text = "Modelo não encontrado"
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(
        "404 Client Error", response=mock_response
    )
    mock_post.return_value = mock_response

    result = chat_completion(messages=MESSAGES_EXAMPLE, stream=False)

    assert result is None
    assert "Erro HTTP 404" in caplog.text
    assert "Modelo não encontrado" in caplog.text
    mock_post.assert_called_once()

def test_chat_completion_connection_error(mocker, caplog):
    """Testa o tratamento de erro de conexão."""
    mock_post = mocker.patch('src.ollama_integration.client.requests.post')
    
    # Configura o mock para levantar ConnectionError diretamente
    mock_post.side_effect = requests.exceptions.ConnectionError("Falha ao conectar")

    result = chat_completion(messages=MESSAGES_EXAMPLE, stream=False)

    assert result is None
    assert "Erro de conexão" in caplog.text
    assert "Falha ao conectar" in caplog.text
    mock_post.assert_called_once()

def test_chat_completion_stream_json_error(mocker, caplog):
    """Testa o erro de JSON inválido durante o streaming."""
    mock_post = mocker.patch('src.ollama_integration.client.requests.post')
    
    stream_lines = [
        json.dumps({"message": {"content": "Parte 1"}, "done": False}).encode('utf-8'),
        b'{"message": {"content": "JSON incompleto', # Linha inválida
        json.dumps({"message": {"content": "Parte 3"}, "done": True}).encode('utf-8'),
    ]
    mock_response = create_mock_stream_response(stream_lines)
    mock_post.return_value = mock_response

    generator = chat_completion(messages=MESSAGES_EXAMPLE, stream=True)
    result_list = list(generator)

    assert result_list == ["Parte 1"]
    assert "Erro ao decodificar linha do stream JSON" in caplog.text
    mock_post.assert_called_once()
    mock_response.close.assert_called_once()

# Exemplo de teste unitário (não implementado, requer mock)
# @pytest.mark.skip(reason="Requer mock da API requests")
# def test_chat_completion_unit_success():
#     # Aqui você usaria pytest-mock ou unittest.mock para simular requests.post
#     # e verificar se chat_completion processa a resposta simulada corretamente
#     pass

# @pytest.mark.skip(reason="Requer mock da API requests")
# def test_chat_completion_unit_connection_error():
#     # Aqui você simularia um requests.exceptions.ConnectionError
#     # e verificaria se chat_completion retorna None e loga o erro
#     pass 