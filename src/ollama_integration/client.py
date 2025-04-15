import requests
import json
import os
import logging
from typing import List, Dict, Generator, Any, Union # Melhorar type hinting
from dotenv import load_dotenv

# Configuração básica do logging - MUDADO PARA DEBUG
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

# Para evitar logs muito verbosos de bibliotecas externas (opcional)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

# Carrega as variáveis do arquivo .env para o ambiente
load_dotenv()

# Agora usa /api/chat por padrão
OLLAMA_API_URL = os.getenv("OLLAMA_API_URL", "http://localhost:11434/api/chat")

# Tenta derivar a URL base da API (removendo /api/chat ou /api/generate)
OLLAMA_BASE_URL = OLLAMA_API_URL.replace("/api/chat", "").replace("/api/generate", "")
OLLAMA_TAGS_URL = f"{OLLAMA_BASE_URL}/api/tags"

def get_available_models() -> List[str]:
    """Busca a lista de modelos disponíveis na API /api/tags do Ollama."""
    try:
        logging.info(f"Buscando modelos disponíveis em {OLLAMA_TAGS_URL}...")
        response = requests.get(OLLAMA_TAGS_URL, timeout=5) # Timeout curto
        response.raise_for_status()
        data = response.json()
        models = [model['name'] for model in data.get('models', [])]
        logging.info(f"Modelos encontrados: {models}")
        # Garante que o modelo padrão do .env esteja na lista, se existir
        default_model = os.getenv("OLLAMA_DEFAULT_MODEL", "llama3")
        if default_model not in models:
             logging.warning(f"Modelo padrão '{default_model}' do .env não encontrado via API /tags.")
             # Poderíamos optar por adicioná-lo mesmo assim, ou apenas logar.
             # Por segurança, não vamos adicioná-lo se a API não o listou.
        return models
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao buscar modelos da API Ollama ({OLLAMA_TAGS_URL}): {e}")
        # Retorna lista vazia ou com fallback?
        # Retornar apenas o default pode ser uma opção segura.
        default_model = os.getenv("OLLAMA_DEFAULT_MODEL", "llama3")
        logging.warning(f"Retornando apenas o modelo padrão '{default_model}' devido a erro na API /tags.")
        return [default_model]
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar resposta JSON da API /tags: {e}")
        default_model = os.getenv("OLLAMA_DEFAULT_MODEL", "llama3")
        logging.warning(f"Retornando apenas o modelo padrão '{default_model}' devido a erro JSON.")
        return [default_model]
    except Exception as e:
        logging.exception(f"Erro inesperado ao buscar modelos: {e}")
        default_model = os.getenv("OLLAMA_DEFAULT_MODEL", "llama3")
        logging.warning(f"Retornando apenas o modelo padrão '{default_model}' devido a erro inesperado.")
        return [default_model]

def chat_completion(messages: List[Dict[str, str]], model: str | None = None, stream: bool = False) -> Union[str, Generator[str, Any, None], None]:
    """Envia um histórico de mensagens para a API /api/chat do Ollama e retorna a resposta.

    Args:
        messages: Uma lista de dicionários, cada um com "role" (user/assistant) e "content".
        model: O nome do modelo Ollama a ser usado. Se None, usa OLLAMA_DEFAULT_MODEL do .env ou 'llama3'.
        stream: Se a resposta deve ser retornada como stream (True) ou de uma vez (False).

    Returns:
        Se stream=False, retorna a string completa da resposta do assistant ou None.
        Se stream=True, retorna um gerador que produz pedaços (chunks) da resposta do assistant.
        Retorna None em caso de erro.
    """
    target_model = model if model else os.getenv("OLLAMA_DEFAULT_MODEL", "llama3")
    logging.debug(f"Enviando para {OLLAMA_API_URL} com modelo {target_model} e stream={stream}")
    logging.debug(f"Messages: {messages}")

    payload = {
        "model": target_model,
        "messages": messages,
        "stream": stream
    }

    try:
        response = requests.post(OLLAMA_API_URL, json=payload, stream=stream) # Habilita stream na request
        response.raise_for_status()

        if stream:
            def stream_generator() -> Generator[str, Any, None]:
                logging.info(f"Iniciando stream para o modelo {target_model}...")
                full_response_content = "" # Para log completo no final
                line_count = 0 # Contador de linhas
                yield_count = 0 # Contador de yields
                try:
                    for line in response.iter_lines():
                        line_count += 1
                        if line:
                            decoded_line = line.decode('utf-8')
                            logging.debug(f"Stream Line {line_count} Raw: {decoded_line}") # Log linha crua
                            try:
                                json_line = json.loads(decoded_line)
                                logging.debug(f"Stream Line {line_count} JSON: {json_line}") # Log JSON decodificado
                                
                                chunk = json_line.get("message", {}).get("content", "")
                                logging.debug(f"Stream Line {line_count} Chunk: '{chunk}'") # Log pedaço extraído
                                
                                if chunk:
                                    full_response_content += chunk
                                    logging.debug(f"Stream Line {line_count}: Yielding chunk...") # Log antes do yield
                                    yield_count += 1
                                    yield chunk
                                else:
                                    logging.debug(f"Stream Line {line_count}: Chunk is empty, skipping yield.")
                                
                                if json_line.get("done", False):
                                    logging.info(f"Stream completo recebido (done=True na linha {line_count}). Resposta: {full_response_content}")
                                    break
                            except json.JSONDecodeError:
                                logging.error(f"Erro ao decodificar linha do stream JSON: {decoded_line}")
                                break
                            except Exception as e:
                                logging.exception(f"Erro processando linha do stream: {decoded_line}")
                                break
                    # Log final após o loop
                    logging.info(f"Stream finalizado para {target_model}. Total linhas: {line_count}, Total yields: {yield_count}.")
                except Exception as e:
                    logging.exception(f"Erro durante o processamento do stream: {e}")
                finally:
                    response.close()
            return stream_generator()
        else:
            logging.info(f"Recebendo resposta completa para o modelo {target_model}...")
            response_data = response.json()
            # Na API /chat, a resposta está em response_data["message"]["content"]
            full_response = response_data.get("message", {}).get("content", "")
            logging.info(f"Resposta completa recebida: {full_response}")
            return full_response

    except requests.exceptions.ConnectionError as e:
        logging.error(f"Erro de conexão ao tentar acessar {OLLAMA_API_URL}: {e}")
        return None
    except requests.exceptions.Timeout as e:
        logging.error(f"Timeout ao tentar acessar {OLLAMA_API_URL}: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        logging.error(f"Erro HTTP {response.status_code} ao acessar {OLLAMA_API_URL}: {e}")
        logging.error(f"Resposta recebida: {response.text}")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro inesperado de request para {OLLAMA_API_URL}: {e}")
        return None
    except json.JSONDecodeError as e:
        # Isso pode acontecer se stream=False e a resposta não for JSON válido
        logging.error(f"Erro ao decodificar a resposta JSON do Ollama (stream=False). Status: {response.status_code}")
        logging.error(f"Resposta recebida: {response.text}")
        return None
    except Exception as e:
        logging.exception(f"Erro inesperado na função chat_completion: {e}")
        return None 