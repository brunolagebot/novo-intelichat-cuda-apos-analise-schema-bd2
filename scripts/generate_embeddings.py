import json
import os
import logging
import asyncio
import aiohttp # Usar aiohttp para requisições assíncronas
import time
from dotenv import load_dotenv
from tqdm.asyncio import tqdm # Usar tqdm.asyncio para barras de progresso assíncronas

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente (para possível URL do Ollama ou outras configs)
load_dotenv()

# --- Constantes ---
INPUT_JSON_FILE = 'data/combined_schema_details.json'
OUTPUT_JSON_FILE = 'data/schema_with_embeddings.json'
# Usa a variável específica para EMBEDDINGS
OLLAMA_EMBEDDINGS_URL = os.getenv("OLLAMA_EMBEDDINGS_URL", "http://localhost:11434/api/embeddings")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
MAX_RETRIES = 3
RETRY_DELAY = 5 # Segundos
CONCURRENT_REQUESTS = 20 # Número de requisições paralelas ao Ollama
REQUEST_TIMEOUT = 60 # Timeout em segundos para cada requisição

def load_json_data(file_path):
    """Carrega dados de um arquivo JSON."""
    logger.info(f"Carregando dados de {file_path}...")
    if not os.path.exists(file_path):
        logger.error(f"Erro: Arquivo de entrada não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info("Dados carregados com sucesso.")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON de {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar {file_path}: {e}")
        return None

async def get_ollama_embedding(session: aiohttp.ClientSession, text_to_embed: str, semaphore: asyncio.Semaphore):
    """Obtém o embedding de um texto usando a API do Ollama de forma assíncrona com retentativas e semáforo."""
    if not text_to_embed or not text_to_embed.strip():
        logger.warning("Texto vazio fornecido para embedding, retornando None.")
        return None

    payload = {
        "model": EMBEDDING_MODEL,
        "prompt": text_to_embed
    }
    headers = {'Content-Type': 'application/json'}
    
    async with semaphore: # Adquire o semáforo antes de fazer a requisição
        for attempt in range(MAX_RETRIES):
            try:
                async with session.post(OLLAMA_EMBEDDINGS_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT) as response:
                    response.raise_for_status() # Levanta erro para status >= 400
                    
                    result = await response.json()
                    if "embedding" in result and isinstance(result["embedding"], list):
                        return result["embedding"]
                    else:
                        logger.error(f"Resposta inesperada da API Ollama: {result}")
                        return None
            except aiohttp.ClientConnectionError as e:
                logger.error(f"Erro de conexão com Ollama em {OLLAMA_EMBEDDINGS_URL}: {e}")
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Tentando novamente em {RETRY_DELAY} segundos...")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    logger.error("Máximo de tentativas atingido. Verifique se Ollama está em execução e acessível.")
                    return None
            except asyncio.TimeoutError:
                logger.error(f"Timeout ao conectar com Ollama em {OLLAMA_EMBEDDINGS_URL}.")
                if attempt < MAX_RETRIES - 1:
                    logger.info(f"Tentando novamente em {RETRY_DELAY} segundos...")
                    await asyncio.sleep(RETRY_DELAY)
                else:
                    return None
            except aiohttp.ClientResponseError as e:
                 logger.error(f"Erro HTTP {e.status} na requisição para Ollama: {e.message}")
                 # Verifica se a mensagem indica que o modelo não foi encontrado
                 if e.status == 404 and "model" in str(e.message).lower() and EMBEDDING_MODEL in str(e.message):
                     logger.error(f"Parece que o modelo '{EMBEDDING_MODEL}' não está disponível no Ollama.")
                     logger.error("Execute 'ollama list' ou 'ollama pull nomic-embed-text' para verificar/baixar.")
                     return "MODEL_NOT_FOUND" # Sinalizador especial
                 if attempt < MAX_RETRIES - 1:
                     logger.info(f"Tentando novamente em {RETRY_DELAY} segundos...")
                     await asyncio.sleep(RETRY_DELAY)
                 else:
                     return None
            except aiohttp.ClientError as e: # Captura outras exceções do aiohttp
                 logger.error(f"Erro do cliente aiohttp para Ollama: {e}")
                 if attempt < MAX_RETRIES - 1:
                     logger.info(f"Tentando novamente em {RETRY_DELAY} segundos...")
                     await asyncio.sleep(RETRY_DELAY)
                 else:
                    return None
    return None # Se todas as tentativas falharem


def build_object_text(obj_name, obj_data):
    """Constrói o texto contextual para um objeto (tabela/view)."""
    obj_type = obj_data.get('object_type', 'N/A')
    # Correção: Tratar caso de business_description ser None
    description_raw = obj_data.get('business_description') # Pode ser None
    description = description_raw.strip() if description_raw else ""
    
    text = f"Objeto: {obj_name}\nTipo: {obj_type}"
    if description: # Agora description é garantido ser string
        text += f"\nDescrição: {description}"
    # Adicionar mais contexto se necessário (ex: PKs)
    # pks = [pk.get('columns', []) for pk in obj_data.get('constraints', {}).get('primary_key', [])]
    # if pks: text += f"\nChaves Primárias: {pks}"
    return text

def build_column_text(table_name, col_data):
    """Constrói o texto contextual para uma coluna."""
    col_name = col_data.get('name', 'N/A')
    col_type = col_data.get('type', 'N/A')
    # Correção: Tratar caso de business_description ser None
    description_raw = col_data.get('business_description') # Pode ser None
    description = description_raw.strip() if description_raw else ""
    # Correção: Tratar caso de value_mapping_notes ser None
    notes_raw = col_data.get('value_mapping_notes') # Pode ser None
    notes = notes_raw.strip() if notes_raw else ""

    text = f"Tabela: {table_name}\nColuna: {col_name}\nTipo: {col_type}"
    if description:
        text += f"\nDescrição: {description}"
    if notes:
        text += f"\nNotas de Mapeamento: {notes}"
        
    # Adicionar info PK/FK (simplificado)
    is_pk = col_data.get('is_pk', False)
    fk_ref = col_data.get('fk_references')
    if is_pk:
         text += "\nStatus: Chave Primária"
    elif fk_ref:
         ref_table = fk_ref.get('references_table')
         ref_col = fk_ref.get('references_columns')
         if ref_table and ref_col:
             # Correção: Garantir que ref_col[0] exista e tratar caso de erro
             try:
                 text += f"\nStatus: Chave Estrangeira referenciando {ref_table}.{ref_col[0]}"
             except IndexError:
                 logger.warning(f"Índice inválido para ref_col em {table_name}.{col_name}")
                 text += f"\nStatus: Chave Estrangeira referenciando {ref_table} (coluna ref. inválida)"

    return text

def save_json_data(data, file_path):
    """Salva dados em um arquivo JSON."""
    logger.info(f"Salvando dados com embeddings em {file_path}...")
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info("Dados salvos com sucesso.")
        return True
    except IOError as e:
        logger.error(f"Erro de I/O ao salvar {file_path}: {e}")
        return False
    except Exception as e:
        logger.error(f"Erro inesperado ao salvar {file_path}: {e}")
        return False

async def process_item(session, item_key, text, semaphore):
    """Função auxiliar para processar um único item (objeto ou coluna) e retornar a chave e o embedding."""
    embedding = await get_ollama_embedding(session, text, semaphore)
    return item_key, embedding

async def main():
    """Função principal assíncrona para gerar embeddings."""
    logger.info("--- Iniciando Geração de Embeddings (Paralela) ---")
    
    start_time = time.time()
    schema_data = load_json_data(INPUT_JSON_FILE)
    if schema_data is None:
        return # Erro já logado

    objects_to_process = {k: v for k, v in schema_data.items() if isinstance(v, dict) and 'object_type' in v}
    
    items_to_embed = {}
    # Coleta todos os textos a serem embedados
    for obj_name, obj_data in objects_to_process.items():
        obj_text = build_object_text(obj_name, obj_data)
        items_to_embed[(obj_name, None)] = obj_text # Chave: (obj_name, None) para objetos
        if 'columns' in obj_data:
            for i, col_data in enumerate(obj_data['columns']):
                 col_text = build_column_text(obj_name, col_data)
                 items_to_embed[(obj_name, i)] = col_text # Chave: (obj_name, col_index) para colunas

    total_items = len(items_to_embed)
    if total_items == 0:
        logger.info("Nenhum item para processar encontrado no arquivo de entrada.")
        return

    logger.info(f"Modelo de Embedding: {EMBEDDING_MODEL}")
    logger.info(f"API Ollama: {OLLAMA_EMBEDDINGS_URL}")
    logger.info(f"Nível de Concorrência: {CONCURRENT_REQUESTS}")
    logger.info(f"Total de itens (objetos + colunas) para gerar embedding: {total_items}")

    processed_count = 0
    error_count = 0
    model_not_found_flag = False

    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS) # Cria o semáforo
    tasks = []
    results_dict = {}

    async with aiohttp.ClientSession() as session: # Cria uma única sessão
        # Cria as tarefas
        for item_key, text in items_to_embed.items():
            tasks.append(process_item(session, item_key, text, semaphore))

        # Executa as tarefas concorrentemente e mostra progresso
        logger.info("Iniciando requisições paralelas ao Ollama...")
        results = []
        for future in tqdm(asyncio.as_completed(tasks), total=total_items, desc="Gerando Embeddings", unit="item"):
            result = await future
            results.append(result)
            item_key, embedding = result
            if embedding == "MODEL_NOT_FOUND":
                model_not_found_flag = True
                # Idealmente, cancelaríamos as tarefas restantes, mas gather trata isso
                # logger.warning("Modelo não encontrado, interrompendo...")
                # Não podemos usar break aqui diretamente com as_completed
            elif embedding is not None:
                results_dict[item_key] = embedding
                processed_count += 1
            else:
                # Mesmo que tenha erro, precisamos registrar para saber que foi processado
                error_count += 1
                results_dict[item_key] = None # Marcar que houve erro

    end_time = time.time()
    logger.info(f"Tempo de processamento dos embeddings: {end_time - start_time:.2f} segundos")

    # Atualiza o schema_data com os resultados
    if not model_not_found_flag:
        logger.info("Atualizando dicionário de schema com os embeddings gerados...")
        for (obj_name, col_index), embedding in results_dict.items():
             if obj_name in schema_data:
                 if col_index is None: # É um objeto
                     if embedding:
                        schema_data[obj_name]['embedding'] = embedding
                     elif 'embedding' in schema_data[obj_name]: # Remove embedding antigo se houve erro
                         del schema_data[obj_name]['embedding']
                 else: # É uma coluna
                    if 'columns' in schema_data[obj_name] and col_index < len(schema_data[obj_name]['columns']):
                        if embedding:
                            schema_data[obj_name]['columns'][col_index]['embedding'] = embedding
                        elif 'embedding' in schema_data[obj_name]['columns'][col_index]: # Remove embedding antigo se houve erro
                            del schema_data[obj_name]['columns'][col_index]['embedding']
                    else:
                         logger.warning(f"Índice de coluna {col_index} inválido para objeto {obj_name} ao atualizar resultados.")
             else:
                logger.warning(f"Objeto {obj_name} não encontrado no schema_data ao atualizar resultados.")

    logger.info("--- Geração de Embeddings Concluída ---")
    logger.info(f"Itens processados com sucesso: {processed_count}")
    logger.info(f"Erros durante o processamento: {error_count}")

    if model_not_found_flag:
         logger.error(f"Processo interrompido pois o modelo '{EMBEDDING_MODEL}' não foi encontrado no Ollama.")
         logger.error("Nenhum arquivo de saída foi gerado ou atualizado.")
    elif error_count > 0:
         logger.warning("Houveram erros. O arquivo de saída conterá embeddings apenas para os itens processados com sucesso.")
         if save_json_data(schema_data, OUTPUT_JSON_FILE):
             logger.info(f"Arquivo parcialmente preenchido salvo em {OUTPUT_JSON_FILE}")
         else:
             logger.error("Falha ao salvar o arquivo JSON de saída.")
    elif processed_count > 0:
        if save_json_data(schema_data, OUTPUT_JSON_FILE):
            logger.info(f"Arquivo com embeddings salvo com sucesso em {OUTPUT_JSON_FILE}")
        else:
            logger.error("Falha ao salvar o arquivo JSON de saída.")
    # Não precisa de 'else' para processed_count == 0, pois já foi tratado no início.

if __name__ == "__main__":
    asyncio.run(main()) 