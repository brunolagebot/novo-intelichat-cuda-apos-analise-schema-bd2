import json
import os
import logging
import time
from dotenv import load_dotenv
from tqdm import tqdm # Usar tqdm síncrono
import torch # NOVO: Para verificar GPU
from sentence_transformers import SentenceTransformer # NOVO
import numpy as np # NOVO: Para embeddings numpy
import faiss # NOVO: Para criar índice FAISS

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente (para possível URL do Ollama ou outras configs)
load_dotenv()

# --- Constantes ---
INPUT_JSON_FILE = 'data/combined_schema_details.json'
# NOVO: Define o modelo Sentence Transformer
EMBEDDING_MODEL_NAME = 'paraphrase-multilingual-mpnet-base-v2'
# NOVO: Gerar nome curto para usar nos arquivos de saída
MODEL_SHORT_NAME = "mpnet-multi-base-v2" # Simplificação manual
OUTPUT_JSON_FILE = f'data/schema_embeddings_{MODEL_SHORT_NAME}.json' # Nome dinâmico
OUTPUT_FAISS_INDEX_FILE = f'data/faiss_index_{MODEL_SHORT_NAME}.idx' # Nome dinâmico
# Usa a variável específica para EMBEDDINGS
# OLLAMA_EMBEDDINGS_URL = os.getenv("OLLAMA_EMBEDDINGS_URL", "http://localhost:11434/api/embeddings")
# EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")
# MAX_RETRIES = 3
# RETRY_DELAY = 5 # Segundos
# CONCURRENT_REQUESTS = 20 # Número de requisições paralelas ao Ollama
# REQUEST_TIMEOUT = 60 # Timeout em segundos para cada requisição
SENTENCE_TRANSFORMER_BATCH_SIZE = 64 # Ajustável dependendo da memória CPU/GPU

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

def save_faiss_index(index, file_path):
    logger.info(f"Salvando índice FAISS em {file_path}...")
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        faiss.write_index(index, file_path)
        logger.info(f"Índice FAISS com {index.ntotal} vetores salvo com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar índice FAISS em {file_path}: {e}", exc_info=True)
        return False

def main():
    """Função principal síncrona para gerar embeddings com Sentence Transformers e criar índice FAISS."""
    logger.info("--- Iniciando Geração de Embeddings (Sentence Transformers) e Índice FAISS ---")
    logger.info(f"Usando modelo: {EMBEDDING_MODEL_NAME}")

    overall_start_time = time.time()

    # 1. Carregar dados
    load_data_start = time.time()
    schema_data = load_json_data(INPUT_JSON_FILE)
    if schema_data is None:
        return
    load_data_end = time.time()
    logger.info(f"Tempo de carregamento do JSON: {load_data_end - load_data_start:.2f}s")

    # 2. Preparar textos e mapeamento
    prep_start_time = time.time()
    texts_to_embed = []
    # Lista para mapear índice na lista de textos de volta para (tipo, obj_name, col_index)
    index_to_item_map = []

    logger.info("Preparando textos para embedding...")
    objects_to_process = {k: v for k, v in schema_data.items() if isinstance(v, dict) and 'object_type' in v}
    for obj_name, obj_data in objects_to_process.items():
        # Texto para cada coluna (foco principal para similaridade)
        if 'columns' in obj_data:
            for i, col_data in enumerate(obj_data['columns']):
                col_text = build_column_text(obj_name, col_data)
                texts_to_embed.append(col_text)
                index_to_item_map.append(('column', obj_name, i))

    total_items = len(texts_to_embed)
    if total_items == 0:
        logger.info("Nenhum texto de coluna para gerar embedding encontrado.")
        return
    logger.info(f"Total de textos de colunas para gerar embedding: {total_items}")
    prep_end_time = time.time()
    logger.info(f"Tempo de preparação dos textos: {prep_end_time - prep_start_time:.2f}s")

    # 3. Carregar modelo Sentence Transformer
    model_load_start = time.time()
    logger.info(f"Carregando modelo Sentence Transformer: {EMBEDDING_MODEL_NAME}...")
    # Tenta usar GPU se disponível
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    logger.info(f"Usando device: {device}")
    try:
        model = SentenceTransformer(EMBEDDING_MODEL_NAME, device=device)
    except Exception as e:
        logger.error(f"Falha ao carregar o modelo Sentence Transformer '{EMBEDDING_MODEL_NAME}': {e}")
        logger.error("Verifique se a biblioteca 'sentence-transformers' está instalada e o nome do modelo está correto.")
        return
    model_load_end = time.time()
    logger.info(f"Modelo carregado em: {model_load_end - model_load_start:.2f}s")

    # 4. Gerar Embeddings em Batch
    embed_gen_start = time.time()
    logger.info(f"Gerando embeddings em batches de {SENTENCE_TRANSFORMER_BATCH_SIZE}...")
    try:
        embeddings = model.encode(
            texts_to_embed,
            batch_size=SENTENCE_TRANSFORMER_BATCH_SIZE,
            show_progress_bar=True,
            convert_to_numpy=True # Já converte para numpy array
        )
    except Exception as e:
        logger.error(f"Erro durante a geração de embeddings: {e}", exc_info=True)
        return
    embed_gen_end = time.time()
    logger.info(f"Embeddings gerados em: {embed_gen_end - embed_gen_start:.2f}s")

    if len(embeddings) != total_items:
        logger.error(f"Erro: Número de embeddings gerados ({len(embeddings)}) diferente do número de textos ({total_items}). Abortando.")
        return

    # 5. Atualizar o dicionário do schema com embeddings
    update_start_time = time.time()
    logger.info("Atualizando dicionário de schema com os embeddings gerados...")
    for i, (item_type, obj_name, col_index) in enumerate(index_to_item_map):
        if item_type == 'column':
            try:
                # Garante que o embedding é uma lista de floats padrão para JSON
                schema_data[obj_name]['columns'][col_index]['embedding'] = embeddings[i].tolist()
            except (KeyError, IndexError) as e:
                logger.warning(f"Não foi possível encontrar/atualizar item {item_type} {obj_name} col_idx {col_index} no schema_data: {e}")
        # elif item_type == 'object': # Se decidirmos embedar objetos também
        #     try:
        #         schema_data[obj_name]['embedding'] = embeddings[i].tolist()
        #     except KeyError as e:
        #          logger.warning(f"Não foi possível encontrar/atualizar item {item_type} {obj_name} no schema_data: {e}")
    update_end_time = time.time()
    logger.info(f"Dicionário atualizado em: {update_end_time - update_start_time:.2f}s")

    # 6. Salvar JSON com embeddings
    save_json_start = time.time()
    if not save_json_data(schema_data, OUTPUT_JSON_FILE):
        logger.error("Falha ao salvar o arquivo JSON com embeddings. Processo interrompido.")
        return
    save_json_end = time.time()
    logger.info(f"Tempo para salvar JSON: {save_json_end - save_json_start:.2f}s")

    # 7. Criar e Salvar Índice FAISS (Apenas com embeddings de colunas)
    faiss_build_start = time.time()
    try:
        dimension = embeddings.shape[1] # Pega a dimensão do primeiro embedding gerado
        logger.info(f"Criando índice FAISS (IndexFlatL2) com dimensão {dimension}...")
        # Usar IndexFlatL2 para busca exata por distância Euclidiana (comum para Sentence Transformers)
        # Se precisar de L2 normalizado (similaridade cosseno), normalize os embeddings antes de adicionar
        # embeddings_normalized = embeddings / np.linalg.norm(embeddings, axis=1, keepdims=True)
        # index = faiss.IndexFlatIP(dimension) # IP = Inner Product (similaridade cosseno)
        # index.add(embeddings_normalized)
        index = faiss.IndexFlatL2(dimension)
        index.add(embeddings)
        logger.info(f"Índice FAISS criado com {index.ntotal} vetores.")
        if save_faiss_index(index, OUTPUT_FAISS_INDEX_FILE):
            logger.info("Índice FAISS salvo com sucesso.")
        else:
            logger.error("Falha ao salvar o índice FAISS.")
    except Exception as e:
        logger.error(f"Erro ao criar ou salvar índice FAISS: {e}", exc_info=True)
    faiss_build_end = time.time()
    logger.info(f"Tempo para criar e salvar FAISS: {faiss_build_end - faiss_build_start:.2f}s")

    overall_end_time = time.time()
    logger.info(f"--- Processo Concluído em {overall_end_time - overall_start_time:.2f} segundos ---")

if __name__ == "__main__":
    main() 