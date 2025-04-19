import json
import os
import sys
import logging
import time
from dotenv import load_dotenv
import numpy as np # Necessário para array de embeddings
import faiss # Necessário para índice FAISS
from src.core.logging_config import setup_logging
# ATUALIZADO: Importa apenas os arquivos necessários
from src.core.config import EMBEDDED_SCHEMA_FILE, FAISS_INDEX_FILE 

# --- Adiciona o diretório raiz ao sys.path --- #
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM Adição ao sys.path --- #

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# Carregar variáveis de ambiente (pode não ser mais necessário, mas mantido por segurança)
load_dotenv()

# --- Constantes Removidas/Atualizadas ---
# Não precisamos mais do modelo ou batch size aqui
# EMBEDDING_MODEL_NAME = ...
# MODEL_SHORT_NAME = ...
# SENTENCE_TRANSFORMER_BATCH_SIZE = ...

# Arquivo de entrada agora é o que já contém embeddings
INPUT_SCHEMA_WITH_EMBEDDINGS_FILE = EMBEDDED_SCHEMA_FILE 
OUTPUT_FAISS_INDEX_FILE = FAISS_INDEX_FILE 

def load_json_data(file_path):
    """Carrega dados de um arquivo JSON."""
    logger.info(f"Carregando schema com embeddings de {file_path}...")
    if not os.path.exists(file_path):
        logger.error(f"Erro: Arquivo de entrada não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info("Schema com embeddings carregado com sucesso.")
        return data
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON de {file_path}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar {file_path}: {e}")
        return None

# REMOVIDO: build_object_text, build_column_text
# REMOVIDO: save_json_data (não salvamos mais JSON)

def save_faiss_index(index, file_path):
    """Salva o índice FAISS.""" # Docstring simplificada
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
    """Função principal para GERAR ÍNDICE FAISS a partir de embeddings existentes."""
    logger.info("--- Iniciando Geração de Índice FAISS a partir de Embeddings Existentes --- ")
    
    overall_start_time = time.time()

    # 1. Carregar schema COM embeddings
    load_data_start = time.time()
    schema_data = load_json_data(INPUT_SCHEMA_WITH_EMBEDDINGS_FILE) 
    if schema_data is None:
        logger.error(f"Não foi possível carregar o schema de {INPUT_SCHEMA_WITH_EMBEDDINGS_FILE}. Abortando.")
        return
    load_data_end = time.time()
    logger.info(f"Tempo de carregamento do JSON com embeddings: {load_data_end - load_data_start:.2f}s")

    # 2. Extrair Embeddings Existentes
    extract_start_time = time.time()
    logger.info("Extraindo embeddings existentes do schema...")
    existing_embeddings = []
    embedding_keys_map = [] # Mantém o mapeamento para referência, se necessário

    objects_to_process = {k: v for k, v in schema_data.items() if isinstance(v, dict) and 'object_type' in v}
    for obj_name, obj_data in objects_to_process.items():
        if 'columns' in obj_data:
            for i, col_data in enumerate(obj_data['columns']):
                embedding = col_data.get('embedding') 
                if embedding and isinstance(embedding, list):
                    existing_embeddings.append(embedding)
                    # Guarda a referência de onde veio o embedding (índice na lista 'existing_embeddings')
                    embedding_keys_map.append({'schema_key': f"{obj_name}.columns[{i}]", 'index': len(existing_embeddings) - 1}) 
                else:
                    logger.warning(f"Coluna {obj_name}.{col_data.get('name')} não possui um embedding válido (lista numérica). Pulando.")

    total_embeddings = len(existing_embeddings)
    if total_embeddings == 0:
        logger.error("Nenhum embedding encontrado no arquivo de entrada. Não é possível gerar o índice FAISS.")
        return
        
    logger.info(f"Total de embeddings extraídos: {total_embeddings}")
    
    # Converter para numpy array
    try:
        embeddings_np = np.array(existing_embeddings, dtype='float32')
        logger.info(f"Embeddings convertidos para array numpy com shape: {embeddings_np.shape}")
    except ValueError as e:
         logger.error(f"Erro ao converter embeddings para numpy array: {e}. Verifique se todos os embeddings têm a mesma dimensão.", exc_info=True)
         return
         
    extract_end_time = time.time()
    logger.info(f"Tempo de extração e conversão dos embeddings: {extract_end_time - extract_start_time:.2f}s")

    # REMOVIDO: Etapas 3 (Carregar modelo) e 4 (Gerar Embeddings)
    # REMOVIDO: Etapa 5 (Atualizar dicionário - já carregado)
    # REMOVIDO: Etapa 6 (Salvar JSON)

    # 3. (Originalmente 7) Criar e Salvar Índice FAISS
    faiss_build_start = time.time()
    try:
        if embeddings_np.size == 0:
             logger.error("Array de embeddings está vazio após conversão. Abortando criação do índice.")
             return
             
        dimension = embeddings_np.shape[1] 
        logger.info(f"Criando índice FAISS (IndexFlatL2) com dimensão {dimension}...")
        index = faiss.IndexFlatL2(dimension)
        index.add(embeddings_np) # Adiciona os embeddings existentes
        logger.info(f"Índice FAISS criado com {index.ntotal} vetores.")
        
        if save_faiss_index(index, OUTPUT_FAISS_INDEX_FILE):
            logger.info("Índice FAISS salvo com sucesso.")
        else:
            logger.error("Falha ao salvar o índice FAISS.")
            
    except IndexError:
         logger.error("Erro ao obter a dimensão dos embeddings. O array está vazio ou malformado?", exc_info=True)
    except Exception as e:
        logger.error(f"Erro ao criar ou salvar índice FAISS: {e}", exc_info=True)
        
    faiss_build_end = time.time()
    logger.info(f"Tempo para criar e salvar FAISS: {faiss_build_end - faiss_build_start:.2f}s")

    overall_end_time = time.time()
    logger.info(f"--- Processo de Geração de Índice FAISS Concluído em {overall_end_time - overall_start_time:.2f} segundos ---")

if __name__ == "__main__":
    main() 