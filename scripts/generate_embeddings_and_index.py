#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import os
import sys
import logging
import time
from datetime import datetime
from pathlib import Path
import numpy as np
import faiss
import ollama

# --- Adiciona o diretório raiz ao sys.path --- #
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Importações do Projeto --- #
try:
    from src.core.log_utils import setup_logging
    setup_logging() # Configura o logger conforme definido no projeto
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    print("AVISO: src.core.log_utils.setup_logging não encontrado. Usando config básica.")

try:
    from src.core.config import (
        MERGED_SCHEMA_FOR_EMBEDDINGS_FILE, 
        EMBEDDED_SCHEMA_FILE, 
        FAISS_INDEX_FILE, 
        EMBEDDING_DIMENSION
    )
except ImportError:
    logging.error("Erro ao importar constantes de src.core.config. Verifique o PYTHONPATH e a existência do arquivo.")
    # Definir padrões de fallback se a importação falhar (NÃO IDEAL, mas evita crash)
    MERGED_SCHEMA_FOR_EMBEDDINGS_FILE = 'data/processed/merged_schema_for_embeddings.json'
    EMBEDDED_SCHEMA_FILE = 'data/embeddings/schema_with_embeddings.json'
    FAISS_INDEX_FILE = 'data/embeddings/faiss_index.idx'
    EMBEDDING_DIMENSION = 768 # Padrão para nomic-embed-text
    print("AVISO: Usando caminhos e dimensão padrão devido a falha na importação de config.")

logger = logging.getLogger(__name__)

# --- Constantes --- #
OLLAMA_EMBED_MODEL = 'nomic-embed-text'
MAX_SAMPLE_VALUES = 20 # Limite máximo de valores de exemplo a incluir no prompt
MIN_SAMPLE_VALUES = 10 # Mínimo de valores de exemplo a incluir (se disponíveis)
# OLLAMA_BATCH_SIZE removido, pois a API não suporta batch via prompt=

# --- Funções Auxiliares --- #

def load_json_file(file_path: Path, description: str):
    """Carrega um arquivo JSON com tratamento de erro."""
    logger.info(f"Carregando {description} de '{file_path}'...")
    if not file_path.exists():
        logger.error(f"Erro: Arquivo {description} não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON de {description} em '{file_path}': {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar {description} de '{file_path}': {e}", exc_info=True)
        return None

def build_column_text(table_name, column_data):
    """Constrói a string de texto para gerar o embedding da coluna."""
    parts = []
    col_name = column_data.get('name', 'N/A')
    parts.append(f"Tabela: {table_name}")
    parts.append(f"Coluna: {col_name}")
    if column_data.get('type'): parts.append(f"Tipo: {column_data['type']}")
    # Incluir descrições, dando prioridade à manual
    desc = (column_data.get('business_description') or 
            column_data.get('ai_generated_description') or 
            column_data.get('description')) # Descrição técnica original
    if desc: parts.append(f"Descrição: {desc}")
    if column_data.get('value_mapping_notes'): parts.append(f"Notas Mapeamento: {column_data['value_mapping_notes']}")
    
    # Adicionar valores de exemplo
    sample_values = column_data.get('sample_values', [])
    if sample_values:
        num_samples = len(sample_values)
        # Garantir que sample_values seja uma lista antes de fatiar
        if isinstance(sample_values, list):
            samples_to_include = sample_values[:max(MIN_SAMPLE_VALUES, min(num_samples, MAX_SAMPLE_VALUES))]
            # Garantir que os itens sejam strings antes do join
            parts.append(f"Valores Exemplo ({len(samples_to_include)} de {num_samples}): {', '.join(map(str, samples_to_include))}")
        else:
            logger.warning(f"'sample_values' para {table_name}.{col_name} não é uma lista, ignorando exemplos.")

    return "\n".join(parts)

def generate_timestamp_string():
    """Gera um timestamp no formato YYYYMMDD_HHMMSS."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def insert_timestamp_in_path(original_path_str: str, timestamp: str) -> Path:
    """Insere um timestamp no nome do arquivo antes da extensão."""
    original_path = Path(original_path_str)
    return original_path.parent / f"{original_path.stem}_{timestamp}{original_path.suffix}"

def save_json_file(data, file_path: Path, description: str):
    """Salva dados em um arquivo JSON com tratamento de erro."""
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"{description} salvo com sucesso em '{file_path}'")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar {description} em '{file_path}': {e}", exc_info=True)
        return False

def save_faiss_index(index, file_path: Path):
    """Salva o índice FAISS."""
    logger.info(f"Salvando índice FAISS em '{file_path}'...")
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        faiss.write_index(index, str(file_path)) # write_index espera string
        logger.info(f"Índice FAISS com {index.ntotal} vetores salvo com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar índice FAISS em '{file_path}': {e}", exc_info=True)
        return False

# Função process_batch removida

# --- Função Principal --- #
def main():
    logger.info("--- Iniciando Geração de Embeddings e Índice FAISS (Sequencial) --- ") # Título atualizado
    overall_start_time = time.time()

    # 1. Carregar Schema Mesclado
    merged_schema_path = Path(MERGED_SCHEMA_FOR_EMBEDDINGS_FILE)
    schema_data = load_json_file(merged_schema_path, "Schema Mesclado")
    if schema_data is None:
        logger.critical("Não foi possível carregar o schema mesclado. Abortando.")
        sys.exit(1)
        
    # Fazer cópia para adicionar embeddings
    schema_with_embeddings = json.loads(json.dumps(schema_data)) 

    # 2. Inicializar Cliente Ollama
    try:
        client = ollama.Client()
        client.list()
        logger.info(f"Cliente Ollama inicializado e conectado. Usando modelo: {OLLAMA_EMBED_MODEL}")
    except Exception as e:
        logger.critical(f"Falha ao inicializar ou conectar ao cliente Ollama: {e}. Verifique se o Ollama está rodando.", exc_info=True)
        sys.exit(1)

    # 3. Gerar Embeddings (Sequencialmente)
    logger.info("Iniciando geração de embeddings (processando coluna por coluna)...")
    embeddings_list = [] # Lista final para FAISS
    embedding_map = [] # Para rastrear qual embedding pertence a qual coluna
    processed_columns_count = 0
    total_columns_estimate = sum(len(tbl.get('columns', [])) for tbl in schema_data.values() if isinstance(tbl, dict))
    total_generation_errors = 0

    embed_loop_start = time.time()
    if isinstance(schema_with_embeddings, dict):
        for table_name_orig, table_data in schema_with_embeddings.items():
            table_name = table_name_orig 
            if isinstance(table_data, dict) and 'columns' in table_data and isinstance(table_data['columns'], list):
                for col_index, column_data in enumerate(table_data['columns']): 
                    processed_columns_count += 1
                    col_name = column_data.get('name', f'col_idx_{col_index}')
                    
                    # Construir texto
                    text_to_embed = build_column_text(table_name, column_data)
                    logger.debug(f"Texto para {table_name}.{col_name}:\n{text_to_embed}\n---")

                    # Gerar embedding para esta coluna
                    try:
                        response = client.embeddings(model=OLLAMA_EMBED_MODEL, prompt=text_to_embed)
                        embedding_vector = response.get('embedding')
                        
                        if embedding_vector and isinstance(embedding_vector, list):
                            # Adicionar ao schema
                            column_data['embedding'] = embedding_vector
                            # Adicionar à lista para FAISS
                            embeddings_list.append(embedding_vector)
                            embedding_map.append({
                                'table': table_name_orig,
                                'column': column_data.get('name'),
                                'list_index': len(embeddings_list) - 1
                            })
                            logger.debug(f"Embedding gerado para {table_name}.{col_name}")
                        else:
                            logger.warning(f"Resposta de embedding inválida para {table_name}.{col_name}. Resposta: {response}")
                            total_generation_errors += 1
                            column_data['embedding'] = None # Marcar como falha

                    except Exception as e:
                        logger.error(f"Erro ao gerar embedding para {table_name}.{col_name}: {e}", exc_info=True)
                        total_generation_errors += 1
                        column_data['embedding'] = None # Marcar como falha
                    
                    # Log de progresso
                    if processed_columns_count % 50 == 0:
                        logger.info(f"Progresso: {processed_columns_count}/{total_columns_estimate} colunas processadas...")
                        
            else:
                 logger.warning(f"Objeto '{table_name_orig}' ignorado na geração por não conter lista 'columns' válida.")
    else:
        logger.error("Schema mesclado não é um dicionário. Geração não pode ser realizada.")
        sys.exit(1)

    embed_loop_end = time.time()
    logger.info(f"Geração de embeddings concluída em {embed_loop_end - embed_loop_start:.2f}s.")
    logger.info(f"Total de colunas processadas: {processed_columns_count}. Erros na geração: {total_generation_errors}")

    if not embeddings_list:
        logger.error("Nenhum embedding foi gerado com sucesso. Abortando.")
        sys.exit(1)

    # 4. Salvar Schema com Embeddings
    timestamp = generate_timestamp_string()
    output_schema_path = insert_timestamp_in_path(EMBEDDED_SCHEMA_FILE, timestamp)
    logger.info(f"Salvando schema com embeddings em: {output_schema_path}")
    if not save_json_file(schema_with_embeddings, output_schema_path, "Schema com Embeddings"):
        logger.error("Falha ao salvar o schema com embeddings.")

    # 5. Criar e Salvar Índice FAISS
    faiss_build_start = time.time()
    logger.info("Preparando para criar índice FAISS...")
    try:
        embeddings_np = np.array(embeddings_list).astype('float32')
        logger.info(f"Array NumPy de embeddings criado com shape: {embeddings_np.shape}")
        
        if embeddings_np.size == 0:
             logger.error("Array de embeddings está vazio. Abortando criação do índice.")
             sys.exit(1)
             
        actual_dimension = embeddings_np.shape[1]
        if actual_dimension != EMBEDDING_DIMENSION:
             logger.warning(f"Dimensão real do embedding ({actual_dimension}) difere da esperada ({EMBEDDING_DIMENSION}). Verifique o modelo e config.py.")
             dimension_to_use = actual_dimension 
        else:
             dimension_to_use = EMBEDDING_DIMENSION

        logger.info(f"Criando índice FAISS (IndexFlatL2) com dimensão {dimension_to_use}...")
        index = faiss.IndexFlatL2(dimension_to_use)
        index.add(embeddings_np)
        logger.info(f"Índice FAISS criado com {index.ntotal} vetores.")
        
        output_index_path = insert_timestamp_in_path(FAISS_INDEX_FILE, timestamp)
        if save_faiss_index(index, output_index_path):
            logger.info(f"Índice FAISS salvo com sucesso em {output_index_path}")
        else:
            logger.error("Falha ao salvar o índice FAISS.")
            
    except ValueError as e:
        logger.error(f"Erro ao converter embeddings para NumPy array: {e}. Todos os embeddings têm a mesma dimensão?", exc_info=True)
    except IndexError:
         logger.error("Erro ao obter a dimensão dos embeddings NumPy. Array vazio ou malformado?", exc_info=True)
    except Exception as e:
        logger.error(f"Erro inesperado ao criar ou salvar índice FAISS: {e}", exc_info=True)
        
    faiss_build_end = time.time()
    logger.info(f"Tempo para criar e salvar FAISS: {faiss_build_end - faiss_build_start:.2f}s")

    overall_end_time = time.time()
    logger.info(f"--- Processo Geral Concluído em {overall_end_time - overall_start_time:.2f} segundos ---")

if __name__ == "__main__":
    main() 