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
import concurrent.futures # Para paralelismo

# --- Adiciona o diretório raiz ao sys.path --- #
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# --- Importações do Projeto --- #
try:
    from src.core.log_utils import setup_logging
    setup_logging()
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
    logging.error("Erro ao importar constantes de src.core.config.")
    MERGED_SCHEMA_FOR_EMBEDDINGS_FILE = 'data/processed/schema_enriched_for_embedding.json'
    EMBEDDED_SCHEMA_FILE = 'data/embeddings/schema_with_embeddings.json'
    FAISS_INDEX_FILE = 'data/embeddings/faiss_index.idx'
    EMBEDDING_DIMENSION = 768 # Ajuste se seu modelo for diferente
    print("AVISO: Usando caminhos e dimensão padrão devido a falha na importação de config.")

logger = logging.getLogger(__name__)

# --- Constantes --- #
OLLAMA_EMBED_MODEL = 'nomic-embed-text'
MAX_SAMPLE_VALUES = 20
MIN_SAMPLE_VALUES = 10
NUM_WORKERS = 10 # Número de threads paralelas (Ajuste!)

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

# --- FUNÇÃO build_column_text MODIFICADA ---
def build_column_text(table_name, column_data):
    """Constrói a string de texto para gerar o embedding da coluna, combinando descrições."""
    parts = []
    col_name = column_data.get('name', 'N/A')
    parts.append(f"Tabela: {table_name}")
    parts.append(f"Coluna: {col_name}")
    if column_data.get('type'): parts.append(f"Tipo: {column_data['type']}")

    # Coleta todas as descrições disponíveis
    description_parts = []
    bus_desc = column_data.get('business_description')
    ai_desc = column_data.get('ai_generated_description')
    tech_desc = column_data.get('description') # Descrição técnica original

    if bus_desc and isinstance(bus_desc, str) and bus_desc.strip():
        description_parts.append(f"Descrição Negócio: {bus_desc.strip()}")
    if ai_desc and isinstance(ai_desc, str) and ai_desc.strip():
        description_parts.append(f"Descrição IA: {ai_desc.strip()}")
    if tech_desc and isinstance(tech_desc, str) and tech_desc.strip():
        description_parts.append(f"Descrição Técnica: {tech_desc.strip()}")

    # Adiciona o bloco de descrições combinadas se alguma foi encontrada
    if description_parts:
        combined_desc = "\n  ".join(description_parts) # Adiciona indentação
        parts.append(f"Descrições:\n  {combined_desc}")

    # Adiciona Notas de Mapeamento se existirem e não estiverem vazias
    if column_data.get('value_mapping_notes'):
        value_notes = column_data['value_mapping_notes']
        if value_notes and isinstance(value_notes, str) and value_notes.strip():
             parts.append(f"Notas Mapeamento: {value_notes.strip()}")

    # Adicionar contexto de FK, se aplicável
    if column_data.get('is_fk') and isinstance(column_data.get('fk_references'), dict):
        ref_table = column_data['fk_references'].get('references_table')
        ref_column = column_data['fk_references'].get('references_column')
        if ref_table and ref_column:
            parts.append(f"Relação FK: Referencia a tabela '{ref_table}' (coluna '{ref_column}')")

    # Adicionar valores de exemplo
    sample_values = column_data.get('sample_values', [])
    if sample_values:
        num_samples = len(sample_values)
        if isinstance(sample_values, list):
            samples_to_include = sample_values[:max(MIN_SAMPLE_VALUES, min(num_samples, MAX_SAMPLE_VALUES))]
            # Remove valores None e converte para string antes de juntar
            valid_samples = [str(s) for s in samples_to_include if s is not None]
            if valid_samples:
                parts.append(f"Valores Exemplo ({len(valid_samples)} de {num_samples} originais): {', '.join(valid_samples)}")
        else:
            logger.warning(f"'sample_values' para {table_name}.{col_name} não é uma lista, ignorando exemplos.")

    return "\n".join(parts)
# --- FIM DA FUNÇÃO MODIFICADA ---

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
        faiss.write_index(index, str(file_path))
        logger.info(f"Índice FAISS com {index.ntotal} vetores salvo com sucesso.")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar índice FAISS em '{file_path}': {e}", exc_info=True)
        return False

def generate_single_embedding(client, text_to_embed, table_name, col_index, col_name_for_log):
    """Função worker para paralelismo."""
    try:
        response = client.embeddings(model=OLLAMA_EMBED_MODEL, prompt=text_to_embed)
        if 'embedding' in response and isinstance(response['embedding'], list):
            logger.debug(f"Embedding gerado para {table_name}.{col_name_for_log}")
            return table_name, col_index, response['embedding']
        else:
            logger.warning(f"Resposta inesperada do Ollama para {table_name}.{col_name_for_log}. Resposta: {response}")
            return table_name, col_index, None
    except Exception as e:
        logger.error(f"Erro ao gerar embedding para {table_name}.{col_name_for_log}: {e}", exc_info=False)
        return table_name, col_index, None

def main():
    logger.info("--- Iniciando Geração de Embeddings e Índice FAISS (Paralelo / Descrições Combinadas) --- ")
    overall_start_time = time.time()

    # 1. Carregar Schema Mesclado
    merged_schema_path = Path(MERGED_SCHEMA_FOR_EMBEDDINGS_FILE)
    schema_data = load_json_file(merged_schema_path, "Schema Mesclado")
    if schema_data is None:
        logger.critical("Não foi possível carregar o schema mesclado. Abortando.")
        sys.exit(1)

    schema_with_embeddings = json.loads(json.dumps(schema_data)) # Cópia profunda

    # 2. Inicializar Cliente Ollama
    try:
        client = ollama.Client()
        client.list()
        logger.info(f"Cliente Ollama inicializado. Modelo: {OLLAMA_EMBED_MODEL}")
    except Exception as e:
        logger.critical(f"Falha ao conectar ao cliente Ollama: {e}. Verifique se o Ollama está rodando.", exc_info=True)
        sys.exit(1)

    # 3. Coletar Textos e Referências (Preparar tarefas)
    logger.info("Coletando textos (combinando descrições) e referências das colunas...")
    tasks_data = []
    total_columns_estimate = 0
    if isinstance(schema_with_embeddings, dict):
        for table_name_orig, table_data in schema_with_embeddings.items():
            if isinstance(table_data, dict) and 'columns' in table_data and isinstance(table_data['columns'], list):
                total_columns_estimate += len(table_data['columns'])
                for col_index, column_data in enumerate(table_data['columns']):
                    # Chama a função build_column_text MODIFICADA
                    text_to_embed = build_column_text(table_name_orig, column_data)
                    col_name_for_log = column_data.get('name', 'N/A')
                    tasks_data.append((text_to_embed, table_name_orig, col_index, col_name_for_log))
            else:
                 logger.warning(f"Ignorando '{table_name_orig}': estrutura inválida.")
    else:
         logger.error("Schema mesclado não é um dicionário.")
         sys.exit(1)

    logger.info(f"{len(tasks_data)} colunas preparadas para geração de embeddings.")

    # 4. Gerar Embeddings em Paralelo
    logger.info(f"Iniciando geração paralela com {NUM_WORKERS} workers...")
    results_map = {}
    futures_map = {}
    processed_columns_count = 0
    total_generation_errors = 0
    embed_loop_start = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for text, table, idx, name in tasks_data:
            future = executor.submit(generate_single_embedding, client, text, table, idx, name)
            futures_map[future] = (table, idx)

        for future in concurrent.futures.as_completed(futures_map):
            table_name, col_index = futures_map[future]
            try:
                res_table, res_idx, embedding_vector = future.result()
                results_map[(res_table, res_idx)] = embedding_vector
                if embedding_vector is None:
                    total_generation_errors += 1
            except Exception as exc:
                logger.error(f'Erro em {table_name} (col idx {col_index}): {exc}')
                total_generation_errors += 1
                results_map[(table_name, col_index)] = None

            processed_columns_count += 1
            if processed_columns_count % 100 == 0:
                 logger.info(f"Processado {processed_columns_count}/{len(tasks_data)} embeddings...")

    embed_loop_end = time.time()
    logger.info(f"Geração de embeddings concluída em {embed_loop_end - embed_loop_start:.2f}s.")
    logger.info(f"Total processado: {processed_columns_count}. Erros: {total_generation_errors}")

    # 5. Pós-processamento: Atualizar Schema e Preparar para FAISS
    logger.info("Atualizando schema com embeddings gerados...")
    embeddings_list_for_faiss = []
    update_count = 0
    if isinstance(schema_with_embeddings, dict):
         for table_name_orig, table_data in schema_with_embeddings.items():
             if isinstance(table_data, dict) and 'columns' in table_data and isinstance(table_data['columns'], list):
                 for col_index, column_data in enumerate(table_data['columns']):
                     embedding_vector = results_map.get((table_name_orig, col_index))
                     # Atribui o vetor de embedding (ou None se houve erro)
                     column_data['embedding'] = embedding_vector
                     if embedding_vector is not None:
                         # Adiciona à lista para FAISS apenas se o embedding foi gerado com sucesso
                         embeddings_list_for_faiss.append(embedding_vector)
                         update_count += 1
                     else:
                         logger.warning(f"Resultado não encontrado para {table_name_orig} col idx {col_index}")

    logger.info(f"Schema atualizado com {update_count} embeddings.")

    # 6. Construir e Salvar Índice FAISS
    faiss_index = None
    faiss_save_success = False
    if embeddings_list_for_faiss:
        logger.info("Construindo índice FAISS...")
        try:
            embeddings_np = np.array(embeddings_list_for_faiss).astype('float32')
            dimension = embeddings_np.shape[1]
            if dimension != EMBEDDING_DIMENSION:
                logger.warning(f"Dimensão real do embedding ({dimension}) difere da configurada ({EMBEDDING_DIMENSION})! Ajuste config.py.")
            
            faiss_index = faiss.IndexFlatL2(dimension)
            faiss_index.add(embeddings_np)
            logger.info(f"Índice FAISS construído com {faiss_index.ntotal} vetores.")
            
            # Salvar Índice FAISS
            faiss_output_path = Path(FAISS_INDEX_FILE)
            faiss_save_success = save_faiss_index(faiss_index, faiss_output_path)
            
        except Exception as e:
            logger.error(f"Erro ao construir ou salvar o índice FAISS: {e}", exc_info=True)
            faiss_save_success = False # Garante que o status seja Falha
    else:
        logger.warning("Nenhum embedding gerado com sucesso. Índice FAISS não será criado/salvo.")

    # 7. Salvar Schema com Embeddings
    schema_output_path = Path(EMBEDDED_SCHEMA_FILE)
    schema_save_success = save_json_file(schema_with_embeddings, schema_output_path, "Schema com Embeddings")

    overall_end_time = time.time()
    total_duration = overall_end_time - overall_start_time
    logger.info(f"--- Script Concluído em {total_duration:.2f} segundos --- ")

    # --- NOVO: Imprimir Resumo --- #
    print("\n--- Resumo da Geração de Embeddings e Índice FAISS --- ")
    print(f"Schema de Entrada: {merged_schema_path}")
    print(f"Modelo Embedding: {OLLAMA_EMBED_MODEL}")
    print(f"Dimensão Esperada: {EMBEDDING_DIMENSION}")
    print(f"Colunas Preparadas: {len(tasks_data)}")
    print(f"Colunas Processadas: {processed_columns_count}")
    print(f"Embeddings Gerados com Sucesso: {update_count}")
    print(f"Erros na Geração: {total_generation_errors}")
    print(f"Duração Geração Embeddings: {embed_loop_end - embed_loop_start:.2f}s")
    print("-" * 20)
    print(f"Índice FAISS Construído: {'Sim' if faiss_index and faiss_index.ntotal > 0 else 'Não'}")
    if faiss_index and faiss_index.ntotal > 0:
        print(f"  - Vetores no Índice: {faiss_index.ntotal}")
        print(f"  - Arquivo Índice: {faiss_output_path}")
        print(f"  - Status Salvamento Índice: {'Sucesso' if faiss_save_success else 'FALHA'}")
    print(f"Arquivo Schema c/ Embeddings: {schema_output_path}")
    print(f"Status Salvamento Schema: {'Sucesso' if schema_save_success else 'FALHA'}")
    print("-" * 20)
    print(f"Duração Total do Script: {total_duration:.2f}s")
    print("------------------------------------------------------\n")

    # Sair com erro se algum salvamento falhou
    if not schema_save_success or (faiss_index and faiss_index.ntotal > 0 and not faiss_save_success):
        sys.exit(1)

if __name__ == "__main__":
    # Poderia adicionar argparse aqui para configurar modelo, paths, workers, etc.
    main()