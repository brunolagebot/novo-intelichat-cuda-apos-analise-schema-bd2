#!/usr/bin/env python
# coding: utf-8

import os
import json
import argparse
import logging
from collections import OrderedDict
import time
import faiss
import numpy as np
import torch
from sentence_transformers import SentenceTransformer
import sys

# --- Adiciona a raiz do projeto ao sys.path ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM --- 

from src.utils.json_helpers import load_json, save_json
from src.core.logging_config import setup_logging

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Constantes e Configurações Padrão --- 
DEFAULT_INPUT_ENRICHED_PATH = "data/processed/schema_enriched_for_embedding.json"
DEFAULT_OUTPUT_EMBEDDING_PATH = "data/embeddings/schema_with_enriched_embeddings_latest.json"
DEFAULT_OUTPUT_FAISS_PATH = "data/embeddings/faiss_index_enriched_latest.idx"
DEFAULT_EMBEDDING_MODEL = "all-mpnet-base-v2"

def main(args):
    script_start_time = time.time()
    logger.info("--- Iniciando Script: Geração de Embeddings e FAISS a partir de Schema Enriquecido ---")

    # --- Carregar Dados Enriquecidos --- 
    logger.info(f"Carregando Schema Enriquecido de: {args.input_enriched_json}")
    enriched_schema = load_json(args.input_enriched_json)
    if not enriched_schema:
        logger.critical("Falha ao carregar schema enriquecido. Abortando.")
        return

    # --- Extrair Textos para Embedding --- 
    texts_for_embedding = []
    embedding_map_keys = [] # Guarda (obj_name, col_name) para mapear embedding de volta
    output_data = OrderedDict() # Estrutura para salvar JSON final com embeddings

    logger.info("Extraindo textos para embedding do schema enriquecido...")
    cols_to_embed = 0
    for obj_name, obj_data in enriched_schema.items():
        if not isinstance(obj_data, dict) or 'columns' not in obj_data:
            continue
        
        # Copia dados do objeto para a saída (exceto colunas inicialmente)
        output_data[obj_name] = OrderedDict(
            [(k, v) for k, v in obj_data.items() if k != 'columns'] 
        )
        output_data[obj_name]['columns'] = []

        for col_data in obj_data.get('columns', []):
            text_to_embed = col_data.get('text_for_embedding')
            col_name = col_data.get('name')

            if text_to_embed and col_name:
                texts_for_embedding.append(text_to_embed)
                embedding_map_keys.append((obj_name, col_name))
                # Adiciona a coluna à saída, mas sem o embedding ainda
                output_col_data = OrderedDict(col_data.items())
                output_col_data['embedding'] = None # Placeholder
                output_data[obj_name]['columns'].append(output_col_data)
                cols_to_embed += 1
            else:
                logger.warning(f"Coluna {obj_name}.{col_name} sem texto para embedding ou nome. Pulando.")
                # Adiciona a coluna à saída mesmo sem embedding, para manter a estrutura
                output_data[obj_name]['columns'].append(OrderedDict(col_data.items())) 


    logger.info(f"Total de {cols_to_embed} textos extraídos para embedding.")
    if not texts_for_embedding:
        logger.error("Nenhum texto encontrado para gerar embeddings. Verifique o arquivo JSON de entrada.")
        return

    # --- Gerar Embeddings --- 
    logger.info(f"Carregando modelo de embedding: {args.embedding_model}")
    try:
        device = 'cuda' if torch.cuda.is_available() else 'cpu'
        model = SentenceTransformer(args.embedding_model, device=device)
        logger.info(f"Modelo carregado com sucesso em: {device}")
    except Exception as e:
        logger.critical(f"Falha ao carregar modelo de embedding '{args.embedding_model}': {e}", exc_info=True)
        return

    logger.info(f"Gerando embeddings para {len(texts_for_embedding)} textos...")
    embedding_start_time = time.time()
    try:
        embeddings_np = model.encode(texts_for_embedding, convert_to_numpy=True, show_progress_bar=True)
        embedding_duration = time.time() - embedding_start_time
        logger.info(f"Embeddings gerados em {embedding_duration:.2f} segundos.")
    except Exception as e:
        logger.critical(f"Erro durante a geração dos embeddings: {e}", exc_info=True)
        return
        
    embedding_dim = embeddings_np.shape[1]
    logger.info(f"Dimensão dos embeddings gerados: {embedding_dim}")

    # --- Adicionar Embeddings ao JSON de Saída --- 
    logger.info("Adicionando embeddings gerados ao JSON de saída...")
    output_embedding_count = 0
    embedding_dict = {key: emb for key, emb in zip(embedding_map_keys, embeddings_np)}

    for obj_name, obj_data_out in output_data.items():
        if 'columns' in obj_data_out:
            for col_data_out in obj_data_out['columns']:
                col_name = col_data_out.get('name')
                if col_name:
                    # Usar a chave composta para buscar o embedding correto
                    emb = embedding_dict.get((obj_name, col_name))
                    if emb is not None:
                        col_data_out['embedding'] = emb.tolist() # Converte para lista para JSON
                        output_embedding_count += 1
                    else:
                        # Se não achou embedding (pq foi pulado antes), garante que é None
                         col_data_out['embedding'] = None 

    logger.info(f"{output_embedding_count} embeddings adicionados ao JSON final.")

    # --- Salvar Arquivo JSON com Novos Embeddings --- 
    logger.info(f"Salvando JSON com novos embeddings em: {args.output_embedding_json}")
    if save_json(output_data, args.output_embedding_json):
        logger.info("JSON com embeddings salvo com sucesso.")
    else:
        logger.error("Falha ao salvar JSON com embeddings.")
        # Não abortar, tentar salvar o FAISS mesmo assim

    # --- Construir e Salvar Índice FAISS --- 
    if output_embedding_count > 0:
        logger.info("Construindo índice FAISS...")
        try:
            # Garantir que temos apenas os embeddings válidos para o índice
            valid_embeddings_np = embeddings_np[np.arange(len(embedding_map_keys))] 
            
            index = faiss.IndexFlatL2(embedding_dim)
            index.add(valid_embeddings_np)
            logger.info(f"Índice FAISS construído com {index.ntotal} vetores (Dim: {embedding_dim}).")

            logger.info(f"Salvando índice FAISS em: {args.output_faiss_index}")
            output_dir_faiss = os.path.dirname(args.output_faiss_index)
            if output_dir_faiss:
                os.makedirs(output_dir_faiss, exist_ok=True)
            faiss.write_index(index, args.output_faiss_index)
            logger.info("Índice FAISS salvo com sucesso.")

        except Exception as e:
            logger.error(f"Erro ao construir ou salvar o índice FAISS: {e}", exc_info=True)
    else:
        logger.warning("Nenhum embedding foi gerado, índice FAISS não será criado.")

    script_duration = time.time() - script_start_time
    logger.info(f"--- Script Concluído em {script_duration:.2f} segundos ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gera embeddings e índice FAISS a partir de um JSON de schema enriquecido.")
    parser.add_argument("--input_enriched_json", default=DEFAULT_INPUT_ENRICHED_PATH, help=f"Caminho para o JSON de schema enriquecido (entrada). Padrão: {DEFAULT_INPUT_ENRICHED_PATH}")
    parser.add_argument("--output_embedding_json", default=DEFAULT_OUTPUT_EMBEDDING_PATH, help=f"Caminho para salvar o JSON final com os novos embeddings. Padrão: {DEFAULT_OUTPUT_EMBEDDING_PATH}")
    parser.add_argument("--output_faiss_index", default=DEFAULT_OUTPUT_FAISS_PATH, help=f"Caminho para salvar o novo índice FAISS. Padrão: {DEFAULT_OUTPUT_FAISS_PATH}")
    parser.add_argument("--embedding_model", default=DEFAULT_EMBEDDING_MODEL, help=f"Nome do modelo Sentence Transformer a ser usado. Padrão: {DEFAULT_EMBEDDING_MODEL}")

    args = parser.parse_args()

    # Cria diretórios de saída se não existirem
    output_dir_json = os.path.dirname(args.output_embedding_json)
    if output_dir_json:
        os.makedirs(output_dir_json, exist_ok=True)
    output_dir_faiss = os.path.dirname(args.output_faiss_index)
    if output_dir_faiss:
        os.makedirs(output_dir_faiss, exist_ok=True)

    main(args) 