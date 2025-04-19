import streamlit as st
import json
import os
import logging
import re # NOVO: Para limpar tipo
from collections import OrderedDict, defaultdict # NOVO: defaultdict
import datetime # NOVO: Para timestamps
import pandas as pd # NOVO: Para o DataFrame da visão geral
import fdb # NOVO: Para conectar ao Firebird
import subprocess # NOVO: Para executar o script externo
import sys # NOVO: Para obter o executável python correto
import io # NOVO: Para manipulação de bytes em memória (Excel)
import numpy as np # NOVO: Para manipulação de vetores
import faiss # NOVO: Para busca por similaridade
import copy # NOVO: Para deepcopy
import time # NOVO: Para medir o tempo
import argparse # NOVO
import uuid # NOVO: Para IDs de mensagem
from src.utils.json_helpers import load_json, save_json # NOVO: Importa funções auxiliares
import src.core.config as config # ATUALIZADO: Importa configurações da pasta core
# NOVO: Importa funções de carregamento
from src.core.data_loader import load_and_process_data, load_metadata, load_overview_counts, load_technical_schema
# NOVO: Importa outras funções core (serão removidas/refatoradas depois)
from src.core.ai_integration import generate_ai_description, build_faiss_index, get_query_embedding, handle_embedding_toggle
from src.core.metadata_logic import get_type_explanation, find_existing_info, get_column_concept, apply_heuristics_globally, populate_descriptions_from_keys, compare_metadata_changes, save_metadata
from src.core.db_utils import fetch_latest_nfs_timestamp, fetch_sample_data
from src.core.analysis import generate_documentation_overview, analyze_key_structure
from ui.sidebar import display_sidebar # NOVO: Importa função da sidebar
from ui.overview_page import display_overview_page # NOVO: Importa função Visão Geral
from ui.edit_page import display_edit_page # NOVO: Importa função Editar Metadados
from ui.analysis_page import display_analysis_page # NOVO: Importa função Análise
from ui.chat_page import display_chat_page # NOVO: Importa função Chat

# NOVO: Configura o logging ANTES de qualquer outra coisa
from src.core.logging_config import setup_logging # ATUALIZADO
setup_logging()

# O logger agora será configurado pela função acima
logger = logging.getLogger(__name__)

# NOVO: Tentar importar a função de chat (lidar com erro se não existir)
try:
    from src.ollama_integration.client import chat_completion
    # NOVO: Tentar importar função de embedding
    try:
        from src.ollama_integration.client import get_embedding
        OLLAMA_EMBEDDING_AVAILABLE = True
        logger.info("Função de embedding Ollama (get_embedding) carregada.")
    except ImportError:
        OLLAMA_EMBEDDING_AVAILABLE = False
        logger.warning("Função get_embedding não encontrada em src.ollama_integration.client. Busca semântica no chat desabilitada.")
        def get_embedding(text): # Define dummy
            st.error("Função de embedding Ollama não encontrada.")
            return None

    OLLAMA_AVAILABLE = True
    logger.info("Integração Ollama carregada com sucesso.")
except ImportError:
    OLLAMA_AVAILABLE = False
    logger.warning("src.ollama_integration.client não encontrado. Funcionalidades de IA estarão desabilitadas.")
    # Define uma função dummy para evitar erros NameError
    def chat_completion(messages, stream=False):
        st.error("Integração Ollama não configurada/encontrada.")
        return None
except Exception as e:
    OLLAMA_AVAILABLE = False
    logger.error(f"Erro inesperado ao importar Ollama: {e}")
    def chat_completion(messages, stream=False):
        st.error(f"Erro na integração Ollama: {e}")
        return None

# CONSTANTES MOVIDAS PARA config.py

# --- Funções de Carregamento MOVIDAS para core/data_loader.py --- #

# --- Função save_metadata MOVIDA para core/metadata_logic.py --- #

# --- Função generate_ai_description MOVIDA para core/ai_integration.py --- #

# --- Função find_existing_info MOVIDA para core/metadata_logic.py --- #

# --- Função get_column_concept MOVIDA para core/metadata_logic.py --- #

# --- NOVAS Funções para Visão Geral --- #

# --- Função fetch_latest_nfs_timestamp MOVIDA para core/db_utils.py --- #

# --- Função fetch_sample_data MOVIDA para core/db_utils.py --- #

# --- Função apply_heuristics_globally MOVIDA para core/metadata_logic.py --- #

# --- Função populate_descriptions_from_keys MOVIDA para core/metadata_logic.py --- #

# --- Funções FAISS MOVIDAS para core/ai_integration.py (build_faiss_index, find_similar_columns) --- #
# build_faiss_index já removido
# REMOVE find_similar_columns definition

# --- Função get_query_embedding MOVIDA para core/ai_integration.py --- #

# --- Função compare_metadata_changes MOVIDA para core/metadata_logic.py --- #

# --- Funções de Análise Estrutural MOVIDAS para core/analysis.py --- #

# --- Função handle_embedding_toggle MOVIDA para core/ai_integration.py --- #

# --- Função Principal / Carregamento de Dados MOVIDA para core/data_loader.py --- #

# --- Interface Streamlit --- #
st.set_page_config(layout="wide", page_title="Editor de Metadados de Schema")

# --- Carregamento Inicial e Inicialização do Estado --- #
# Chama a função importada de data_loader
init_load_start = time.perf_counter()
logger.debug("Iniciando carregamento inicial e processamento de dados...")
load_and_process_data()
init_load_end = time.perf_counter()
logger.info(f"Carregamento inicial e processamento concluído em {init_load_end - init_load_start:.4f} segundos")
# --- FIM: Carregamento Inicial --- #

# --- Referência local aos dados no estado da sessão --- #
metadata_dict = st.session_state.metadata
technical_schema_data = st.session_state.technical_schema # NOVO: Usar do estado da sessão

# --- Lógica DB Credentials (para passar para as páginas) --- #
db_path = st.session_state.get('db_path', config.DEFAULT_DB_PATH)
db_user = st.session_state.get('db_user', config.DEFAULT_DB_USER)
db_password = st.session_state.get('db_password') # Senha já lida em load_and_process_data
db_charset = st.session_state.get('db_charset', config.DEFAULT_DB_CHARSET)

# --- Barra Lateral ---
# Chama a função da sidebar e obtém o modo selecionado
sidebar_start = time.perf_counter()
app_mode = display_sidebar(OLLAMA_AVAILABLE, technical_schema_data)
sidebar_end = time.perf_counter()
logger.debug(f"Renderização da Sidebar levou {sidebar_end - sidebar_start:.4f} segundos")

# --- Conteúdo Principal (Condicional ao Modo) ---
page_render_start = time.perf_counter()
logger.debug(f"Iniciando renderização da página: {app_mode}")

if app_mode == "Visão Geral":
    # Chama a função da página de Visão Geral, passando os dados necessários
    display_overview_page(
        technical_schema_data=technical_schema_data,
        metadata_dict=metadata_dict,
        db_path=db_path,
        db_user=db_user,
        db_password=db_password,
        db_charset=db_charset
    )

elif app_mode == "Editar Metadados":
    # Chama a função da página Editar Metadados
    display_edit_page(
        technical_schema_data=technical_schema_data,
        metadata_dict=metadata_dict,
        OLLAMA_AVAILABLE=OLLAMA_AVAILABLE,
        chat_completion=chat_completion,
        db_path=db_path,
        db_user=db_user,
        db_password=db_password,
        db_charset=db_charset
    )

elif app_mode == "Análise":
    # Chama a função da página de Análise
    display_analysis_page(technical_schema_data=technical_schema_data)

elif app_mode == "Chat com Schema":
    # Chama a função da página de Chat
    display_chat_page(
        OLLAMA_AVAILABLE=OLLAMA_AVAILABLE,
        chat_completion=chat_completion,
        OLLAMA_EMBEDDING_AVAILABLE=OLLAMA_EMBEDDING_AVAILABLE,
        get_embedding=get_embedding, # Passa a função de embedding (ou dummy)
        technical_schema_data=technical_schema_data,
        metadata_dict=metadata_dict # Passa metadados (embora use st.session_state)
    )

page_render_end = time.perf_counter()
logger.info(f"Renderização da página '{app_mode}' levou {page_render_end - page_render_start:.4f} segundos")

# --- LÓGICA DE AUTO-SAVE (Executa no final de cada rerun) ---
# Mantém a lógica de Auto-Save aqui, pois é global para o app
auto_save_check_start = time.perf_counter()
if st.session_state.get('auto_save_enabled', False):
    time_since_last_save = time.time() - st.session_state.get('last_save_time', 0)
    
    if time_since_last_save >= config.AUTO_SAVE_INTERVAL_SECONDS:
        logger.info(f"Verificando auto-save. Tempo desde último save: {time_since_last_save:.2f}s")
        # Verifica se há mudanças reais antes de salvar
        auto_save_desc_count, auto_save_notes_count = 0, 0
        auto_save_has_changes = False
        comp_start = time.perf_counter()
        if 'initial_metadata' in st.session_state:
            try:
                auto_save_desc_count, auto_save_notes_count = compare_metadata_changes(
                        st.session_state.initial_metadata,
                        st.session_state.metadata
                    )
                if auto_save_desc_count > 0 or auto_save_notes_count > 0:
                    auto_save_has_changes = True
            except Exception as e:
                logger.error(f"Erro ao comparar metadados para auto-save: {e}")
                auto_save_has_changes = False
        else:
            logger.warning("initial_metadata não encontrado no estado da sessão. Auto-save não pode verificar mudanças.")
            auto_save_has_changes = False
        comp_end = time.perf_counter()
        logger.debug(f"Comparação de metadados para auto-save levou {comp_end - comp_start:.4f} segundos.")
        
        if auto_save_has_changes:
            logger.info(f"Mudanças detectadas ({auto_save_desc_count} desc, {auto_save_notes_count} notes), iniciando auto-save...")
            save_start = time.perf_counter()
            if save_metadata(st.session_state.metadata, config.METADATA_FILE):
                save_end = time.perf_counter()
                logger.info(f"Auto-save concluído em {save_end - save_start:.4f} segundos.")
                try:
                    post_save_start = time.perf_counter()
                    # Limpar cache e atualizar estado após salvar
                    load_metadata.clear()
                    st.session_state.initial_metadata = copy.deepcopy(st.session_state.metadata)
                    st.session_state.last_save_time = time.time()
                    post_save_end = time.perf_counter()
                    logger.info(f"Pós-processamento do auto-save (cache/estado) levou {post_save_end - post_save_start:.4f}s. Tempo atualizado: {st.session_state.last_save_time}")
                    st.toast("Metadados salvos automaticamente.", icon="⏱️")
                except Exception as e:
                    logger.error(f"Erro durante pós-processamento do auto-save (limpeza de cache/atualização estado): {e}")
            else:
                save_end = time.perf_counter() # Mesmo se falhou, registra tempo
                logger.error(f"Falha no auto-save (função save_metadata retornou False). Tentativa levou {save_end - save_start:.4f} segundos.")
        else:
            logger.debug("Auto-save verificado, mas sem alterações pendentes ou erro na comparação.")

auto_save_check_end = time.perf_counter()
logger.debug(f"Verificação/execução de auto-save levou {auto_save_check_end - auto_save_check_start:.4f} segundos.")
# --- FIM: LÓGICA DE AUTO-SAVE ---