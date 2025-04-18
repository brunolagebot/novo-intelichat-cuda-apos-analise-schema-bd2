# Funções para carregar dados (schema, metadados, contagens) 

import streamlit as st # Necessário para decorators de cache e st.session_state
import os
import json
import logging
from collections import OrderedDict
import time
import copy
import numpy as np # Necessário para build_faiss_index

# Importar de outros módulos core
import src.core.config as config
from src.core.ai_integration import build_faiss_index # Será movido eventualmente?
from src.core.analysis import analyze_key_structure # Será movido eventualmente?
from src.utils.json_helpers import load_json # Mantém

logger = logging.getLogger(__name__)

# --- Funções de Carregamento de Arquivos --- #

@st.cache_data # Cache para estrutura técnica (não muda na sessão)
def load_technical_schema(file_path):
    logger.info(f"---> EXECUTANDO load_technical_schema para: {file_path}") # Log de diagnóstico
    """Carrega o schema técnico (combinado) do arquivo JSON."""
    if not os.path.exists(file_path):
        st.error(f"Erro: Arquivo de schema técnico não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f, object_pairs_hook=OrderedDict)
            logger.info(f"Schema técnico carregado de {file_path}")
            return data
    except json.JSONDecodeError as e:
        st.error(f"Erro ao decodificar JSON do schema técnico {file_path}: {e}")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao carregar schema técnico {file_path}: {e}")
        return None

@st.cache_data # Cache para evitar recarregar a cada interação
def load_metadata(file_path):
    logger.info(f"---> EXECUTANDO load_metadata para: {file_path}") # Log de diagnóstico
    """Carrega o arquivo JSON de metadados."""
    if not os.path.exists(file_path):
        st.error(f"Erro: Arquivo de metadados não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Usar OrderedDict para tentar manter a ordem original das chaves
            data = json.load(f, object_pairs_hook=OrderedDict)
            logger.info(f"Metadados carregados de {file_path}")
            return data
    except json.JSONDecodeError as e:
        st.error(f"Erro ao decodificar JSON do arquivo {file_path}: {e}")
        return None
    except IOError as e:
        st.error(f"Erro ao ler o arquivo {file_path}: {e}")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao carregar {file_path}: {e}")
        logger.exception(f"Erro inesperado ao carregar {file_path}")
        return None

@st.cache_data # Cache para contagens (não devem mudar frequentemente sem ação externa)
def load_overview_counts(file_path):
    """Carrega as contagens e timestamps da visão geral."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.warning(f"Aviso: Arquivo de contagens '{file_path}' inválido.")
            return {}
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar contagens: {e}")
            return {}
    else:
        logger.info(f"Arquivo de contagens '{file_path}' não encontrado. Contagens não serão exibidas.")
        return {}

# --- Função Principal de Carregamento e Processamento --- #

def load_and_process_data():
    # --- Configuração da Barra de Progresso e Tempos --- #
    total_steps = 6
    progress_bar = st.progress(0.0, text="Iniciando carregamento...")
    start_time_total = time.time()
    step_times = {}
    current_step = 0

    def update_progress(step_name, step_start_time):
        nonlocal current_step
        duration = time.time() - step_start_time
        step_times[step_name] = duration
        current_step += 1
        progress_value = float(current_step) / total_steps
        progress_bar.progress(progress_value, text=f"({current_step}/{total_steps}) {step_name} concluída em {duration:.2f}s...")
        logger.info(f"Etapa '{step_name}' concluída em {duration:.2f}s")

    # --- Etapa 1: Carregar Schema Base --- #
    step_name = "Carregando Schema Base"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    schema_base = load_technical_schema(config.TECHNICAL_SCHEMA_FILE)
    if schema_base is None:
        st.error(f"Falha crítica: Não foi possível carregar o arquivo de schema técnico base obrigatório em '{config.TECHNICAL_SCHEMA_FILE}'.")
        st.stop()
    update_progress(step_name, start_time_step) # Movido para depois do check

    # --- Etapa 2: Carregar Metadados --- #
    step_name = "Carregando Metadados"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    metadata_dict = load_metadata(config.METADATA_FILE)
    if metadata_dict is None:
        metadata_dict = {"TABLES": {}, "VIEWS": {}}
    update_progress(step_name, start_time_step)

    # --- Etapa 3: Carregar Contagens da Visão Geral --- #
    step_name = "Carregando Contagens (Cache)"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    overview_counts = load_overview_counts(config.OVERVIEW_COUNTS_FILE)
    update_progress(step_name, start_time_step)

    # --- Etapa 4: Construir Índice FAISS (Base) --- #
    step_name = "Construindo Índice FAISS (Base)"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    faiss_index, index_to_key_map = build_faiss_index(schema_base)
    update_progress(step_name, start_time_step)

    # --- Etapa 5: Analisar Estrutura de Chaves (Base) --- #
    step_name = "Analisando Estrutura de Chaves (Base)"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    key_analysis_result = analyze_key_structure(schema_base)
    update_progress(step_name, start_time_step)

    # --- Etapa 6: Inicializar Estado da Sessão --- #
    step_name = "Inicializando Estado da Sessão"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")

    # Inicializa estados (mantidos aqui por enquanto pois usam st.session_state)
    if 'auto_save_enabled' not in st.session_state:
        st.session_state.auto_save_enabled = False
    if 'last_save_time' not in st.session_state:
        st.session_state.last_save_time = time.time()
    if 'use_embeddings' not in st.session_state:
        st.session_state.use_embeddings = False
    if 'initial_metadata' not in st.session_state:
        logger.info("Armazenando estado inicial dos metadados.")
        try:
            st.session_state.initial_metadata = copy.deepcopy(metadata_dict)
        except Exception as e:
            logger.error(f"Erro ao fazer deepcopy dos metadados iniciais: {e}")
            st.session_state.initial_metadata = {}

    # Armazena dados carregados no estado da sessão
    if 'metadata' not in st.session_state:
        st.session_state.metadata = metadata_dict
    if 'technical_schema' not in st.session_state:
        st.session_state.technical_schema = schema_base
    if 'overview_counts' not in st.session_state:
        st.session_state.overview_counts = overview_counts if overview_counts else {}
    if 'faiss_index' not in st.session_state:
         st.session_state.faiss_index = faiss_index
    if 'index_to_key_map' not in st.session_state:
         st.session_state.index_to_key_map = index_to_key_map
    if 'key_analysis' not in st.session_state:
        st.session_state.key_analysis = key_analysis_result

    # Inicializa estados da UI (ainda precisam ser acessados pelo app principal)
    if 'unsaved_changes' not in st.session_state:
        st.session_state.unsaved_changes = False
    if 'current_view' not in st.session_state:
        st.session_state.current_view = 'overview'
    if 'selected_object' not in st.session_state:
        st.session_state.selected_object = None
    if 'selected_column_index' not in st.session_state:
        st.session_state.selected_column_index = None
    if 'selected_object_type' not in st.session_state:
        st.session_state.selected_object_type = None
    if 'ollama_enabled' not in st.session_state:
        st.session_state.ollama_enabled = False
    if 'db_path' not in st.session_state:
        st.session_state.db_path = config.DEFAULT_DB_PATH
    if 'db_user' not in st.session_state:
        st.session_state.db_user = config.DEFAULT_DB_USER
    if 'db_password' not in st.session_state:
        st.session_state.db_password = os.getenv("FIREBIRD_PASSWORD", "")
    if 'db_charset' not in st.session_state:
        st.session_state.db_charset = config.DEFAULT_DB_CHARSET
    if 'latest_db_timestamp' not in st.session_state:
        st.session_state.latest_db_timestamp = None

    update_progress(step_name, start_time_step)

    # --- Finalização --- #
    total_time = time.time() - start_time_total
    progress_bar.empty() # Limpa a barra de progresso
    st.toast(f"Carregamento inicial concluído em {total_time:.2f}s!", icon="🎉")
    logger.info(f"Carregamento inicial concluído em {total_time:.2f}s.")
    with st.expander("Detalhes do Tempo de Carregamento Inicial", expanded=False):
        for name, duration in step_times.items():
            st.write(f"- {name}: {duration:.3f}s")
        st.write(f"**- Tempo Total:** {total_time:.3f}s") 