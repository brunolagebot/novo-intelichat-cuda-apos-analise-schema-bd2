# Funções para carregar dados (schema, metadados, contagens) 

import streamlit as st # Necessário para decorators de cache e st.session_state
import os
import json
import logging
from collections import OrderedDict
import time
import copy
import faiss # Importar faiss aqui

# Importar de outros módulos core
import src.core.config as config
from src.utils.json_helpers import load_json # Mantém

logger = logging.getLogger(__name__)

# --- Funções de Carregamento de Arquivos --- #

@st.cache_data # Cache para estrutura técnica (não muda na sessão)
def load_technical_schema(file_path):
    logger.info(f"---> EXECUTANDO load_technical_schema para: {file_path}") # Log de diagnóstico
    """Carrega o schema técnico (combinado) do arquivo JSON."""
    if not os.path.exists(file_path):
        st.error(f"Erro: Arquivo de schema técnico não encontrado em '{file_path}'")
        logger.error(f"Erro: Arquivo de schema técnico não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f, object_pairs_hook=OrderedDict)
            logger.info(f"Schema técnico carregado de {file_path}")
            return data
    except json.JSONDecodeError as e:
        st.error(f"Erro ao decodificar JSON do schema técnico {file_path}: {e}")
        logger.error(f"Erro ao decodificar JSON do schema técnico {file_path}: {e}", exc_info=True)
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao carregar schema técnico {file_path}: {e}")
        logger.error(f"Erro inesperado ao carregar schema técnico {file_path}: {e}", exc_info=True)
        return None

@st.cache_data # Cache para evitar recarregar a cada interação
def load_metadata(file_path):
    logger.info(f"---> EXECUTANDO load_metadata para: {file_path}") # Log de diagnóstico
    """Carrega o arquivo JSON de metadados."""
    if not os.path.exists(file_path):
        # Log warning em vez de error, pois pode ser iniciado vazio
        logger.warning(f"Arquivo de metadados {file_path} não encontrado ou ainda não criado. Retornando dict vazio.")
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Usar OrderedDict para tentar manter a ordem original das chaves
            data = json.load(f, object_pairs_hook=OrderedDict)
            logger.info(f"Metadados carregados de {file_path}")
            return data
    except json.JSONDecodeError as e:
        st.error(f"Erro ao decodificar JSON do arquivo {file_path}: {e}")
        logger.error(f"Erro ao decodificar JSON do arquivo {file_path}: {e}", exc_info=True)
        return {}
    except IOError as e:
        st.error(f"Erro ao ler o arquivo {file_path}: {e}")
        logger.error(f"Erro ao ler o arquivo {file_path}: {e}", exc_info=True)
        return {}
    except Exception as e:
        st.error(f"Erro inesperado ao carregar {file_path}: {e}")
        logger.exception(f"Erro inesperado ao carregar {file_path}")
        return {}

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
            logger.error(f"Erro inesperado ao carregar contagens: {e}", exc_info=True)
            return {}
    else:
        logger.info(f"Arquivo de contagens '{file_path}' não encontrado. Contagens não serão exibidas.")
        return {}

# --- NOVA Função Cacheada para Carregar FAISS --- #
@st.cache_resource(ttl=3600) # Cache do índice por 1 hora (ou ajuste conforme necessário)
def load_faiss_index(index_path):
    logger.info(f"---> EXECUTANDO load_faiss_index para: {index_path}")
    """Carrega o índice FAISS do arquivo especificado."""
    if not os.path.exists(index_path):
        logger.warning(f"Arquivo de índice FAISS não encontrado em '{index_path}'. Retornando None.")
        return None
    try:
        start_time = time.time()
        index = faiss.read_index(index_path)
        end_time = time.time()
        logger.info(f"Índice FAISS carregado de {index_path} em {end_time - start_time:.2f}s. Vetores: {index.ntotal}")
        return index
    except Exception as e:
        st.error(f"Erro ao carregar índice FAISS de '{index_path}': {e}")
        logger.error(f"Erro ao carregar índice FAISS de '{index_path}': {e}", exc_info=True)
        return None

# --- NOVA Função Cacheada para Carregar Análise de Chaves --- #
@st.cache_data
def load_key_analysis_results(file_path):
    logger.info(f"---> EXECUTANDO load_key_analysis_results para: {file_path}")
    """Carrega os resultados pré-calculados da análise de chaves."""
    # Retorna um dicionário com chaves esperadas mesmo em caso de falha,
    # para evitar KeyErrors posteriores.
    default_result = {
        "composite_pk_tables": {},
        "junction_tables": {},
        "composite_fk_details": {},
        "column_roles": {}
    }
    if not os.path.exists(file_path):
        logger.warning(f"Arquivo de resultados da análise de chaves não encontrado em '{file_path}'. Usando resultados vazios.")
        return default_result
    try:
        start_time = time.time()
        # Usa a função load_json importada que já tem tratamento de erro
        data = load_json(file_path)
        end_time = time.time()
        if data is None:
             logger.error(f"Falha ao carregar ou decodificar JSON de '{file_path}'. Usando resultados vazios.")
             return default_result
        else:
            # Verifica se as chaves esperadas existem (opcional, mas bom)
            missing_keys = set(default_result.keys()) - set(data.keys())
            if missing_keys:
                 logger.warning(f"Arquivo {file_path} carregado, mas faltam chaves: {missing_keys}. Usando defaults para chaves ausentes.")
                 # Preenche chaves ausentes com default
                 for key in missing_keys:
                     data[key] = default_result[key]
                     
            logger.info(f"Resultados da análise de chaves carregados de {file_path} em {end_time - start_time:.2f}s.")
            return data
    except Exception as e:
        st.error(f"Erro inesperado ao carregar resultados da análise de chaves de '{file_path}': {e}")
        logger.error(f"Erro inesperado ao carregar resultados da análise de chaves de '{file_path}': {e}", exc_info=True)
        return default_result

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
        try:
            progress_bar.progress(progress_value, text=f"({current_step}/{total_steps}) {step_name} concluída em {duration:.2f}s...")
        except st.runtime.scriptrunner.ScriptStoppException:
             logger.warning(f"Script parado durante {step_name}")
             raise # Re-raise para parar a execução
        except Exception as e:
             logger.error(f"Erro ao atualizar barra de progresso em {step_name}: {e}")
        logger.info(f"Etapa '{step_name}' concluída em {duration:.2f}s")

    # --- Determinar qual schema carregar inicialmente --- #
    use_embeddings_on_load = st.session_state.get('use_embeddings', False)
    if use_embeddings_on_load:
        schema_file_to_load = config.EMBEDDED_SCHEMA_FILE
        faiss_index_file_to_load = config.FAISS_INDEX_FILE
        analysis_file_to_load = config.KEY_ANALYSIS_RESULTS_FILE
        logger.info(f"Carregamento inicial: Usando schema com embeddings ({schema_file_to_load}), índice FAISS ({faiss_index_file_to_load}) e análise pré-calculada ({analysis_file_to_load}).")
    else:
        schema_file_to_load = config.TECHNICAL_SCHEMA_FILE # Ajuste se o nome for diferente
        faiss_index_file_to_load = None # Não carrega FAISS se não usar embeddings
        analysis_file_to_load = None # Não carrega análise se não usar embeddings
        logger.info(f"Carregamento inicial: Usando schema base ({schema_file_to_load}).")

    # --- Etapa 1: Carregar Schema (Base ou com Embeddings) --- #
    step_name = "Carregando Schema"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    schema_loaded = load_technical_schema(schema_file_to_load)
    if schema_loaded is None:
        st.error(f"Falha crítica: Não foi possível carregar o arquivo de schema em '{schema_file_to_load}'.")
        st.stop()
    update_progress(step_name, start_time_step)

    # --- Etapa 2: Carregar Metadados --- #
    step_name = "Carregando Metadados"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    metadata_dict = load_metadata(config.METADATA_FILE)
    if metadata_dict is None: # load_metadata agora retorna {} em caso de erro/não encontrado
        metadata_dict = {}
    update_progress(step_name, start_time_step)

    # --- Etapa 3: Carregar Contagens da Visão Geral --- #
    step_name = "Carregando Contagens (Cache)"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    overview_counts = load_overview_counts(config.OVERVIEW_COUNTS_FILE)
    update_progress(step_name, start_time_step)

    # --- Etapa 4: CARREGAR Índice FAISS (se aplicável) --- #
    step_name = "Carregando Índice FAISS"
    start_time_step = time.time()
    faiss_index = None # Inicializa como None
    if faiss_index_file_to_load:
        progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
        faiss_index = load_faiss_index(faiss_index_file_to_load)
        # Se o carregamento falhar, faiss_index será None
    else:
        logger.info("Índice FAISS não será carregado (embeddings desativados).")
    update_progress(step_name, start_time_step)
    # TODO: O `index_to_key_map` precisa ser gerado/carregado separadamente ou junto com o índice FAISS.
    # Assumindo por agora que ele pode ser derivado do `schema_loaded` se embeddings estiverem presentes.
    index_to_key_map = None # Precisa implementar a lógica para gerar/carregar isso

    # --- Etapa 5: Analisar Estrutura de Chaves (usa schema carregado) --- #
    step_name = "Analisando Estrutura de Chaves"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    # A função analyze_key_structure já está cacheada em analysis.py
    key_analysis_result = None # Inicializa
    if analysis_file_to_load:
        progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
        key_analysis_result = load_key_analysis_results(analysis_file_to_load)
        # load_key_analysis_results retorna default em caso de erro
    else:
        logger.info("Análise de chaves pré-calculada não será carregada.")
        # Opcional: Calcular análise para schema base se necessário aqui?
        # key_analysis_result = analyze_key_structure(schema_loaded) # Mas isso pode ser lento
        # Por enquanto, deixamos None se não usar embeddings
        key_analysis_result = load_key_analysis_results(None) # Chama com None para obter default vazio
        
    update_progress(step_name, start_time_step)

    # --- Etapa 6: Inicializar Estado da Sessão --- #
    step_name = "Inicializando Estado da Sessão"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")

    # Inicializa estados
    if 'auto_save_enabled' not in st.session_state:
        st.session_state.auto_save_enabled = False
    if 'last_save_time' not in st.session_state:
        st.session_state.last_save_time = time.time()
    if 'use_embeddings' not in st.session_state:
        st.session_state.use_embeddings = False
    if 'initial_metadata' not in st.session_state or st.session_state.get("_force_reload", False):
        logger.info("Armazenando estado inicial dos metadados (ou forçando reload).")
        try:
            st.session_state.initial_metadata = copy.deepcopy(metadata_dict)
        except Exception as e:
            logger.error(f"Erro ao fazer deepcopy dos metadados iniciais: {e}")
            st.session_state.initial_metadata = {}
        st.session_state._force_reload = False # Reseta o flag

    # Garante que o estado use_embeddings está sincronizado com o que foi carregado
    st.session_state.use_embeddings = use_embeddings_on_load

    # Armazena dados carregados no estado da sessão
    st.session_state.metadata = metadata_dict
    st.session_state.technical_schema = schema_loaded
    st.session_state.overview_counts = overview_counts if overview_counts else {}
    st.session_state.faiss_index = faiss_index # Salva o índice carregado (ou None)
    st.session_state.index_to_key_map = index_to_key_map # Salva o mapa (ou None)
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
        try:
            db_password_loaded = st.secrets.get("database", {}).get("password")
            if not db_password_loaded:
                db_password_loaded = os.getenv("FIREBIRD_PASSWORD")
                if db_password_loaded:
                    logger.info("Senha do DB carregada da variável de ambiente FIREBIRD_PASSWORD.")
                else:
                    logger.warning("Senha do DB não encontrada em st.secrets nem na env var FIREBIRD_PASSWORD.")
                    db_password_loaded = ""
            else:
                logger.info("Senha do DB carregada de st.secrets.")
            st.session_state.db_password = db_password_loaded
        except Exception as e:
            logger.error(f"Erro ao tentar carregar senha do DB de st.secrets/env var: {e}")
            st.session_state.db_password = ""
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