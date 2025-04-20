# Código da interface da barra lateral (Sidebar)

import streamlit as st
import os
import datetime
import subprocess
import sys
import time
import logging
import copy # Para deepcopy do estado inicial no reload

# Importações de módulos core
import src.core.config as config
from src.database.db_utils import fetch_latest_nfs_timestamp
from src.ollama_integration.ai_integration import handle_embedding_toggle
from src.core.metadata_logic import save_metadata, apply_heuristics_globally, populate_descriptions_from_keys, compare_metadata_changes
from src.core.data_loader import load_metadata, load_technical_schema # Para reload e merge

logger = logging.getLogger(__name__)

def display_sidebar(OLLAMA_AVAILABLE, technical_schema_data):
    """Renderiza a barra lateral e retorna o modo de operação selecionado."""

    st.sidebar.title("Navegação e Ações")

    # Seletor de Modo
    app_mode = st.sidebar.radio(
        "Modo de Operação",
        ["Editar Metadados", "Visão Geral", "Análise", "Chat com Schema"],
        key='app_mode_selector' # Mantém a chave para consistência do estado
    )
    st.sidebar.divider()

    # --- Exibição do Timestamp da Última NFS --- #
    st.sidebar.subheader("Referência Banco de Dados")
    db_path_for_ts = config.DEFAULT_DB_PATH
    db_user_for_ts = config.DEFAULT_DB_USER
    db_charset_for_ts = config.DEFAULT_DB_CHARSET

    # Lógica para obter a senha (mantida aqui para acesso a st.secrets/getenv)
    db_password_for_ts = None
    try:
        db_password_for_ts = st.secrets.get("database", {}).get("password")
        if not db_password_for_ts:
            db_password_for_ts = os.getenv("FIREBIRD_PASSWORD")
            if not db_password_for_ts:
                st.sidebar.error("Senha do banco Firebird não configurada (st.secrets ou FIREBIRD_PASSWORD).")
                # Não usar st.stop() aqui para não parar o app inteiro, apenas desabilitar ações dependentes
            else:
                # Aviso sutil, pode ser removido se desnecessário
                # st.sidebar.caption("Usando senha de FIREBIRD_PASSWORD.")
                pass 
    except Exception as e:
        st.sidebar.error(f"Erro ao obter senha do banco: {e}")
        logger.error(f"Erro ao acessar st.secrets ou env var para senha: {e}")
        db_password_for_ts = None # Garante que é None em caso de erro

    # Botão de atualização para o timestamp (desabilitado se senha não encontrada)
    if st.sidebar.button("Atualizar Referência DB", key="refresh_db_ts", disabled=(not db_password_for_ts)):
        if db_password_for_ts:
            st.session_state.latest_db_timestamp = fetch_latest_nfs_timestamp(
                db_path_for_ts, db_user_for_ts, db_password_for_ts, db_charset_for_ts
            )
            st.sidebar.success("Referência DB atualizada!", icon="✅")
            st.rerun()
        # else: O botão já estará desabilitado

    # Busca o timestamp inicial se necessário (desabilitado se senha não encontrada)
    if 'latest_db_timestamp' not in st.session_state and db_password_for_ts:
        logger.info("Buscando timestamp inicial do DB...")
        st.session_state.latest_db_timestamp = fetch_latest_nfs_timestamp(
            db_path_for_ts, db_user_for_ts, db_password_for_ts, db_charset_for_ts
        )
    elif 'latest_db_timestamp' not in st.session_state:
         st.session_state.latest_db_timestamp = "Senha não configurada"

    # Exibe o timestamp (ou erro)
    latest_ts_result = st.session_state.get('latest_db_timestamp')
    if isinstance(latest_ts_result, datetime.datetime):
        ts_display = latest_ts_result.strftime("%d/%m/%Y %H:%M:%S")
        st.sidebar.metric(label="Última NFS Emitida", value=ts_display)
    elif isinstance(latest_ts_result, datetime.date):
        ts_display = latest_ts_result.strftime("%d/%m/%Y")
        st.sidebar.metric(label="Última NFS (Data)", value=ts_display, help="Não foi possível obter a hora.")
    elif isinstance(latest_ts_result, str):
        st.sidebar.metric(label="Última NFS Emitida", value="-")
        st.sidebar.caption(f"Status: {latest_ts_result}")
        if "Erro DB" in latest_ts_result:
            st.sidebar.warning(f"Erro DB: {latest_ts_result}. Verifique configs/log.", icon="⚠️")
        elif "Senha não configurada" in latest_ts_result:
             st.sidebar.warning("Senha do DB não encontrada.", icon="🔒")
    else:
        st.sidebar.metric(label="Última NFS Emitida", value="-")
        st.sidebar.caption("Status: Desconhecido")

    st.sidebar.divider()

    # --- Toggle para Embeddings e IA --- #
    st.sidebar.subheader("Recursos Otimizados")
    embeddings_file_exists = os.path.exists(config.EMBEDDED_SCHEMA_FILE)
    if embeddings_file_exists:
        st.sidebar.toggle(
            "Usar Embeddings (Schema Otimizado)",
            key='use_embeddings',
            help=f"Carrega `{config.EMBEDDED_SCHEMA_FILE}`. Pode levar um momento.",
            on_change=handle_embedding_toggle # Chama a função importada
        )
    else:
        st.sidebar.toggle(
            "Usar Embeddings (Schema Otimizado)",
            key='use_embeddings',
            help=f"Arquivo `{config.EMBEDDED_SCHEMA_FILE}` não encontrado.",
            value=False,
            disabled=True
        )
        if st.session_state.get('use_embeddings'):
            st.session_state.use_embeddings = False

    # Toggle Ollama (Usa variável global passada como argumento)
    if OLLAMA_AVAILABLE:
        st.sidebar.toggle("Habilitar Sugestões IA (Ollama)", 
                          key='ollama_enabled', 
                          help="Desabilitar pode melhorar a performance.")
    else:
        st.sidebar.caption("Sugestões IA (Ollama) indisponíveis.")

    st.sidebar.divider()

    # --- Botões de Ação Global ---
    st.sidebar.subheader("Ações")

    # Botão Salvar
    save_button_disabled = not st.session_state.get('unsaved_changes', False)
    if st.sidebar.button("Salvar Alterações", key="save_button", type="primary", disabled=save_button_disabled):
        if save_metadata(st.session_state.metadata, config.METADATA_FILE):
            st.session_state.unsaved_changes = False
            st.session_state.initial_metadata = copy.deepcopy(st.session_state.metadata) # Atualiza o estado inicial
            st.toast("Metadados salvos com sucesso!", icon="✅")
            # Não precisa de rerun, a UI se atualiza com o estado
        else:
            st.sidebar.error("Falha ao salvar metadados.")

    # Comentado: Botão Descartar
    # if st.sidebar.button("Descartar Alterações", key="discard_button", disabled=not st.session_state.get('unsaved_changes', False)):
    #     st.session_state.metadata = copy.deepcopy(st.session_state.initial_metadata) # Restaura do estado inicial
    #     st.session_state.unsaved_changes = False
    #     st.toast("Alterações descartadas.", icon="🗑️")
    #     st.rerun() # Rerun para refletir a restauração

    # --- Processamento de Dados --- #
    st.sidebar.divider()
    st.sidebar.subheader("Processamento de Dados")

    # Botão Heurística Global
    if st.sidebar.button("Aplicar Heurística Globalmente", key="apply_heuristics_button", help="Tenta preencher descrições de colunas vazias."):
        with st.spinner("Aplicando heurística..."):
            # Passa technical_schema_data como argumento
            upd_desc, upd_notes = apply_heuristics_globally(st.session_state.metadata, technical_schema_data)
            st.sidebar.success(f"Heurística Concluída!", icon="✅")
            st.sidebar.info(f"- Descrições: {upd_desc}\n- Notas: {upd_notes}")
            st.sidebar.warning("As alterações estão em memória. Salve para persistir.")

    # Botão Preencher via Chaves
    if st.sidebar.button("Preencher Descrições (Chaves FK->PK)", key="populate_keys_button", help="Usa descrição da PK referenciada."):
        with st.spinner("Analisando chaves..."):
             # Passa technical_schema_data como argumento
            updated_key_count = populate_descriptions_from_keys(st.session_state.metadata, technical_schema_data)
            if updated_key_count > 0:
                st.sidebar.success(f"{updated_key_count} descrições preenchidas via chaves!", icon="🔑")
                st.sidebar.warning("As alterações estão em memória. Salve para persistir.")
            else:
                st.sidebar.info("Nenhuma descrição de FK vazia pôde ser preenchida.")

    # Botão Executar Merge
    if st.sidebar.button("Executar Merge de Dados", key="run_merge_script"):
        script_path = os.path.join("scripts", "merge_schema_data.py")
        if not os.path.exists(script_path):
            st.sidebar.error(f"Erro: Script de merge não encontrado em '{script_path}'")
        else:
            st.sidebar.info(f"Executando '{script_path}'...")
            try:
                python_executable = sys.executable
                process = subprocess.Popen(
                    [python_executable, script_path],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                    text=True, encoding='utf-8', errors='replace'
                )
                stdout, stderr = process.communicate()
                logger.info(f"Saída stdout do merge script:\n{stdout}")
                if stderr:
                    logger.error(f"Saída stderr do merge script:\n{stderr}")

                if process.returncode == 0:
                    st.sidebar.success(f"Merge concluído! '{config.OUTPUT_COMBINED_FILE}' atualizado.")
                    try:
                        load_technical_schema.clear()
                        logger.info("Cache do schema técnico limpo após merge.")
                        st.rerun()
                    except Exception as e:
                        logger.warning(f"Erro ao limpar cache/rerun: {e}")
                        st.sidebar.warning("Merge concluído, recarregue a página.")
                else:
                    st.sidebar.error(f"Erro no merge (Código: {process.returncode}). Verifique logs.")
                    if stderr:
                        st.sidebar.text_area("Erro Reportado:", stderr, height=100)
            except Exception as e:
                st.sidebar.error(f"Erro inesperado ao executar merge: {e}")
                logger.exception("Erro ao executar subprocesso de merge")

    # --- Configurações Extras --- #
    st.sidebar.divider()
    st.sidebar.subheader("Configurações Extras")
    st.sidebar.toggle(
        "Habilitar Auto-Save (Intervalo)",
        key='auto_save_enabled',
        help=f"Salva automaticamente a cada {config.AUTO_SAVE_INTERVAL_SECONDS // 60} minutos."
    )

    st.sidebar.info("Para executar: `streamlit run streamlit_app.py`")

    return app_mode # Retorna o modo selecionado 