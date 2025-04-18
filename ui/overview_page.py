# Código da interface para o modo 'Visão Geral'

import streamlit as st
import os
import sys
import subprocess
import logging
import datetime # NOVO: Para formatar timestamp
import pandas as pd

# Importações de módulos core e utils
import src.core.config as config
from src.core.analysis import generate_documentation_overview
from src.core.data_loader import load_overview_counts

logger = logging.getLogger(__name__)

def display_overview_page(technical_schema_data, metadata_dict, db_path, db_user, db_password, db_charset):
    """Renderiza a página de Visão Geral."""

    st.header("Visão Geral da Documentação e Contagens (Cache)")
    st.caption(f"Metadados de: `{config.METADATA_FILE}` | Schema de: `{config.TECHNICAL_SCHEMA_FILE}` | Contagens de: `{config.OVERVIEW_COUNTS_FILE}`")
    st.divider()
    
    # --- NOVO: Exibir Informações de Validação e Contagem --- #
    metadata_info = technical_schema_data.get('_metadata_info')
    if metadata_info:
        total_cols = metadata_info.get('total_column_count', 'N/A')
        manual_cols = metadata_info.get('manual_metadata_column_count', 'N/A')
        missing_manual_cols = metadata_info.get('missing_manual_metadata_column_count', 'N/A')
        status = metadata_info.get('validation_status', 'Desconhecido')
        timestamp_str = metadata_info.get('validation_timestamp')
        missing_objs = metadata_info.get('missing_objects', [])
        missing_cols = metadata_info.get('missing_columns', {})
        
        try:
            if timestamp_str:
                ts = datetime.datetime.fromisoformat(timestamp_str).strftime('%d/%m/%Y %H:%M:%S')
            else:
                ts = "N/A"
        except ValueError:
            ts = "Data inválida"
            
        st.subheader("Status do Schema Combinado")
        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("Total Colunas", str(total_cols))
        col2.metric("Com Metadados Manuais", str(manual_cols))
        col3.metric("Sem Metadados Manuais", str(missing_manual_cols))
        col4.metric("Status Validação", status, delta_color="inverse" if status != 'OK' else "normal")
        col5.metric("Última Validação", ts)
        
        if status != 'OK':
            st.warning(f"Atenção: A validação do schema combinado (executada em {ts}) falhou. O schema pode estar incompleto.", icon="⚠️")
            with st.expander("Detalhes dos Itens Faltando"):
                if missing_objs:
                    st.markdown("**Objetos (Tabelas/Views) faltando:**")
                    st.json(missing_objs)
                if missing_cols:
                    st.markdown("**Colunas faltando (por Tabela/View):**")
                    st.json(missing_cols)
        else:
            st.success(f"O schema combinado foi validado com sucesso em {ts}.", icon="✅")
        st.caption("A validação compara o schema combinado atual com o último schema técnico extraído no momento da execução do script `scripts/merge_schema_data.py`.")
    else:
        st.warning("Informações de validação e contagem não encontradas no schema. Execute o script `scripts/merge_schema_data.py` para gerá-las.", icon="❓")
    # --- FIM: Informações de Validação --- #
    
    # --- Botão para Executar Contagem --- #
    st.divider()
    st.subheader("Atualizar Contagem de Linhas")
    st.warning("Executar a contagem pode levar vários minutos dependendo do tamanho do banco.", icon="⏱️")

    # Desabilitar botão se a senha não foi fornecida
    run_count_disabled = not db_password
    button_help = "Senha do banco não configurada." if run_count_disabled else None

    if st.button("Executar Cálculo de Contagem Agora", key="run_count_script", disabled=run_count_disabled, help=button_help):
        script_path = os.path.join("scripts", "calculate_row_counts.py")
        if not os.path.exists(script_path):
            st.error(f"Erro: Script de contagem não encontrado em '{script_path}'")
        else:
            st.info(f"Executando '{script_path}'... Acompanhe o progresso abaixo.")
            progress_bar = st.progress(0.0, text="Iniciando...")
            status_text = st.empty()
            error_messages = []
            final_stdout = ""

            try:
                python_executable = sys.executable
                cmd_list = [
                    python_executable,
                    script_path,
                    "--db-path", db_path,
                    "--db-user", db_user,
                    "--db-password", db_password,
                    "--db-charset", db_charset
                ]
                logger.info(f"Executando comando: {' '.join(cmd_list[:5])} --db-password **** ...")

                process = subprocess.Popen(
                    cmd_list,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    bufsize=1
                )

                for line in process.stdout:
                    line = line.strip()
                    final_stdout += line + "\n"
                    logger.debug(f"Linha lida do script: {line}")
                    if line.startswith("PROGRESS:"):
                        try:
                            parts = line.split(':')
                            progress_part = parts[1].split('/')
                            current = int(progress_part[0])
                            total = int(progress_part[1])
                            current_table = parts[2]
                            progress_value = float(current) / float(total) if total > 0 else 0.0
                            progress_text = f"Contando: {current_table} ({current}/{total})"
                            progress_bar.progress(progress_value, text=progress_text)
                            status_text.text(progress_text)
                        except (IndexError, ValueError) as e:
                            logger.warning(f"Não foi possível parsear linha de progresso '{line}': {e}")
                    elif line.startswith("DONE:"):
                         logger.info(f"Script reportou conclusão: {line}")
                         break
                    else:
                         logger.debug(f"Output não reconhecido do script: {line}")
                
                stderr = process.stderr.read()
                if stderr:
                    error_messages.append(stderr)
                    logger.error(f"Erro stderr do script de contagem:\n{stderr}")

                process.wait()
                status_text.empty()

                if process.returncode == 0:
                    progress_bar.progress(1.0, text="Contagem Concluída!")
                    st.success(f"Script '{script_path}' executado com sucesso!")
                    logger.info(f"Saída final stdout do script:\n{final_stdout}")
                    load_overview_counts.clear()
                    st.session_state.overview_counts = load_overview_counts(config.OVERVIEW_COUNTS_FILE)
                    st.rerun()
                else:
                    progress_bar.progress(1.0, text="Erro na Contagem!")
                    st.error(f"Erro ao executar '{script_path}' (Código de saída: {process.returncode}).")
                    if error_messages:
                        st.text_area("Erro(s) Reportado(s) pelo Script:", "\n".join(error_messages), height=150)
                    load_overview_counts.clear()
                    st.session_state.overview_counts = load_overview_counts(config.OVERVIEW_COUNTS_FILE)
                    st.rerun()
            except Exception as e:
                st.error(f"Erro inesperado ao tentar executar/ler o script: {e}")
                logger.exception("Erro ao executar subprocesso de contagem")
                progress_bar.progress(1.0, text="Erro Inesperado!")

    st.caption("Este botão executa um script externo para recalcular as contagens e salvar no cache.")
    st.divider()
    # --- FIM: Botão para Executar Contagem --- #

    st.info("A coluna 'Linhas (Cache)' mostra a última contagem salva. Para atualizar, use o botão acima.")

    # Gera e exibe o DataFrame
    df_overview = generate_documentation_overview(
        technical_schema_data,
        metadata_dict,
        st.session_state.overview_counts # Pega contagens do estado da sessão
    )
    st.dataframe(df_overview, use_container_width=True)

    # Botão para recarregar apenas as contagens do arquivo
    if st.button("Recarregar Contagens do Arquivo", key="refresh_counts_overview"):
        load_overview_counts.clear()
        st.session_state.overview_counts = load_overview_counts(config.OVERVIEW_COUNTS_FILE)
        st.success("Contagens recarregadas.")
        st.rerun()
    st.caption("Este botão apenas recarrega os dados do último cálculo salvo no arquivo, sem conectar ao banco.") 