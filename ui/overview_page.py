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
from src.analysis.analysis import generate_documentation_overview
from src.core.data_loader import load_overview_counts

logger = logging.getLogger(__name__)

def display_overview_page(technical_schema_data, metadata_dict, db_path, db_user, db_password, db_charset):
    """Renderiza a página de Visão Geral."""

    # --- Cabeçalho e Informações do Schema Carregado --- #
    st.header("Visão Geral do Schema Ativo")
    
    # Pega o path do arquivo carregado do session_state
    loaded_schema_path = st.session_state.get('loaded_schema_file', "Não definido")
    st.caption(f"**Schema Ativo:** `{loaded_schema_path}`")
    st.caption(f"**Metadados Manuais:** `{config.METADATA_FILE}`") # Mantém info dos metadados manuais
    
    st.divider()

    # --- Calcular Contagens Dinamicamente --- #
    total_objects = 0
    total_columns = 0
    columns_with_business_desc = 0

    if technical_schema_data and isinstance(technical_schema_data, dict):
        total_objects = len(technical_schema_data)
        for obj_name, obj_data in technical_schema_data.items():
            if isinstance(obj_data, dict) and 'columns' in obj_data and isinstance(obj_data['columns'], list):
                obj_columns = len(obj_data['columns'])
                total_columns += obj_columns
                for col_data in obj_data['columns']:
                    if isinstance(col_data, dict) and col_data.get('business_description'):
                        columns_with_business_desc += 1
            # Adicionar um else para logar/avisar sobre objetos malformados se necessário
            # else:
            #     logger.warning(f"Objeto '{obj_name}' no schema não tem formato esperado (dict com lista 'columns').")
    else:
        st.warning("Schema técnico não carregado ou em formato inválido.")

    # --- Exibir Novas Contagens --- #
    st.subheader("Estatísticas do Schema Ativo")
    col1, col2, col3 = st.columns(3)
    col1.metric("Objetos (Tabelas/Views)", str(total_objects))
    col2.metric("Total de Colunas", str(total_columns))
    col3.metric("Colunas com Desc. Negócio", str(columns_with_business_desc))
    st.caption("Contagens calculadas diretamente do schema carregado em memória.")

    # --- Bloco de Contagem de Linhas (Existente) --- #
    # O código para o botão "Executar Cálculo de Contagem Agora" permanece aqui
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
                # Tentativa 3: Chamar APENAS com --help para testar argparse
                module_path = "scripts.calculate_row_counts"
                cmd_list = [
                    python_executable,
                    "-m",
                    module_path,
                    "--help" # Apenas pedir ajuda
                ]
                logger.info(f"Executando comando (--help test): {' '.join(cmd_list)}")

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