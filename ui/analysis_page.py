# Código da interface para o modo 'Análise'

import streamlit as st
import pandas as pd
import logging

# Importações de módulos core
import src.core.config as config

logger = logging.getLogger(__name__)

def display_analysis_page(technical_schema_data):
    """Renderiza a página de Análise Estrutural."""

    st.header("🔎 Análise Estrutural e de Referências do Schema")
    loaded_schema_path = st.session_state.get('loaded_schema_file', "Não definido")
    st.caption(f"Analisando informações de: `{loaded_schema_path}`")
    st.divider()

    # Verifica se a análise de chaves está no estado da sessão
    if 'key_analysis' not in st.session_state or not st.session_state.key_analysis:
        st.error("Dados da análise de chaves não encontrados no estado da sessão. Verifique o carregamento inicial.")
        return
        
    # Recupera a análise estrutural do estado da sessão (agora é um dicionário)
    key_analysis_data = st.session_state.key_analysis
    composite_pk_tables = key_analysis_data.get('composite_pk_tables', []) # Usar .get com default
    junction_tables = key_analysis_data.get('junction_tables', [])
    composite_fk_details = key_analysis_data.get('composite_fk_details', {})
    column_roles = key_analysis_data.get('column_roles', {})
    # fk_reference_counts é pego mais abaixo
    # composite_pk_tables, junction_tables, composite_fk_details, column_roles = st.session_state.key_analysis # <-- Linha removida

    # --- Seção: Colunas Mais Referenciadas (com Importância) ---
    st.subheader("Colunas Mais Referenciadas por FKs (com Prioridade)")
    
    # Usa fk_reference_counts da análise de chaves no estado da sessão
    if 'key_analysis' in st.session_state and st.session_state.key_analysis:
        # key_analysis é uma tupla: (composite_pk_tables, junction_tables, composite_fk_details, column_roles, fk_reference_counts)
        # A ordem pode variar, então é melhor pegar do dicionário salvo em key_analysis_results.json
        # Assumindo que load_key_analysis_results carrega um dicionário:
        fk_counts = st.session_state.key_analysis.get('fk_reference_counts', {})
    else:
        fk_counts = {}

    if not fk_counts:
        st.info("Nenhuma contagem de referência de FK encontrada na análise de chaves.")
    else:
        fk_list = []
        processed_columns = set()
        # Processa colunas com contagem
        for key, count in fk_counts.items():
            try:
                table_name, column_name = key.split('.', 1)
                if not table_name or not column_name: continue

                # Usa os roles pré-calculados do estado da sessão
                role_info = column_roles.get((table_name, column_name), {'role': 'Normal', 'importance_level': 'Baixa'})
                
                # Pega descrição do metadado (business_description ou description técnica)
                # Idealmente, metadados deveriam ser passados, mas por simplicidade inicial,
                # vamos assumir que as descrições relevantes podem já estar no schema técnico
                # (gerado pelo merge_schema_data.py)
                metadata_info = technical_schema_data.get(table_name, {}).get('columns', [])
                col_data = next((col for col in metadata_info if col.get('name') == column_name), None)
                
                col_desc = col_data.get('business_description') if col_data else None
                if not col_desc and col_data:
                    col_desc = col_data.get('description') # Fallback para descrição técnica
                has_description = bool(col_desc.strip()) if col_desc else False
                
                col_notes = col_data.get('value_mapping_notes') if col_data else None # Verifica se tem notas
                has_notes = bool(col_notes.strip()) if col_notes else False

                fk_list.append({
                    "Importância": role_info.get('importance_level', 'Baixa'),
                    "Tabela": table_name,
                    "Coluna": column_name,
                    "Função Chave": role_info.get('role', 'Normal'),
                    "Nº Referências FK": count,
                    "Tem Descrição?": "✅" if has_description else "❌",
                    "Tem Notas?": "✅" if has_notes else "❌"
                })
                processed_columns.add((table_name, column_name))
            except ValueError:
                logger.warning(f"Formato inválido na chave fk_reference_counts: {key}")
            except Exception as e:
                logger.error(f"Erro processando contagem FK para {key}: {e}")

        # Adiciona outras colunas importantes (PK Comp, PK/FK) que não foram referenciadas
        # Chaves de column_roles são strings "tabela:coluna"
        for key_str, role_info in column_roles.items():
            try:
                table_name, column_name = key_str.split(':', 1)
            except ValueError:
                logger.warning(f"Chave de column_roles inválida encontrada: {key_str}")
                continue # Pula esta entrada

            if (table_name, column_name) not in processed_columns and role_info.get('importance_level') in ['Máxima', 'Alta']:
                try:
                    metadata_info = technical_schema_data.get(table_name, {}).get('columns', [])
                    col_data = next((col for col in metadata_info if col.get('name') == column_name), None)
                    
                    col_desc = col_data.get('business_description') if col_data else None
                    if not col_desc and col_data:
                        col_desc = col_data.get('description')
                    has_description = bool(col_desc.strip()) if col_desc else False
                    
                    col_notes = col_data.get('value_mapping_notes') if col_data else None
                    has_notes = bool(col_notes.strip()) if col_notes else False
                    
                    fk_list.append({
                        "Importância": role_info.get('importance_level', 'Baixa'),
                        "Tabela": table_name,
                        "Coluna": column_name,
                        "Função Chave": role_info.get('role', 'Normal'),
                        "Nº Referências FK": 0, # Não foi referenciada diretamente
                        "Tem Descrição?": "✅" if has_description else "❌",
                        "Tem Notas?": "✅" if has_notes else "❌"
                    })
                except Exception as e:
                    logger.error(f"Erro processando coluna importante não referenciada {(table_name, column_name)}: {e}")

        if not fk_list:
             st.warning("Não foi possível processar as colunas para análise.")
        else:
            # Ordenar
            importance_order = {'Máxima': 0, 'Alta': 1, 'Média': 2, 'Baixa': 3}
            fk_list_sorted = sorted(fk_list,
                                   # Ordena primeiro por Nº Ref FK (desc) e depois por Importância (asc no map, ie, desc na label)
                                   key=lambda x: (-x["Nº Referências FK"], importance_order.get(x["Importância"], 99)),
                                   reverse=False)

            df_fk_analysis = pd.DataFrame(fk_list_sorted)
            cols_ordered_analysis = ["Importância", "Tabela", "Coluna", "Função Chave", "Nº Referências FK", "Tem Descrição?", "Tem Notas?"]
            df_fk_analysis = df_fk_analysis[[col for col in cols_ordered_analysis if col in df_fk_analysis.columns]]

            # Slider e DataFrame
            num_to_show_analysis = st.slider(
                "Mostrar Top N colunas por importância/referência:",
                min_value=5,
                max_value=len(df_fk_analysis),
                value=min(30, len(df_fk_analysis)),
                step=5,
                key="slider_analysis_importance"
            )
            st.dataframe(df_fk_analysis.head(num_to_show_analysis), use_container_width=True)
            with st.expander("Mostrar todas as colunas analisadas"):
                 st.dataframe(df_fk_analysis, use_container_width=True)

    st.divider()

    # --- Seção: Tabelas com PK Composta ---
    st.subheader("Tabelas com Chave Primária Composta")
    if composite_pk_tables:
        pk_comp_list = []
        # Verifica se é uma lista (esperando lista de strings baseado nos logs)
        if isinstance(composite_pk_tables, list):
            for table_name in composite_pk_tables:
                if isinstance(table_name, str):
                     pk_comp_list.append({"Tabela": table_name}) # Só podemos listar a tabela
                else:
                    logger.warning(f"Item inesperado na lista composite_pk_tables (esperava string): {table_name}")
        elif isinstance(composite_pk_tables, dict):
             logger.warning("Formato composite_pk_tables é dict, mas esperava lista. Tentando adaptar...")
             for table, cols in composite_pk_tables.items(): # Fallback se for dict antigo
                 pk_comp_list.append({"Tabela": table, "Colunas PK (estrutura antiga?)": ", ".join(cols)})
        else:
            logger.error(f"Formato inesperado para composite_pk_tables: {type(composite_pk_tables)}")

        if pk_comp_list:
            df_pk_comp = pd.DataFrame(pk_comp_list).sort_values(by="Tabela")
            st.dataframe(df_pk_comp, use_container_width=True)
        else:
            st.warning("Não foi possível processar os dados de PK Composta no formato encontrado.")
    else:
        st.info("Nenhuma tabela com chave primária composta identificada.")

    st.divider()

    # --- Seção: Tabelas de Junção ---
    st.subheader("Tabelas de Ligação (Junção)")
    if junction_tables:
        junction_list = []
        # Verifica se é uma lista (esperando lista de strings baseado nos logs e no erro KeyError)
        if isinstance(junction_tables, list):
            for table_name in junction_tables:
                if isinstance(table_name, str):
                     junction_list.append({"Tabela": table_name}) # Só podemos listar a tabela
                else:
                    logger.warning(f"Item inesperado na lista junction_tables (esperava string): {table_name}")
        elif isinstance(junction_tables, dict):
             logger.warning("Formato junction_tables é dict, mas esperava lista. Tentando adaptar...")
             for table, details in junction_tables.items(): # Fallback se for dict antigo
                 junction_list.append({"Tabela": table, "Detalhes (estrutura antiga?)": "; ".join(details)})
        else:
            logger.error(f"Formato inesperado para junction_tables: {type(junction_tables)}")

        if junction_list:
            df_junction = pd.DataFrame(junction_list).sort_values(by="Tabela")
            st.dataframe(df_junction, use_container_width=True)
        else:
             st.warning("Não foi possível processar os dados de Tabelas de Junção no formato encontrado.")
    else:
        st.info("Nenhuma tabela de junção identificada (PK composta totalmente por FKs).")

    st.divider()

    # --- Seção: Colunas em FK Composta ---
    st.subheader("Referências de Chave Estrangeira Composta")
    if composite_fk_details:
        composite_fk_list = []
        # Verifica se é um dicionário
        if isinstance(composite_fk_details, dict):
            for key_str, detail in composite_fk_details.items():
                try:
                    table, column = key_str.split(':', 1)
                except ValueError:
                    logger.warning(f"Chave de FK composta inválida encontrada: {key_str}")
                    continue # Pula esta entrada se a chave estiver mal formada

                fk_name = detail.get('fk_name', 'N/A')
                referenced_table = detail.get('referenced_table', 'N/A')
                referenced_column = detail.get('referenced_column', 'N/A')
                composite_fk_list.append({
                    "Tabela": table,
                    "Coluna": column,
                    "Nome FK": fk_name,
                    "Tabela Ref.": referenced_table,
                    "Coluna Ref.": referenced_column
                })

            if composite_fk_list:
                df_composite_fk = pd.DataFrame(composite_fk_list).sort_values(by=["Tabela", "Nome FK", "Coluna"])
                st.dataframe(df_composite_fk, use_container_width=True)
            else:
                st.warning("Não foi possível processar os dados de Chaves Estrangeiras Compostas a partir do dicionário fornecido.")
        else:
            logger.error(f"Formato inesperado para composite_fk_details: {type(composite_fk_details)}. Esperava um dicionário.")
            st.error("Formato de dados inválido para Chaves Estrangeiras Compostas. Verifique os logs.")
    else:
        st.info("Nenhuma chave estrangeira composta identificada.") 