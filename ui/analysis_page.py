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
    st.caption(f"Analisando informações de: `{config.TECHNICAL_SCHEMA_FILE}`")
    st.divider()

    # Verifica se a análise de chaves está no estado da sessão
    if 'key_analysis' not in st.session_state or not st.session_state.key_analysis:
        st.error("Dados da análise de chaves não encontrados no estado da sessão. Verifique o carregamento inicial.")
        return
        
    # Recupera a análise estrutural do estado da sessão
    composite_pk_tables, junction_tables, composite_fk_details, column_roles = st.session_state.key_analysis

    # --- Seção: Colunas Mais Referenciadas (com Importância) ---
    st.subheader("Colunas Mais Referenciadas por FKs (com Prioridade)")
    
    # Usa fk_reference_counts que deve estar dentro de technical_schema_data
    fk_counts = technical_schema_data.get('fk_reference_counts', {})
    if not fk_counts:
        st.info("Nenhuma contagem de referência de FK encontrada no schema técnico.")
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
        for (table_name, column_name), role_info in column_roles.items():
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
                                    key=lambda x: (importance_order.get(x["Importância"], 99), -x["Nº Referências FK"]),
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
        for table, cols in composite_pk_tables.items():
            pk_comp_list.append({"Tabela": table, "Colunas PK": ", ".join(cols)})
        df_pk_comp = pd.DataFrame(pk_comp_list).sort_values(by="Tabela")
        st.dataframe(df_pk_comp, use_container_width=True)
    else:
        st.info("Nenhuma tabela com chave primária composta identificada.")

    st.divider()

    # --- Seção: Tabelas de Junção ---
    st.subheader("Tabelas de Ligação (Junção)")
    if junction_tables:
        junction_list = []
        for table, details in junction_tables.items():
             junction_list.append({"Tabela": table, "Detalhes FKs na PK": "; ".join(details)})
        df_junction = pd.DataFrame(junction_list).sort_values(by="Tabela")
        st.dataframe(df_junction, use_container_width=True)
    else:
        st.info("Nenhuma tabela de junção identificada (PK composta totalmente por FKs).")

    st.divider()

    # --- Seção: Colunas em FK Composta ---
    st.subheader("Colunas em Chaves Estrangeiras Compostas")
    if composite_fk_details:
        fk_comp_list = []
        for (table, column), detail in composite_fk_details.items():
             fk_comp_list.append({"Tabela": table, "Coluna": column, "Referência (parte de FK Comp.)": detail})
        df_fk_comp = pd.DataFrame(fk_comp_list).sort_values(by=["Tabela", "Coluna"])
        st.dataframe(df_fk_comp, use_container_width=True)
    else:
        st.info("Nenhuma coluna identificada como parte de chave estrangeira composta.") 