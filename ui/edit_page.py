# Código da interface para o modo 'Editar Metadados'

import streamlit as st
import os
import re
from collections import OrderedDict
import logging
import numpy as np
import io
import pandas as pd
import copy
import time
import textwrap # Para formatar prompts
import json
import fdb

# Importações de módulos core e utils
import src.core.config as config
from src.core.metadata_logic import (
    get_type_explanation,
    find_existing_info,
    get_column_concept,
    save_metadata,
    compare_metadata_changes
)
from src.core.data_loader import load_metadata
from src.core.ai_integration import (
    generate_ai_description,
    find_similar_columns,
    get_query_embedding,
    # OLLAMA_AVAILABLE e chat_completion são passados como argumentos
)
from src.core.db_utils import fetch_sample_data
# Funções como populate_descriptions_from_keys, apply_heuristics_globally são chamadas pela sidebar

logger = logging.getLogger(__name__)

# --- NOVA FUNÇÃO AUXILIAR ---
def formatar_amostra_para_prompt(sample_data, technical_columns, tech_constraints, target_column=None, max_rows=3, max_cols=7, max_chars_per_cell=50):
    """
    Formata os dados de exemplo para inclusão em prompts LLM, tentando ser conciso e informativo.

    Args:
        sample_data: Lista de dicionários ou DataFrame Pandas com os dados de exemplo.
        technical_columns: Lista de dicionários com metadados das colunas técnicas.
        tech_constraints: Dicionário com constraints (primary_key, foreign_keys).
        target_column (str, optional): A coluna específica foco do prompt. Defaults to None.
        max_rows (int): Máximo de linhas de exemplo a incluir.
        max_cols (int): Máximo de colunas a incluir.
        max_chars_per_cell (int): Máximo de caracteres por célula antes de truncar.

    Returns:
        str: String formatada da amostra ou None se não for possível formatar.
    """
    if not sample_data:
        return None

    try:
        if isinstance(sample_data, list) and all(isinstance(row, dict) for row in sample_data):
            df = pd.DataFrame(sample_data)
        elif isinstance(sample_data, pd.DataFrame):
            df = sample_data.copy()
        else:
            # Tenta converter outros tipos para string, mas limita o tamanho
            return f"Dados de exemplo (formato não tabular):\\n{textwrap.shorten(str(sample_data), width=200)}"

        if df.empty:
            return None

        # Limita linhas
        df = df.head(max_rows)

        # Identifica colunas chave
        pk_cols = set()
        fk_cols = set()
        if tech_constraints:
            for pk in tech_constraints.get('primary_key', []):
                pk_cols.update(pk.get('columns', []))
            for fk in tech_constraints.get('foreign_keys', []):
                fk_cols.update(fk.get('columns', []))

        # Seleciona colunas a exibir
        available_cols = df.columns.tolist()
        cols_to_display = []
        if target_column and target_column in available_cols:
            cols_to_display.append(target_column)
            # Tenta adicionar colunas adjacentes, evitando duplicatas
            try:
                target_idx = available_cols.index(target_column)
                if target_idx > 0 and available_cols[target_idx - 1] not in cols_to_display:
                    cols_to_display.insert(0, available_cols[target_idx - 1]) # Adiciona antes
                if target_idx < len(available_cols) - 1 and available_cols[target_idx + 1] not in cols_to_display:
                    cols_to_display.append(available_cols[target_idx + 1]) # Adiciona depois
            except ValueError:
                pass # target_column não encontrado, segue com ele sozinho se adicionado

        # Preenche com outras colunas até o limite, priorizando não-chaves se possível
        non_key_cols = [c for c in available_cols if c not in pk_cols and c not in fk_cols and c not in cols_to_display]
        key_cols = [c for c in available_cols if (c in pk_cols or c in fk_cols) and c not in cols_to_display]

        remaining_slots = max_cols - len(cols_to_display)
        cols_to_display.extend(non_key_cols[:remaining_slots])

        remaining_slots = max_cols - len(cols_to_display)
        if remaining_slots > 0:
            cols_to_display.extend(key_cols[:remaining_slots])

        # Garante que target_column está presente se ele existe nos dados
        if target_column and target_column in available_cols and target_column not in cols_to_display and len(cols_to_display) < max_cols:
             cols_to_display.append(target_column)

        # Garante ordem e limita ao máximo
        final_cols = []
        for col in available_cols: # Mantém ordem original o máximo possível
            if col in cols_to_display and col not in final_cols:
                final_cols.append(col)
        df_display = df[final_cols[:max_cols]].copy() # Aplica limite final

        # Formata para Markdown, truncando células longas e adicionando info de tipo/chave
        header = []
        col_details_map = {c['name']: c for c in technical_columns if 'name' in c}

        for col_name in df_display.columns:
            col_info = col_details_map.get(col_name, {})
            col_type = col_info.get('type', 'Desconhecido')
            tags = []
            if col_name in pk_cols: tags.append("PK")
            if col_name in fk_cols: tags.append("FK")
            # Verifica baixa variabilidade (excluindo nulos)
            unique_vals = df_display[col_name].dropna().unique()
            if len(unique_vals) <= 1: tags.append("Valor Fixo?") # Indica potencial valor constante na amostra

            tag_str = f" ({', '.join(tags)})" if tags else ""
            header.append(f"{col_name} ({col_type}){tag_str}")

        # Prepara linhas, tratando BLOBs e truncando
        rows = [header, ["---"] * len(header)]
        for _, row_data in df_display.iterrows():
            formatted_row = []
            for col_name in df_display.columns:
                val = row_data[col_name]
                if isinstance(val, bytes):
                    cell_content = "[BLOB]"
                else:
                    cell_content = str(val)
                # Trunca célula
                formatted_row.append(textwrap.shorten(cell_content, width=max_chars_per_cell, placeholder="..."))
            rows.append(formatted_row)

        # Constrói a tabela markdown
        markdown_table = ""
        # Calcula largura das colunas para alinhar (opcional, mas melhora leitura)
        col_widths = [max(len(str(rows[r][c])) for r in range(len(rows))) for c in range(len(df_display.columns))]
        for row in rows:
            markdown_table += "| " + " | ".join(f"{str(row[c]).ljust(col_widths[c])}" for c in range(len(row))) + " |\n"

        return f"Amostra de Dados ({min(max_rows, len(df))} linhas):\n```markdown\n{markdown_table}\n```"

    except Exception as e:
        logger.error(f"Erro ao formatar dados de exemplo: {e}", exc_info=True)
        return None # Retorna None em caso de erro

# --- FIM DA FUNÇÃO AUXILIAR ---

def display_edit_page(technical_schema_data, metadata_dict, OLLAMA_AVAILABLE, chat_completion, db_path, db_user, db_password, db_charset):
    """Renderiza a página de Edição de Metadados."""

    st.header("Editor de Metadados")
    st.caption(f"Editando o arquivo: `{config.METADATA_FILE}` | Contexto técnico de: `{config.TECHNICAL_SCHEMA_FILE}`")

    # --- Seleção do Objeto --- #
    all_technical_objects = {}
    for name, data in technical_schema_data.items():
        obj_type = data.get('object_type')
        if obj_type in ["TABLE", "VIEW"]: all_technical_objects[name] = obj_type

    if not all_technical_objects: 
        st.error("Nenhuma tabela/view no schema técnico.")
        return # Retorna se não há objetos

    object_types_available = sorted(list(set(all_technical_objects.values())))
    selected_type_display = st.radio("Filtrar por Tipo:", ["Todos"] + object_types_available, horizontal=True, index=0)

    # Filtra por tipo primeiro
    if selected_type_display == "Todos":
        base_object_names = sorted(list(all_technical_objects.keys()))
    elif selected_type_display in object_types_available:
        base_object_names = sorted([name for name, type in all_technical_objects.items() if type == selected_type_display])
    else:
        base_object_names = []

    # --- NOVO: Campo de Busca ---
    search_term = st.text_input("Buscar Objeto por Nome:", key="search_object_input", placeholder="Digite parte do nome...")

    # Filtra pelo termo de busca
    if search_term:
        search_term_lower = search_term.lower()
        filtered_object_names = [name for name in base_object_names if search_term_lower in name.lower()]
    else:
        filtered_object_names = base_object_names # Usa a lista filtrada por tipo se a busca estiver vazia

    # --- Lógica Atualizada do Selectbox ---
    if not filtered_object_names:
        if search_term:
             st.warning(f"Nenhum objeto encontrado para a busca '{search_term}' (Tipo: {selected_type_display}).")
        elif selected_type_display != "Todos":
             st.warning(f"Nenhum objeto do tipo '{selected_type_display}'.")
        # Não mostra aviso se for "Todos" e busca vazia mas não houver objetos (erro já mostrado acima)
        selected_object = None
    else:
        # Usar st.session_state para persistir a seleção entre reruns
        if 'selected_object' not in st.session_state or st.session_state.selected_object not in filtered_object_names:
             # Se o objeto selecionado não está mais na lista (devido a filtro ou busca),
             # ou se é a primeira vez, seleciona o primeiro da lista filtrada.
             st.session_state.selected_object = filtered_object_names[0]

        # Encontra o índice atual da seleção para o selectbox
        try:
            # Certifica que o objeto selecionado ainda existe na lista filtrada
            if st.session_state.selected_object in filtered_object_names:
                current_index = filtered_object_names.index(st.session_state.selected_object)
            else:
                # Se não existe mais, reseta para o primeiro item da lista filtrada
                current_index = 0
                st.session_state.selected_object = filtered_object_names[0]
        except (ValueError, IndexError):
             # Fallback seguro caso algo dê errado
             current_index = 0
             st.session_state.selected_object = filtered_object_names[0] if filtered_object_names else None

        selected_object = st.selectbox(
            "Selecione o Objeto para Editar",
            filtered_object_names, # Usa a lista filtrada
            index=current_index,
            key="selectbox_edit_object" # Mantém a chave única
        )
        # Atualiza o estado da sessão se a seleção mudar no selectbox
        if selected_object is not None and selected_object != st.session_state.selected_object:
             st.session_state.selected_object = selected_object
             # Não precisa de rerun aqui, pois o Streamlit atualiza quando o selectbox muda.
             # st.rerun() # Evitar rerun desnecessário aqui

    st.divider()

    # --- Edição dos Metadados --- #
    if selected_object:
        selected_object_technical_type = all_technical_objects.get(selected_object)
        metadata_key_type = selected_object_technical_type + "S" if selected_object_technical_type else None
        tech_obj_data = technical_schema_data.get(selected_object)

        # Garante estrutura no metadata_dict (usa st.session_state.metadata diretamente)
        if metadata_key_type and metadata_key_type not in st.session_state.metadata: 
            st.session_state.metadata[metadata_key_type] = OrderedDict()
        if metadata_key_type and selected_object not in st.session_state.metadata[metadata_key_type]:
             st.session_state.metadata[metadata_key_type][selected_object] = OrderedDict({'description': '', 'COLUMNS': OrderedDict()})

        # Acessa os dados do objeto diretamente do estado da sessão
        obj_data = st.session_state.metadata.get(metadata_key_type, {}).get(selected_object, {})
        tech_constraints = tech_obj_data.get('constraints', {}) # Pega as constraints

        if not tech_obj_data:
            st.error(f"Dados técnicos não encontrados para '{selected_object}'")
        else:
            st.subheader(f"Editando: `{selected_object}` ({tech_obj_data.get('object_type', 'Desconhecido')})", divider='rainbow')
            
            # --- NOVA SEÇÃO: Descrição do Objeto (Horizontal) ---
            with st.container(border=True): # Usando container para agrupar
                st.markdown("**Descrição do Objeto**")
                obj_desc_key = f"desc_{selected_object_technical_type}_{selected_object}"
                if "description" not in obj_data: obj_data["description"] = "" # Garante que a chave existe

                desc_obj_area, btn_ai_obj_area = st.columns([4, 1]) # Mantém colunas para descrição e botão IA

                with desc_obj_area:
                    new_obj_desc = st.text_area(
                        "Descrição Geral",
                        value=obj_data.get("description", ""),
                        key=obj_desc_key,
                        height=100,
                        label_visibility="collapsed",
                        on_change=lambda: st.session_state.update({'unsaved_changes': True})
                    )
                    if new_obj_desc != obj_data.get("description", ""):
                        st.session_state.metadata[metadata_key_type][selected_object]['description'] = new_obj_desc
                        st.session_state.unsaved_changes = True

                with btn_ai_obj_area:
                    if st.button("Sugerir IA", key=f"btn_ai_obj_{selected_object}", use_container_width=True, disabled=not OLLAMA_AVAILABLE or not st.session_state.get('ollama_enabled', True)):
                        # --- PREPARA CONTEXTO PARA IA (OBJETO) ---
                        sample_data = tech_obj_data.get("SAMPLE_DATA")
                        technical_columns_list = tech_obj_data.get("columns", [])
                        formatted_sample_str = formatar_amostra_para_prompt(sample_data, technical_columns_list, tech_constraints)
                        contexto_amostra = f"\\n\\nConsidere esta amostra de dados da tabela (pode conter valores fixos ou chaves):\\n{formatted_sample_str}" if formatted_sample_str else ""
                        # --- FIM PREPARA CONTEXTO ---
                        prompt_object = textwrap.dedent(f"""
                            Sugira uma descrição concisa e informativa em português brasileiro para o objeto de banco de dados '{selected_object}' (tipo: {selected_object_technical_type}).
                            Qual o propósito principal deste objeto com base em seu nome e, se disponíveis, nos exemplos de dados abaixo?
                            Seja direto e evite frases como "Esta tabela armazena...". Foque no significado do negócio.{contexto_amostra}

                            Responda apenas a descrição.
                        """)
                        # Limpa prompt para evitar excesso de espaços
                        prompt_object = "\\n".join([line.strip() for line in prompt_object.strip().splitlines()])

                        suggestion = generate_ai_description(prompt_object, OLLAMA_AVAILABLE, chat_completion)
                        if suggestion:
                             st.session_state.metadata[metadata_key_type][selected_object]['description'] = suggestion
                             st.session_state.unsaved_changes = True
                             st.rerun()

                # --- NOVO: Exibir Dados de Exemplo ---
                sample_data = tech_obj_data.get("SAMPLE_DATA")
                if sample_data:
                    with st.expander("Exemplos de Dados", expanded=False):
                        # Tenta detectar se é um DataFrame pandas ou apenas texto/lista
                        if isinstance(sample_data, list) and all(isinstance(row, dict) for row in sample_data):
                            try:
                                df_sample = pd.DataFrame(sample_data)
                                st.dataframe(df_sample, hide_index=True)
                            except Exception as e:
                                st.warning(f"Não foi possível exibir os dados de exemplo como tabela: {e}")
                                st.text(str(sample_data)) # Fallback para texto
                        elif isinstance(sample_data, (str, list)):
                             st.text(str(sample_data)) # Exibe como texto se for string ou lista simples
                        else:
                             st.text(str(sample_data)) # Fallback genérico

            st.markdown("---") # Separador

            # --- Bloco de Edição Colunas --- 
            st.markdown("**Descrição das Colunas**")
            obj_data.setdefault('COLUMNS', OrderedDict())
            columns_dict_meta = obj_data["COLUMNS"]
            technical_columns = tech_obj_data.get("columns", [])
            if not technical_columns: 
                st.write("*Nenhuma coluna no schema técnico.*")
            else:
                technical_column_names = [c['name'] for c in technical_columns if 'name' in c]
                column_tabs = st.tabs(technical_column_names)
                
                for i, col_name in enumerate(technical_column_names):
                    with column_tabs[i]:
                        if col_name not in columns_dict_meta: columns_dict_meta[col_name] = OrderedDict()
                        col_meta_data = columns_dict_meta[col_name]
                        if "description" not in col_meta_data: col_meta_data["description"] = ""
                        if "value_mapping_notes" not in col_meta_data: col_meta_data["value_mapping_notes"] = ""

                        tech_col_data = next((c for c in technical_columns if c['name'] == col_name), None)
                        if not tech_col_data: 
                            st.warning(f"Dados técnicos não encontrados para coluna '{col_name}'.")
                            continue 

                        col_type = tech_col_data.get('type', 'N/A')
                        col_nullable = tech_col_data.get('nullable', True)
                        type_explanation = get_type_explanation(col_type)

                        # --- Info PK/FK --- #
                        constraints = tech_obj_data.get('constraints', {})
                        key_info = []
                        for pk in constraints.get('primary_key', []):
                            if col_name in pk.get('columns', []): key_info.append("🔑 PK"); break
                        if not key_info: 
                            for fk in constraints.get('foreign_keys', []):
                                if col_name in fk.get('columns', []):
                                    try:
                                        idx = fk['columns'].index(col_name)
                                        ref_table = fk.get('references_table', '?')
                                        ref_cols = fk.get('references_columns', [])
                                        ref_col = ref_cols[idx] if idx < len(ref_cols) else '?'
                                        key_info.append(f"🔗 FK -> {ref_table}.{ref_col}")
                                    except (IndexError, ValueError, KeyError): key_info.append("🔗 FK (Erro)")
                                    break 
                        key_info_str = f" | {' | '.join(key_info)}" if key_info else ""
                        st.markdown(f"**Tipo:** `{col_type}` {type_explanation} | **Anulável:** {'Sim' if col_nullable else 'Não'}{key_info_str}")
                        st.markdown("--- Descrição --- ")

                        # --- Heurística --- #
                        current_col_desc_saved = col_meta_data.get('description', '').strip()
                        description_value_to_display = current_col_desc_saved
                        heuristic_desc_source = None
                        if not current_col_desc_saved: 
                            suggested_desc, desc_source_from_func, _, _ = find_existing_info(
                                st.session_state.metadata, technical_schema_data, selected_object, col_name
                            )
                            if suggested_desc:
                                description_value_to_display = suggested_desc
                                heuristic_desc_source = desc_source_from_func
                        if heuristic_desc_source: st.caption(f"ℹ️ Sugestão ({heuristic_desc_source}). Edite abaixo.")

                        # --- Busca FAISS --- #
                        col_embedding_data = tech_col_data.get('embedding')
                        if st.session_state.get('faiss_index') and col_embedding_data and st.session_state.get('use_embeddings'):
                            if st.button("🔍 Buscar Similares (FAISS)", key=f"faiss_search_{selected_object}_{col_name}"):
                                try:
                                    target_embedding = np.array(col_embedding_data).astype('float32')
                                    if target_embedding.shape[0] != config.EMBEDDING_DIMENSION:
                                        st.error(f"Dimensão do embedding ({target_embedding.shape[0]}) != {config.EMBEDDING_DIMENSION}.")
                                        target_embedding = None
                                except Exception as e:
                                    st.error(f"Erro ao converter embedding: {e}")
                                    target_embedding = None
                                if target_embedding is not None:
                                    with st.spinner("Buscando..."):
                                        similar_cols = find_similar_columns(
                                            st.session_state.faiss_index,
                                            st.session_state.technical_schema,
                                            st.session_state.index_to_key_map,
                                            target_embedding,
                                            k=5
                                        )
                                    if similar_cols:
                                        with st.expander("💡 Colunas Similares", expanded=True):
                                            for sim_col in similar_cols:
                                                st.markdown(f"**`{sim_col['table']}.{sim_col['column']}`** (Dist: {sim_col['distance']:.4f})")
                                                st.markdown(f"> _{sim_col['description']}_")
                                                st.markdown("---")
                                    else:
                                        st.info("Nenhuma coluna similar com descrição encontrada.")
                        elif not st.session_state.get('use_embeddings'):
                             st.caption("_(Embeddings desabilitados para busca similaridade)_")

                        # --- Área de Texto Descrição + Botões --- #
                        desc_col_area, btns_col_area = st.columns([4, 1])
                        col_desc_key = f"desc_{selected_object}_{col_name}"
                        with desc_col_area:
                            new_col_desc = st.text_area(
                                f"Descrição Coluna `{col_name}`",
                                value=description_value_to_display,
                                key=col_desc_key,
                                height=75,
                                label_visibility="collapsed",
                                on_change=lambda: st.session_state.update({'unsaved_changes': True})
                            )
                            # Atualiza estado se mudou
                            if new_col_desc != col_meta_data.get('description', ''):
                                st.session_state.metadata[metadata_key_type][selected_object]['COLUMNS'][col_name]['description'] = new_col_desc
                                st.session_state.unsaved_changes = True
                        
                        with btns_col_area:
                            if st.button("IA", key=f"btn_ai_col_{col_name}", help="Sugerir descrição com IA", use_container_width=True, disabled=not OLLAMA_AVAILABLE or not st.session_state.get('ollama_enabled', True)):
                                # --- PREPARA CONTEXTO PARA IA (COLUNA) ---
                                col_type = tech_col_data.get('type', 'N/A')
                                sample_data_col = tech_obj_data.get("SAMPLE_DATA")
                                formatted_sample_str_col = formatar_amostra_para_prompt(
                                    sample_data_col,
                                    technical_columns, # Lista completa de colunas do objeto
                                    tech_constraints,
                                    target_column=col_name # Indica a coluna foco
                                )
                                contexto_amostra_col = f"\\n\\nConsidere esta amostra de dados da tabela (foco em '{col_name}', pode conter valores fixos ou chaves):\\n{formatted_sample_str_col}" if formatted_sample_str_col else ""
                                # --- FIM PREPARA CONTEXTO ---
                                prompt_column = textwrap.dedent(f"""
                                    Sugira uma descrição concisa e informativa em português brasileiro para a coluna '{col_name}' (tipo: {col_type}) do objeto '{selected_object}'.
                                    Qual o significado desta coluna no contexto do negócio, considerando seu nome, tipo e, se disponíveis, os exemplos de dados abaixo?
                                    Se for uma chave ou código, explique o que ela representa.
                                    Se for um indicador ('1'/'0', 'S'/'N'), explique o que o valor ativo significa.
                                    Evite apenas repetir o nome da coluna.{contexto_amostra_col}

                                    Responda apenas a descrição.
                                """)
                                # Limpa prompt
                                prompt_column = "\\n".join([line.strip() for line in prompt_column.strip().splitlines()])

                                suggestion = generate_ai_description(prompt_column, OLLAMA_AVAILABLE, chat_completion)
                                if suggestion:
                                    st.session_state.metadata[metadata_key_type][selected_object]['COLUMNS'][col_name]['description'] = suggestion
                                    st.session_state.unsaved_changes = True
                                    st.rerun()

                                description_to_propagate = st.session_state.metadata.get(metadata_key_type,{}).get(selected_object,{}).get('COLUMNS',{}).get(col_name,{}).get('description', '').strip()
                                notes_to_propagate = st.session_state.metadata.get(metadata_key_type,{}).get(selected_object,{}).get('COLUMNS',{}).get(col_name,{}).get('value_mapping_notes', '').strip()
                                if description_to_propagate: # Só mostra se tem descrição para propagar
                                    if st.button("🔁", key=f"propagate_{col_name}", help="Propagar esta descrição e notas", use_container_width=True):
                                        source_concept = get_column_concept(technical_schema_data, selected_object, col_name)
                                        propagated_count = 0
                                        for obj_type_prop in list(st.session_state.metadata.keys()):
                                            if not isinstance(st.session_state.metadata[obj_type_prop], dict): continue
                                            for obj_name_prop, obj_meta_prop in st.session_state.metadata[obj_type_prop].items():
                                                if obj_name_prop not in technical_schema_data: continue
                                                if 'COLUMNS' not in obj_meta_prop: continue
                                                for col_name_prop, col_meta_prop_target in obj_meta_prop['COLUMNS'].items():
                                                    if obj_name_prop == selected_object and col_name_prop == col_name: continue
                                                    is_target_desc_empty = not col_meta_prop_target.get('description', '').strip()
                                                    if is_target_desc_empty:
                                                        target_concept = get_column_concept(technical_schema_data, obj_name_prop, col_name_prop)
                                                        if target_concept == source_concept:
                                                            st.session_state.metadata[obj_type_prop][obj_name_prop]['COLUMNS'][col_name_prop]['description'] = description_to_propagate
                                                            st.session_state.metadata[obj_type_prop][obj_name_prop]['COLUMNS'][col_name_prop]['value_mapping_notes'] = notes_to_propagate
                                                            propagated_count += 1
                                                            st.session_state.unsaved_changes = True # Marcar mudanças
                                        if propagated_count > 0:
                                            st.toast(f"Propagado para {propagated_count} coluna(s).", icon="✅")
                                        else: 
                                            st.toast("Nenhuma coluna encontrada para propagar.", icon="ℹ️")

                        # --- Notas de Mapeamento --- #
                        st.markdown("--- Notas de Mapeamento ---")
                        current_col_notes_saved = col_meta_data.get('value_mapping_notes', '').strip()
                        notes_value_to_display = current_col_notes_saved
                        heuristic_notes_source = None
                        if not current_col_notes_saved:
                            _, _, suggested_notes, notes_source_from_func = find_existing_info(
                                st.session_state.metadata, technical_schema_data, selected_object, col_name
                            )
                            if suggested_notes:
                                notes_value_to_display = suggested_notes
                                heuristic_notes_source = notes_source_from_func
                        if heuristic_notes_source: st.caption(f"ℹ️ Sugestão ({heuristic_notes_source}). Edite abaixo.")
                        
                        col_notes_key = f"notes_{selected_object}_{col_name}"
                        new_col_notes = st.text_area(
                            f"Notas Mapeamento (`{col_name}`)",
                            value=notes_value_to_display,
                            key=col_notes_key,
                            height=75,
                            label_visibility="collapsed",
                            help="Explique valores (1=Ativo) ou formatos.",
                            on_change=lambda: st.session_state.update({'unsaved_changes': True})
                        )
                        if new_col_notes != col_meta_data.get('value_mapping_notes', ''):
                            st.session_state.metadata[metadata_key_type][selected_object]['COLUMNS'][col_name]['value_mapping_notes'] = new_col_notes
                            st.session_state.unsaved_changes = True

            st.divider()

            # --- Seção de Pré-visualização e Exportação --- #
            with st.expander("👁️ Amostra de Dados e Exportação", expanded=False):
                # Colunas para Inputs e Botões
                num_rows_col, load_btn_col, export_txt_btn_col, export_xls_btn_col = st.columns([1,1,1,1])
                
                with num_rows_col:
                    num_rows_fetch = st.number_input("Linhas:", min_value=1, value=10, step=1, key=f"num_rows_{selected_object}", label_visibility="collapsed")
                
                with load_btn_col:
                    if st.button("Carregar", key=f"load_sample_{selected_object}", help="Carregar amostra na tela", use_container_width=True):
                        # Limpa estados de exportação anteriores
                        for suffix in ['excel', 'txt']:
                            for state_key in [f'{suffix}_export_data_{selected_object}', f'{suffix}_export_filename_{selected_object}', f'{suffix}_export_error_{selected_object}', f'{suffix}_export_bytes_{selected_object}']:
                                if state_key in st.session_state: st.session_state[state_key] = None
                        
                        # Busca dados (usa credenciais passadas)
                        sample_data_display = fetch_sample_data(db_path, db_user, db_password, db_charset, selected_object, num_rows_fetch)
                        st.session_state[f'sample_data_display_{selected_object}'] = sample_data_display
                        st.rerun() # Rerun para exibir

                # Botão Exportar TXT
                with export_txt_btn_col:
                    export_txt_key = f"export_txt_{selected_object}"
                    txt_data_key = f'txt_export_bytes_{selected_object}'
                    txt_name_key = f'txt_export_filename_{selected_object}'
                    txt_error_key = f'txt_export_error_{selected_object}'
                    
                    if st.button("Gerar TXT", key=f"generate_{export_txt_key}", help="Gerar amostra para TXT", use_container_width=True):
                        # Limpa estados de exportação anteriores
                        for suffix in ['excel', 'txt']:
                           for state_key in [f'{suffix}_export_data_{selected_object}', f'{suffix}_export_filename_{selected_object}', f'{suffix}_export_error_{selected_object}', f'{suffix}_export_bytes_{selected_object}']:
                               if state_key in st.session_state: st.session_state[state_key] = None
                        
                        export_data_txt = fetch_sample_data(db_path, db_user, db_password, db_charset, selected_object, num_rows_fetch)
                        if isinstance(export_data_txt, pd.DataFrame):
                            if not export_data_txt.empty:
                                try:
                                    df_to_export_txt = export_data_txt.copy()
                                    for col in df_to_export_txt.columns:
                                        if df_to_export_txt[col].dtype == 'object':
                                            first_non_null = df_to_export_txt[col].dropna().iloc[0] if not df_to_export_txt[col].dropna().empty else None
                                            if isinstance(first_non_null, bytes): df_to_export_txt[col] = df_to_export_txt[col].apply(lambda x: "[BLOB]" if isinstance(x, bytes) else x)
                                    txt_string = df_to_export_txt.to_string(index=False)
                                    st.session_state[txt_data_key] = txt_string.encode('utf-8')
                                    st.session_state[txt_name_key] = f"amostra_{selected_object}.txt"
                                except Exception as e: st.session_state[txt_error_key] = f"Erro TXT: {e}"
                            else: st.session_state[txt_error_key] = "Sem dados para TXT."
                        else: st.session_state[txt_error_key] = f"Erro busca TXT: {export_data_txt}"
                        st.rerun() # Rerun para mostrar botão download ou erro
                        
                    # Botão Download TXT (se dados prontos)
                    if st.session_state.get(txt_data_key) and st.session_state.get(txt_name_key):
                        st.download_button(label="⬇️ TXT", data=st.session_state[txt_data_key], file_name=st.session_state[txt_name_key], mime="text/plain", key=export_txt_key, use_container_width=True)
                    # Exibe erro se houver
                    if st.session_state.get(txt_error_key):
                        st.error(st.session_state[txt_error_key])
                        st.session_state[txt_error_key] = None # Limpa após exibir

                # Botão Exportar Excel
                with export_xls_btn_col:
                    export_xls_key = f"export_xls_{selected_object}"
                    xls_data_key = f'excel_export_data_{selected_object}'
                    xls_name_key = f'excel_export_filename_{selected_object}'
                    xls_error_key = f'excel_export_error_{selected_object}'

                    if st.button("Gerar Excel", key=f"generate_{export_xls_key}", help="Gerar amostra para Excel", use_container_width=True):
                         # Limpa estados de exportação anteriores
                        for suffix in ['excel', 'txt']:
                           for state_key in [f'{suffix}_export_data_{selected_object}', f'{suffix}_export_filename_{selected_object}', f'{suffix}_export_error_{selected_object}', f'{suffix}_export_bytes_{selected_object}']:
                               if state_key in st.session_state: st.session_state[state_key] = None
                        
                        export_data_xls = fetch_sample_data(db_path, db_user, db_password, db_charset, selected_object, num_rows_fetch)
                        if isinstance(export_data_xls, pd.DataFrame):
                            if not export_data_xls.empty:
                                try:
                                    df_to_export = export_data_xls.copy()
                                    for col in df_to_export.columns:
                                        if df_to_export[col].dtype == 'object':
                                            first_non_null = df_to_export[col].dropna().iloc[0] if not df_to_export[col].dropna().empty else None
                                            if isinstance(first_non_null, bytes): df_to_export[col] = df_to_export[col].apply(lambda x: "[BLOB]" if isinstance(x, bytes) else x)
                                    output = io.BytesIO()
                                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                        df_to_export.to_excel(writer, index=False, sheet_name=selected_object[:31])
                                    st.session_state[xls_data_key] = output.getvalue()
                                    st.session_state[xls_name_key] = f"amostra_{selected_object}.xlsx"
                                except Exception as e: st.session_state[xls_error_key] = f"Erro Excel: {e}"
                            else: st.session_state[xls_error_key] = "Sem dados para Excel."
                        else: st.session_state[xls_error_key] = f"Erro busca Excel: {export_data_xls}"
                        st.rerun()
                    
                    # Botão Download Excel
                    if st.session_state.get(xls_data_key) and st.session_state.get(xls_name_key):
                        st.download_button(label="⬇️ Excel", data=st.session_state[xls_data_key], file_name=st.session_state[xls_name_key], mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key=export_xls_key, use_container_width=True)
                    # Exibe erro se houver
                    if st.session_state.get(xls_error_key):
                        st.error(st.session_state[xls_error_key])
                        st.session_state[xls_error_key] = None # Limpa após exibir
                
                # Exibe o DataFrame carregado (se existir no estado)
                sample_data_result = st.session_state.get(f'sample_data_display_{selected_object}')
                if isinstance(sample_data_result, pd.DataFrame):
                    if not sample_data_result.empty:
                        st.dataframe(sample_data_result, use_container_width=True)
                    # Não exibe nada se vazio, msg de erro já tratada
                elif isinstance(sample_data_result, str):
                    # Erro já exibido pelo botão carregar, não precisa repetir
                    pass 
            
            # --- Botão Salvar (no final da página de edição) --- #
            st.divider()
            save_button_key = f"save_edit_{selected_object}" 
            if st.button("💾 Salvar Alterações neste Objeto", type="primary", key=save_button_key):
                # A lógica de salvamento principal agora está na sidebar (ui/sidebar.py)
                # Este botão pode ser removido ou ter sua funcionalidade ajustada.
                # Por ora, vamos assumir que ele chama a função save_metadata e
                # depois executa a lógica de limpeza de cache original.
                if save_metadata(st.session_state.metadata, config.METADATA_FILE):
                    st.success(f"Alterações em `{selected_object}` salvas!", icon="✅")
                    st.session_state.unsaved_changes = False # Reseta flag
                    # Limpa cache e atualiza estado inicial
                    try:
                        load_metadata.clear()
                        st.session_state.initial_metadata = copy.deepcopy(st.session_state.metadata)
                        st.session_state.last_save_time = time.time()
                    except Exception as e:
                        logger.warning(f"Erro ao limpar cache/atualizar estado pós-save: {e}")
                else:
                    st.error(f"Falha ao salvar alterações em `{selected_object}`.")

    else: # Nenhum objeto selecionado
        st.info("Selecione um objeto na lista acima para editar seus metadados.") 