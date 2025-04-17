# Código da interface para o modo 'Editar Metadados'

import streamlit as st
import os
import re
from collections import OrderedDict
import logging
import numpy as np
import io
import pandas as pd

# Importações de módulos core
import core.config as config
from core.metadata_logic import (
    get_type_explanation,
    find_existing_info,
    get_column_concept,
    save_metadata,
    compare_metadata_changes
)
from core.ai_integration import (
    generate_ai_description,
    find_similar_columns
    # OLLAMA_AVAILABLE e chat_completion são passados como argumentos
)
from core.db_utils import fetch_sample_data
# Funções como populate_descriptions_from_keys, apply_heuristics_globally são chamadas pela sidebar

logger = logging.getLogger(__name__)

def display_edit_page(technical_schema_data, metadata_dict, OLLAMA_AVAILABLE, chat_completion, db_path, db_user, db_password, db_charset):
    """Renderiza a página de Edição de Metadados."""

    st.header("Editor de Metadados")
    # Ajuste para usar config diretamente
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

    if selected_type_display == "Todos": 
        object_names = sorted(list(all_technical_objects.keys()))
    elif selected_type_display in object_types_available: 
        object_names = sorted([name for name, type in all_technical_objects.items() if type == selected_type_display])
    else: 
        object_names = []

    if not object_names: 
        st.warning(f"Nenhum objeto do tipo '{selected_type_display}'.")
        selected_object = None
    else: 
        # Usar st.session_state para persistir a seleção entre reruns
        if 'selected_object' not in st.session_state:
             st.session_state.selected_object = object_names[0] # Default para o primeiro da lista
        
        # Encontra o índice atual da seleção para o selectbox
        try:
            current_index = object_names.index(st.session_state.selected_object)
        except ValueError:
             current_index = 0 # Default para 0 se o objeto salvo não estiver mais na lista filtrada
             st.session_state.selected_object = object_names[0] if object_names else None

        selected_object = st.selectbox(
            "Selecione o Objeto para Editar", 
            object_names, 
            index=current_index,
            key="selectbox_edit_object" # Adiciona chave única
        )
        # Atualiza o estado da sessão se a seleção mudar
        if selected_object != st.session_state.selected_object:
             st.session_state.selected_object = selected_object
             # Resetar índice da coluna ao mudar de objeto?
             # if 'selected_column_index' in st.session_state:
             #     del st.session_state['selected_column_index']
             st.rerun() # Força rerun para atualizar UI com novo objeto

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
        
        if not tech_obj_data: 
            st.error(f"Dados técnicos não encontrados para '{selected_object}'")
        else:
            st.subheader(f"Editando: `{selected_object}` ({tech_obj_data.get('object_type', 'Desconhecido')})", divider='rainbow')
            
            # --- Bloco de Edição Objeto --- 
            col1_edit, col2_edit = st.columns([1, 2])
            with col1_edit:
                st.markdown("**Descrição do Objeto**")
                obj_desc_key = f"desc_{selected_object_technical_type}_{selected_object}"
                if "description" not in obj_data: obj_data["description"] = "" # Garante que a chave existe
                desc_obj_area, btn_ai_obj_area = st.columns([4, 1])
                with desc_obj_area:
                    new_obj_desc = st.text_area(
                        "Descrição Geral", 
                        value=obj_data.get("description", ""), 
                        key=obj_desc_key, 
                        height=100, 
                        label_visibility="collapsed",
                        on_change=lambda: st.session_state.update({'unsaved_changes': True})
                    )
                    # Atualiza o dicionário diretamente (cuidado com reruns)
                    # É mais seguro atualizar via callback ou botão salvar
                    # Se o valor mudou, atualiza o estado da sessão
                    if new_obj_desc != obj_data.get("description", ""):
                        st.session_state.metadata[metadata_key_type][selected_object]['description'] = new_obj_desc
                        st.session_state.unsaved_changes = True # Marca que há mudanças
                        # st.rerun() # Pode causar loop se não tratado com cuidado

                with btn_ai_obj_area:
                    # Passa OLLAMA_AVAILABLE e chat_completion
                    if st.button("Sugerir IA", key=f"btn_ai_obj_{selected_object}", use_container_width=True, disabled=not OLLAMA_AVAILABLE or not st.session_state.get('ollama_enabled', True)):
                        prompt_object = f"Sugira descrição concisa pt-br para o objeto de banco de dados '{selected_object}' (tipo: {selected_object_technical_type}). Propósito? Responda só descrição."
                        # Passa OLLAMA_AVAILABLE e chat_completion
                        suggestion = generate_ai_description(prompt_object, OLLAMA_AVAILABLE, chat_completion)
                        if suggestion:
                             st.session_state.metadata[metadata_key_type][selected_object]['description'] = suggestion
                             st.session_state.unsaved_changes = True
                             st.rerun()
                             
            # --- Bloco de Edição Colunas --- 
            with col2_edit:
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
                                    prompt_column = f"Sugira descrição concisa pt-br para coluna '{col_name}' ({col_type}) do objeto '{selected_object}'. Significado? Responda só descrição."
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
                # Lógica de comparação e salvamento movida para a sidebar, 
                # mas pode ter um save específico aqui se desejado.
                # Por ora, apenas chama a função save_metadata importada.
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