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
from src.ollama_integration.ai_integration import (
    generate_ai_description,
    find_similar_columns,
    get_query_embedding,
    # OLLAMA_AVAILABLE e chat_completion são passados como argumentos
)
from src.database.db_utils import fetch_sample_data
# Funções como populate_descriptions_from_keys, apply_heuristics_globally são chamadas pela sidebar

logger = logging.getLogger(__name__)

# --- NOVA FUNÇÃO CALLBACK --- #
def update_metadata_value(key, obj_type, obj_name, col_name=None, field_name=None):
    """Callback para atualizar st.session_state.metadata diretamente."""
    new_value = st.session_state.get(key)
    if new_value is None:
        logger.warning(f"Callback de atualização chamado para key '{key}' mas valor não encontrado no estado.")
        return

    try:
        # Primeiro, garanta a estrutura do objeto e das chaves de descrição na ordem correta
        if obj_type in st.session_state.metadata and obj_name in st.session_state.metadata[obj_type]:
            obj_meta = st.session_state.metadata[obj_type][obj_name]
            # Garante a ordem das chaves do objeto antes de COLUMNS
            obj_meta.setdefault('description', '')
            obj_meta.setdefault('object_business_description', '')
            obj_meta.setdefault('object_value_mapping_notes', '')
            # Move COLUMNS para o final se já existir
            if 'COLUMNS' in obj_meta:
                obj_meta.move_to_end('COLUMNS')
        else:
            # Cria o objeto com as chaves na ordem certa, se não existir
            if obj_type not in st.session_state.metadata:
                st.session_state.metadata[obj_type] = OrderedDict()
            st.session_state.metadata[obj_type][obj_name] = OrderedDict([
                ('description', ''), 
                ('object_business_description', ''), 
                ('object_value_mapping_notes', ''), 
                ('COLUMNS', OrderedDict()) # Cria COLUMNS aqui
            ])
            obj_meta = st.session_state.metadata[obj_type][obj_name]

        # Atualiza campo de coluna (nested) para business_description e value_mapping_notes
        if col_name and field_name in ['business_description','value_mapping_notes']:
            obj_meta = st.session_state.metadata[obj_type][obj_name]
            col_map = obj_meta.setdefault('COLUMNS', OrderedDict())
            col_meta = col_map.setdefault(col_name, OrderedDict())
            current_value = col_meta.get(field_name, '') or ''
            if new_value.strip() != current_value.strip():
                col_meta[field_name] = new_value
                st.session_state.unsaved_changes = True
                logger.debug(f"Callback: Atualizado {obj_name}.{col_name}.{field_name} para '{new_value[:50]}...'" )
            return
        elif field_name == 'description':
            # Atualizando descrição do objeto (já garantido existir acima)
            current_value = obj_meta.get('description', '')
            if new_value.strip() != current_value.strip():
                obj_meta['description'] = new_value
                st.session_state.unsaved_changes = True
                logger.debug(f"Callback: Atualizado {obj_name}.description para '{new_value[:50]}...'")
        elif field_name in ['object_business_description', 'object_value_mapping_notes']:
            # Atualizando outros campos do objeto (já garantido existir acima)
            current_value = obj_meta.get(field_name, '')
            if new_value.strip() != current_value.strip():
                obj_meta[field_name] = new_value
                st.session_state.unsaved_changes = True
                logger.debug(f"Callback: Atualizado {obj_name}.{field_name} para '{new_value[:50]}...'")
        else:
            if not col_name:
                logger.error(f"Callback de atualização chamado com argumentos inválidos: obj={obj_name}, col={col_name}, field={field_name}")
            # Ignora erros de coluna (campo não suportado)
            
    except Exception as e:
        logger.error(f"Erro dentro do callback update_metadata_value: {e}", exc_info=True)
# --- FIM NOVA FUNÇÃO CALLBACK --- #

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

    # --- LOG DE DEBUG ADICIONADO --- #
    if metadata_dict:
        logger.debug(f"[display_edit_page] Recebido metadata_dict com chaves: {list(metadata_dict.keys())}")
        # Opcional: Logar um trecho específico para mais detalhes
        # first_table_key = next(iter(metadata_dict.get('TABLES', {})), None)
        # if first_table_key:
        #     logger.debug(f"[display_edit_page] Descrição da primeira tabela ({first_table_key}): {metadata_dict['TABLES'][first_table_key].get('description', 'N/A')}")
    else:
        logger.warning("[display_edit_page] Recebido metadata_dict vazio ou None.")
    # --- FIM LOG DE DEBUG --- #

    # Inicializa estado da sessão específico da página, se necessário
    if 'current_table_for_ai' not in st.session_state:
        st.session_state.current_table_for_ai = None

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

    # --- Filtros --- #
    filter_col1, filter_col2, filter_col3 = st.columns([1, 2, 1])
    with filter_col1:
        show_only_with_data = st.checkbox("Mostrar apenas com dados (>0 linhas)", key="filter_empty_objects", value=False)

    object_types_available = sorted(list(set(all_technical_objects.values())))
    with filter_col2:
        selected_type_display = st.radio("Filtrar por Tipo:", ["Todos"] + object_types_available, horizontal=True, index=0, key="type_filter_radio")
    with filter_col3:
        search_term = st.text_input("Buscar Objeto:", key="search_object_input", placeholder="Nome...")

    # --- Lógica de Filtragem Combinada --- #
    # 1. Pega contagens
    overview_counts_data = st.session_state.get('overview_counts', {})
    counts_map = overview_counts_data.get('counts', {}) # Acessa o dicionário interno "counts"

    # 2. Filtra por contagem se necessário
    objects_to_consider = list(all_technical_objects.keys())
    if show_only_with_data:
        # Tenta buscar a contagem usando a chave com e sem o prefixo "table:"
        objects_to_consider = [
            name for name in objects_to_consider
            if counts_map.get(f"table:{name}", counts_map.get(name, 0)) > 0
        ]

    # 3. Filtra por tipo
    if selected_type_display == "Todos":
        base_object_names = sorted(objects_to_consider)
    else:
        base_object_names = sorted([
            name for name in objects_to_consider
            if all_technical_objects.get(name) == selected_type_display
        ])

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

        # Não criamos mapeamento nested prévio; obj_data é definido dinamicamente na seção abaixo
        tech_constraints = tech_obj_data.get('constraints', {}) # Pega as constraints

        if not tech_obj_data:
            st.error(f"Dados técnicos não encontrados para '{selected_object}'")
        else:
            st.subheader(f"Editando: `{selected_object}` ({tech_obj_data.get('object_type', 'Desconhecido')})", divider='rainbow')
            
            # --- Seção de Edição de Metadados do Objeto ---
            # Carrega metadados do objeto a partir do nested (METADATA_FILE)
            md = st.session_state.metadata
            obj_type_key = metadata_key_type  # ex: 'TABLES' ou 'VIEWS'
            if obj_type_key in md and selected_object in md[obj_type_key]:
                obj_data = md[obj_type_key][selected_object]
            else:
                obj_data = {}
            # Obtém valores existentes ou string vazia
            desc_val = obj_data.get('description') or ''
            bus_val = obj_data.get('object_business_description') or ''
            map_val = obj_data.get('object_value_mapping_notes') or ''

            st.markdown("**Descrição do Objeto**")
            # Três campos para edição e botão IA
            col1, col2, col3, btn_ai = st.columns([3, 3, 3, 1])
            # Descrição Geral
            key_desc = f"desc_{selected_object}"
            with col1:
                st.text_area(
                    "Descrição Geral", value=desc_val, key=key_desc, height=100, label_visibility="visible",
                    on_change=update_metadata_value,
                    kwargs=dict(key=key_desc, obj_type=metadata_key_type, obj_name=selected_object, field_name='description')
                )
            # Descrição de Negócio
            key_bus = f"bus_{selected_object}"
            with col2:
                st.text_area(
                    "Descrição de Negócio", value=bus_val, key=key_bus, height=100, label_visibility="visible",
                    on_change=update_metadata_value,
                    kwargs=dict(key=key_bus, obj_type=metadata_key_type, obj_name=selected_object, field_name='object_business_description')
                )
            # Notas de Mapeamento de Valor
            key_map = f"map_{selected_object}"
            with col3:
                st.text_area(
                    "Notas de Mapeamento de Valor", value=map_val, key=key_map, height=100, label_visibility="visible",
                    on_change=update_metadata_value,
                    kwargs=dict(key=key_map, obj_type=metadata_key_type, obj_name=selected_object, field_name='object_value_mapping_notes')
                )
            # Botão IA para objeto
            with btn_ai:
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

            # --- NOVO: Exibir Descrição AI do Objeto (se existir) ---
            ai_obj_desc_list = st.session_state.get('ai_object_descriptions', [])
            ai_obj_desc_data = None
            # Procura o dicionário correspondente ao objeto selecionado na lista
            if isinstance(ai_obj_desc_list, list):
                for item in ai_obj_desc_list:
                    if isinstance(item, dict) and item.get('object_name') == selected_object:
                        ai_obj_desc_data = item
                        break
            # Se encontrou o dicionário e ele tem a descrição:
            # --- DEBUG TEMPORÁRIO --- #
            logger.info(f"[Debug AI Obj Desc] Selected Object: {selected_object}")
            logger.info(f"[Debug AI Obj Desc] Found ai_obj_desc_data: {ai_obj_desc_data}")
            # --- FIM DEBUG --- #

            if ai_obj_desc_data and ai_obj_desc_data.get('generated_description'):
                with st.expander("👁️ Ver Descrição da IA (Objeto)", expanded=False):
                    ai_desc = ai_obj_desc_data['generated_description']
                    ai_model = ai_obj_desc_data.get('ai_model_used', 'N/A')
                    ai_ts_str = ai_obj_desc_data.get('ai_generation_timestamp')
                    ai_ts_display = "N/A"
                    if ai_ts_str:
                        try:
                            # Importar datetime se ainda não foi importado no início do arquivo
                            import datetime 
                            ai_ts_dt = datetime.datetime.fromisoformat(ai_ts_str.replace('Z', '+00:00'))
                            ai_ts_display = ai_ts_dt.strftime("%d/%m/%Y %H:%M")
                        except (ValueError, ImportError):
                            ai_ts_display = "Data inválida"
                    st.caption(f"Gerada por: {ai_model} em {ai_ts_display}")
                    st.markdown(f"> _{ai_desc}_")

            # --- NOVO: Exibir Dados de Exemplo --- #
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

                        # --- Campos de Metadados de Coluna ---
                        # Garante chaves iniciais conforme JSON
                        col_meta_data.setdefault('business_description', '')
                        col_meta_data.setdefault('value_mapping_notes', '')
                        # Layout: duas colunas para business_description e value_mapping_notes
                        bus_area, val_area = st.columns([4, 4])
                        # Campo: Descrição de Negócio
                        bus_key = f"bus_{selected_object}_{col_name}"
                        with bus_area:
                            st.text_area(
                                f"Descrição de Negócio (`{col_name}`)",
                                value=col_meta_data.get('business_description', ''),
                                key=bus_key,
                                height=75,
                                label_visibility="visible",
                                on_change=update_metadata_value,
                                kwargs=dict(key=bus_key, obj_type=metadata_key_type, obj_name=selected_object, col_name=col_name, field_name='business_description')
                            )
                        # Campo: Notas de Mapeamento
                        val_key = f"notes_{selected_object}_{col_name}"
                        with val_area:
                            st.text_area(
                                f"Notas de Mapeamento (`{col_name}`)",
                                value=col_meta_data.get('value_mapping_notes', ''),
                                key=val_key,
                                height=75,
                                label_visibility="visible",
                                on_change=update_metadata_value,
                                kwargs=dict(key=val_key, obj_type=metadata_key_type, obj_name=selected_object, col_name=col_name, field_name='value_mapping_notes')
                            )

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
            # --- FIM: Seção de Pré-visualização e Exportação ---

        # --- FIM Bloco de Edição Colunas --- #

        # Removido o botão "Salvar Alterações neste Objeto"

    # --- Bloco ELSE correspondente ao 'if selected_object:' --- #
    else:
        st.info("Selecione um objeto na lista acima para editar seus metadados.")

    # --- FIM: Edição dos Metadados --- # 