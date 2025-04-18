# Funções para lógica de metadados e heurísticas

import logging
import re
import os
import json
from collections import OrderedDict
import streamlit as st # TODO: Refatorar para remover dependência do Streamlit (ex: st.error em save_metadata)
import shutil # NOVO: Para copiar arquivos
import datetime # NOVO: Para timestamp no nome do backup
import pandas as pd
import src.core.config as config

logger = logging.getLogger(__name__)

# --- Funções Auxiliares / Utilitárias --- #

def get_type_explanation(type_string):
    """Tenta encontrar uma explicação para o tipo SQL base."""
    if not type_string:
        return ""
    # Usa TYPE_EXPLANATIONS do config
    base_type = re.match(r"^([A-Z\s_]+)", type_string.upper())
    if base_type:
        explanation = config.TYPE_EXPLANATIONS.get(base_type.group(1).strip())
        return f"*{explanation}*" if explanation else ""
    return ""

def save_metadata(data, file_path):
    """Salva os dados (dicionário) de volta no arquivo JSON, criando um backup antes."""
    backup_dir = os.path.join(os.path.dirname(file_path), "metadata_backups")
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_{timestamp}.json"
    backup_path = os.path.join(backup_dir, backup_filename)

    try:
        # 1. Criar diretório de backup se não existir
        os.makedirs(backup_dir, exist_ok=True)
        logger.debug(f"Diretório de backup verificado/criado: {backup_dir}")

        # 2. Criar backup do arquivo existente (se ele existir)
        if os.path.exists(file_path):
            try:
                shutil.copy2(file_path, backup_path) # copy2 preserva metadados
                logger.info(f"Backup do metadado criado em: {backup_path}")
            except Exception as backup_err:
                logger.error(f"Falha ao criar backup do metadado em {backup_path}: {backup_err}")
                # Continuar mesmo se o backup falhar? Ou retornar erro?
                # Por segurança, vamos continuar, mas logamos o erro.

        # 3. Salvar o novo arquivo
        # Garante que o diretório principal exista (caso seja a primeira vez)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Metadados salvos em {file_path}")
        return True
    except IOError as e:
        # TODO: Lançar exceção em vez de usar st.error
        st.error(f"Erro ao salvar o arquivo JSON em {file_path}: {e}")
        return False
    except Exception as e:
        # TODO: Lançar exceção em vez de usar st.error
        st.error(f"Erro inesperado ao salvar o JSON em {file_path}: {e}")
        logger.exception(f"Erro inesperado ao salvar o JSON em {file_path}")
        return False

# --- Funções de Heurística e Preenchimento --- #

def find_existing_info(metadata, schema_data, current_object_name, target_col_name):
    """
    Procura por informações existentes (descrição e notas) para uma coluna:
    1. Busca por nome exato em outras tabelas/views (para descrição e notas).
    2. Se for FK, busca a descrição da PK referenciada (apenas descrição).
    3. Se for PK, busca a descrição de uma coluna FK que a referencie (apenas descrição).
    4. Verifica comentário do banco de dados (apenas descrição).

    Retorna: (desc_sugerida, fonte_desc, notas_sugeridas, fonte_notas)
    """
    if not metadata or not schema_data or not target_col_name or not current_object_name:
        return None, None, None, None # Retorna None para tudo

    # --- 1. Verificar Comentário do Banco de Dados (APENAS para Descrição) ---
    current_object_info = schema_data.get(current_object_name)
    if current_object_info:
        tech_col_info = None
        for col_def in current_object_info.get('columns', []):
            if col_def.get('name') == target_col_name:
                tech_col_info = col_def
                break
        if tech_col_info:
            db_comment_raw = tech_col_info.get('description')
            if db_comment_raw:
                db_comment = db_comment_raw.strip()
                if db_comment:
                    obj_type = current_object_info.get('object_type', 'TABLE')
                    obj_type_key = obj_type + "S"
                    manual_desc = metadata.get(obj_type_key, {}).get(current_object_name, {}).get('COLUMNS', {}).get(target_col_name, {}).get('description','').strip()
                    if not manual_desc:
                        logger.debug(f"Heurística: Descrição encontrada via comentário do DB para {current_object_name}.{target_col_name}")
                        return db_comment, "database comment", None, None

    # 2. Busca por nome exato
    for obj_type_key in ['TABLES', 'VIEWS']:
        for obj_name, obj_meta in metadata.get(obj_type_key, {}).items():
            if obj_name == current_object_name: continue
            if not isinstance(obj_meta, dict):
                logger.warning(f"[find_existing_info] Esperava um dicionário para {obj_type_key}.{obj_name}, mas encontrou {type(obj_meta)}. Pulando este objeto.")
                continue
            col_meta = obj_meta.get('COLUMNS', {}).get(target_col_name)
            if col_meta:
                found_desc = col_meta.get('description', '').strip()
                found_notes = col_meta.get('value_mapping_notes', '').strip()
                if found_desc or found_notes:
                    source = f"nome exato em `{obj_name}`"
                    logger.debug(f"Heurística: Informação encontrada por {source} para {current_object_name}.{target_col_name}")
                    return found_desc, source, found_notes, source

    # 3. Busca Direta (Se target_col é FK)
    if current_object_info:
        current_constraints = current_object_info.get('constraints', {})
        for fk in current_constraints.get('foreign_keys', []):
            fk_columns = fk.get('columns', [])
            ref_table = fk.get('references_table')
            ref_columns = fk.get('references_columns', [])
            if target_col_name in fk_columns and ref_table and ref_columns:
                try:
                    idx = fk_columns.index(target_col_name)
                    ref_col_name = ref_columns[idx]
                    ref_object_info = schema_data.get(ref_table)
                    if not ref_object_info:
                        logger.warning(f"Schema técnico não encontrado para tabela referenciada {ref_table}")
                        continue
                    ref_obj_type = ref_object_info.get('object_type', 'TABLE')
                    ref_obj_type_key = ref_obj_type + "S"
                    ref_col_meta = metadata.get(ref_obj_type_key, {}).get(ref_table, {}).get('COLUMNS', {}).get(ref_col_name)
                    if ref_col_meta and ref_col_meta.get('description', '').strip():
                        desc = ref_col_meta['description']
                        source = f"chave estrangeira para `{ref_table}.{ref_col_name}`"
                        logger.debug(f"Heurística: Descrição encontrada por {source} para {current_object_name}.{target_col_name}")
                        return desc, source, None, None
                except (IndexError, ValueError): continue

        # 4. Busca Inversa (Se target_col é PK)
        current_pk_cols = [col for pk in current_constraints.get('primary_key', []) for col in pk.get('columns', [])]
        if target_col_name in current_pk_cols:
            for other_obj_name, other_obj_info in schema_data.items():
                if other_obj_name == current_object_name: continue
                other_constraints = other_obj_info.get('constraints', {})
                for other_fk in other_constraints.get('foreign_keys', []):
                    if other_fk.get('references_table') == current_object_name and target_col_name in other_fk.get('references_columns', []):
                        referencing_columns = other_fk.get('columns', [])
                        ref_pk_columns = other_fk.get('references_columns', [])
                        try:
                            idx_pk = ref_pk_columns.index(target_col_name)
                            referencing_col_name = referencing_columns[idx_pk]
                            other_obj_type = other_obj_info.get('object_type', 'TABLE')
                            other_obj_type_key = other_obj_type + "S"
                            other_col_meta = metadata.get(other_obj_type_key, {}).get(other_obj_name, {}).get('COLUMNS', {}).get(referencing_col_name)
                            if other_col_meta and other_col_meta.get('description', '').strip():
                                desc = other_col_meta['description']
                                source = f"coluna `{referencing_col_name}` em `{other_obj_name}` (ref. esta PK)"
                                logger.debug(f"Heurística: Descrição encontrada por {source} para {current_object_name}.{target_col_name}")
                                return desc, source, None, None
                        except (IndexError, ValueError): continue

    return None, None, None, None

def get_column_concept(schema_data, obj_name, col_name):
    """Determina o conceito raiz (PK referenciada ou a própria PK/coluna)."""
    if not schema_data or obj_name not in schema_data:
        return (obj_name, col_name)
    obj_info = schema_data[obj_name]
    constraints = obj_info.get('constraints', {})
    for fk in constraints.get('foreign_keys', []):
        fk_columns = fk.get('columns', [])
        ref_table = fk.get('references_table')
        ref_columns = fk.get('references_columns', [])
        if col_name in fk_columns and ref_table and ref_columns:
            try:
                idx = fk_columns.index(col_name)
                return (ref_table, ref_columns[idx])
            except (IndexError, ValueError): pass
    return (obj_name, col_name)

def apply_heuristics_globally(metadata_dict, technical_schema):
    """Aplica a heurística find_existing_info a todas as colunas vazias."""
    logger.info("Iniciando aplicação global da heurística...")
    updated_desc_count = 0
    updated_notes_count = 0
    already_filled_desc_count = 0
    already_filled_notes_count = 0
    not_found_count = 0
    columns_processed = 0

    objects_to_process = {}
    for obj_type_key in ['TABLES', 'VIEWS']:
        if obj_type_key in metadata_dict:
            objects_to_process.update(metadata_dict[obj_type_key])

    for obj_name, obj_meta in objects_to_process.items():
        if 'COLUMNS' not in obj_meta:
            continue
        columns_meta = obj_meta['COLUMNS']
        for col_name, col_meta_target in columns_meta.items():
            columns_processed += 1
            current_desc = col_meta_target.get('description', '').strip()
            current_notes = col_meta_target.get('value_mapping_notes', '').strip()
            found_something_new = False

            if not current_desc or not current_notes:
                suggested_desc, desc_source, suggested_notes, notes_source = find_existing_info(
                    metadata_dict, technical_schema, obj_name, col_name
                )

                if not current_desc and suggested_desc:
                    logger.debug(f"Heurística global (Descrição): Atualizando '{obj_name}.{col_name}' com base em '{desc_source}'")
                    col_meta_target['description'] = suggested_desc
                    updated_desc_count += 1
                    found_something_new = True
                elif current_desc:
                    already_filled_desc_count += 1

                if not current_notes and suggested_notes:
                    logger.debug(f"Heurística global (Notas): Atualizando '{obj_name}.{col_name}' com base em '{notes_source}'")
                    col_meta_target['value_mapping_notes'] = suggested_notes
                    updated_notes_count += 1
                    found_something_new = True
                elif current_notes:
                    already_filled_notes_count += 1

                if not found_something_new and (not current_desc or not current_notes):
                    not_found_count += 1
            else:
                already_filled_desc_count += 1
                already_filled_notes_count += 1

    logger.info(f"Aplicação global da heurística concluída.")
    logger.info(f"  Descrições: {updated_desc_count} atualizadas, {already_filled_desc_count} já preenchidas.")
    logger.info(f"  Notas: {updated_notes_count} atualizadas, {already_filled_notes_count} já preenchidas.")
    logger.info(f"  Colunas onde nenhuma sugestão foi encontrada (para campos vazios): {not_found_count}")

    return updated_desc_count, updated_notes_count

def populate_descriptions_from_keys(metadata_dict, technical_schema):
    """Preenche descrições de FKs vazias com base nas descrições das PKs referenciadas."""
    logger.info("Iniciando preenchimento de descrições via chaves FK -> PK...")
    updated_count = 0
    processed_fk_cols = 0

    for table_name, table_data in technical_schema.items():
        if not isinstance(table_data, dict) or table_data.get('object_type') not in ['TABLE', 'VIEW']:
            continue
        obj_type = table_data.get('object_type', 'TABLE')
        obj_type_key = obj_type + "S"
        constraints = table_data.get('constraints', {})
        foreign_keys = constraints.get('foreign_keys', [])

        if obj_type_key not in metadata_dict: metadata_dict[obj_type_key] = OrderedDict()
        if table_name not in metadata_dict[obj_type_key]: metadata_dict[obj_type_key][table_name] = OrderedDict({'description': '', 'COLUMNS': OrderedDict()})
        if 'COLUMNS' not in metadata_dict[obj_type_key][table_name]: metadata_dict[obj_type_key][table_name]['COLUMNS'] = OrderedDict()
        current_table_meta_cols = metadata_dict[obj_type_key][table_name]['COLUMNS']

        for fk in foreign_keys:
            fk_cols = fk.get('columns', [])
            ref_table = fk.get('references_table')
            ref_cols = fk.get('references_columns', [])

            if not ref_table or len(fk_cols) != len(ref_cols):
                logger.warning(f"FK malformada em {table_name}: {fk}")
                continue

            ref_table_data = technical_schema.get(ref_table)
            if not ref_table_data:
                logger.warning(f"Tabela referenciada {ref_table} não encontrada no schema técnico.")
                continue
            ref_obj_type = ref_table_data.get('object_type', 'TABLE')
            ref_obj_type_key = ref_obj_type + "S"

            for i, fk_col_name in enumerate(fk_cols):
                processed_fk_cols += 1
                ref_col_name = ref_cols[i]

                if fk_col_name not in current_table_meta_cols:
                    current_table_meta_cols[fk_col_name] = OrderedDict()
                fk_col_meta = current_table_meta_cols[fk_col_name]

                current_fk_desc = fk_col_meta.get('description', '').strip()
                if not current_fk_desc:
                    ref_table_meta = metadata_dict.get(ref_obj_type_key, {}).get(ref_table, {})
                    ref_col_meta = ref_table_meta.get('COLUMNS', {}).get(ref_col_name, {})
                    ref_pk_desc = ref_col_meta.get('description', '').strip()
                    if ref_pk_desc:
                        source_str = f"key -> {ref_table}.{ref_col_name}"
                        logger.debug(f"Preenchendo '{table_name}.{fk_col_name}' via {source_str}")
                        fk_col_meta['description'] = ref_pk_desc
                        updated_count += 1

    logger.info(f"Preenchimento via chaves concluído. Colunas FK processadas: {processed_fk_cols}. Descrições atualizadas: {updated_count}")
    return updated_count

# --- Função de Comparação de Metadados --- #

def compare_metadata_changes(initial_meta, current_meta):
    """Compara dois dicionários de metadados e conta novas descrições/notas."""
    new_descriptions = 0
    new_notes = 0
    if not initial_meta or not current_meta:
        logger.warning("Metadados iniciais ou atuais ausentes para comparação.")
        return 0, 0

    for obj_type_key in list(current_meta.keys()):
        if obj_type_key not in ['TABLES', 'VIEWS']:
            continue
        current_objects = current_meta.get(obj_type_key, {})
        initial_objects = initial_meta.get(obj_type_key, {})

        for obj_name, current_obj_data in current_objects.items():
            initial_obj_data = initial_objects.get(obj_name, {})
            current_cols = current_obj_data.get('COLUMNS', {})
            initial_cols = initial_obj_data.get('COLUMNS', {})

            for col_name, current_col_data in current_cols.items():
                initial_col_data = initial_cols.get(col_name, {})

                # Compara Descrição
                current_desc = current_col_data.get('description', '').strip()
                initial_desc = initial_col_data.get('description', '').strip()
                if current_desc and not initial_desc:
                    new_descriptions += 1

                # Compara Notas de Mapeamento
                current_notes = current_col_data.get('value_mapping_notes', '').strip()
                initial_notes = initial_col_data.get('value_mapping_notes', '').strip()
                if current_notes and not initial_notes:
                    new_notes += 1

    logger.info(f"Comparação de metadados: {new_descriptions} novas descrições, {new_notes} novas notas.")
    return new_descriptions, new_notes 