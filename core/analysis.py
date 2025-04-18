# Funções de análise estrutural 

import logging
from collections import defaultdict
import streamlit as st # Para @st.cache_data
import pandas as pd
import numpy as np
import datetime

logger = logging.getLogger(__name__)

@st.cache_data # Cacheia a análise estrutural, pois só depende do schema técnico
def analyze_key_structure(schema_data):
    logger.info("---> EXECUTANDO analyze_key_structure") # Log de diagnóstico
    """Analisa o schema_data para identificar tipos de chaves e calcular importância inicial."""
    logger.info("Analisando estrutura de chaves do schema...")
    composite_pk_tables = {}
    junction_tables = {}
    composite_fk_details = {}
    column_roles = defaultdict(lambda: {'role': 'Normal', 'importance_score': 0, 'details': ''}) # Default para coluna normal

    fk_ref_counts = schema_data.get('fk_reference_counts', {})

    for table_name, table_data in schema_data.items():
        if not isinstance(table_data, dict) or table_data.get('object_type') not in ['TABLE', 'VIEW']:
            continue

        constraints = table_data.get('constraints', {})
        primary_keys = constraints.get('primary_key', [])
        foreign_keys = constraints.get('foreign_keys', [])
        columns_in_table = {col.get('name') for col in table_data.get('columns', []) if col.get('name')}

        # 1. Analisar Chaves Primárias
        pk_column_names = set()
        is_composite_pk = False
        if primary_keys:
            pk_def = primary_keys[0] # Assume-se uma PK por tabela para simplificar
            pk_cols = pk_def.get('columns', [])
            pk_column_names.update(pk_cols)
            if len(pk_cols) > 1:
                is_composite_pk = True
                composite_pk_tables[table_name] = pk_cols
                for col_name in pk_cols:
                    column_roles[(table_name, col_name)]['role'] = 'PK Comp'
                    column_roles[(table_name, col_name)]['importance_score'] += 5 # Alta importância base
            elif len(pk_cols) == 1:
                 col_name = pk_cols[0]
                 column_roles[(table_name, col_name)]['role'] = 'PK'
                 column_roles[(table_name, col_name)]['importance_score'] += 3 # Importância base média

        # 2. Analisar Chaves Estrangeiras e Tabelas de Junção
        is_junction_table = False
        junction_fk_details = []
        fk_columns_in_table = set()

        for fk in foreign_keys:
            fk_cols = fk.get('columns', [])
            ref_table = fk.get('references_table')
            ref_cols = fk.get('references_columns', [])
            fk_columns_in_table.update(fk_cols)

            if len(fk_cols) > 1:
                # FK Composta
                for i, col_name in enumerate(fk_cols):
                    if col_name in column_roles[(table_name, col_name)] and column_roles[(table_name, col_name)]['role'] == 'PK Comp':
                         column_roles[(table_name, col_name)]['role'] = 'PK/FK Comp' # Promove se for PK e FK composta
                         column_roles[(table_name, col_name)]['importance_score'] += 2 # Bônus
                    elif col_name not in pk_column_names: # Só marca como FK Comp se não for PK simples
                        column_roles[(table_name, col_name)]['role'] = 'FK Comp'
                    column_roles[(table_name, col_name)]['importance_score'] += 1 # Leve aumento por ser parte de FK composta
                    try: ref_col_name = ref_cols[i] if ref_cols and i < len(ref_cols) else 'N/A'
                    except IndexError: ref_col_name = 'N/A'
                    detail_str = f"parte de FK composta referenciando {ref_table}.{ref_col_name}"
                    column_roles[(table_name, col_name)]['details'] = detail_str
                    composite_fk_details[(table_name, col_name)] = detail_str
            elif len(fk_cols) == 1:
                # FK Simples
                col_name = fk_cols[0]
                if col_name in pk_column_names:
                    if column_roles[(table_name, col_name)]['role'] == 'PK Comp':
                        column_roles[(table_name, col_name)]['role'] = 'PK/FK Comp'
                        column_roles[(table_name, col_name)]['importance_score'] += 2
                    else:
                         column_roles[(table_name, col_name)]['role'] = 'PK/FK'
                         column_roles[(table_name, col_name)]['importance_score'] += 4 # Alta importância base
                    junction_fk_details.append(f"{col_name} -> {ref_table}.{ref_cols[0] if ref_cols else 'N/A'}")
                else:
                    column_roles[(table_name, col_name)]['role'] = 'FK'
                    column_roles[(table_name, col_name)]['importance_score'] += 1 # Baixa importância base
                    try: ref_col_name = ref_cols[0] if ref_cols else 'N/A'
                    except IndexError: ref_col_name = 'N/A'
                    column_roles[(table_name, col_name)]['details'] = f"-> {ref_table}.{ref_col_name}"

            if pk_column_names.intersection(fk_cols):
                 is_junction_table = True

        if is_junction_table and pk_column_names and pk_column_names.issubset(fk_columns_in_table):
             junction_tables[table_name] = junction_fk_details
             for col_name in pk_column_names:
                  column_roles[(table_name, col_name)]['importance_score'] += 2

    # 3. Ajustar Score de Importância baseado na Contagem de Referências
    HIGH_REF_THRESHOLD = 50
    MEDIUM_REF_THRESHOLD = 10

    for (table_name, col_name), role_data in column_roles.items():
        full_col_name = f"{table_name}.{col_name}"
        ref_count = fk_ref_counts.get(full_col_name, 0)
        if ref_count >= HIGH_REF_THRESHOLD:
            role_data['importance_score'] += 3
        elif ref_count >= MEDIUM_REF_THRESHOLD:
            role_data['importance_score'] += 2
        elif ref_count > 0:
            role_data['importance_score'] += 1
        if role_data['role'] == 'PK' and ref_count >= HIGH_REF_THRESHOLD:
            role_data['importance_score'] += 3
        table_ref_count_approx = sum(fk_ref_counts.get(f"{table_name}.{c}", 0) for c in columns_in_table if f"{table_name}.{c}" in fk_ref_counts)
        if role_data['role'] == 'Normal' and table_ref_count_approx > HIGH_REF_THRESHOLD * 2:
             role_data['importance_score'] += 1
             
    # 4. Definir Nível de Importância (Texto)
    for role_data in column_roles.values():
        score = role_data['importance_score']
        if score >= 8:
            role_data['importance_level'] = 'Máxima'
        elif score >= 5:
            role_data['importance_level'] = 'Alta'
        elif score >= 2:
            role_data['importance_level'] = 'Média'
        else:
             role_data['importance_level'] = 'Baixa'

    logger.info(f"Análise estrutural concluída. PKs Comp: {len(composite_pk_tables)}, Junção: {len(junction_tables)}, FKs Comp: {len(composite_fk_details)}")
    return composite_pk_tables, junction_tables, composite_fk_details, dict(column_roles)

def generate_documentation_overview(technical_schema, metadata, overview_counts):
    """Gera DataFrame da visão geral, incluindo contagens/timestamps do cache."""
    logger.info("Gerando visão geral da documentação...")
    overview_data = []
    total_objects_processed = 0

    for name, tech_info in technical_schema.items():
        object_type = tech_info.get('object_type')
        if object_type not in ["TABLE", "VIEW"]:
             continue

        total_objects_processed += 1
        columns_tech = tech_info.get('columns', [])
        total_cols = len(columns_tech)
        
        key_type = object_type + "S" if object_type else None
        object_meta = metadata.get(key_type, {}).get(name, {})
        object_columns_meta = object_meta.get('COLUMNS', {})
        obj_desc_exists = bool(object_meta.get('description', '').strip())
        
        described_cols = 0
        noted_cols = 0
        if total_cols > 0:
            for col_def in columns_tech:
                col_name = col_def.get('name')
                if col_name:
                    col_meta = object_columns_meta.get(col_name, {})
                    if col_meta.get('description', '').strip(): described_cols += 1
                    if col_meta.get('value_mapping_notes', '').strip(): noted_cols += 1
            desc_perc = (described_cols / total_cols) * 100
            notes_perc = (noted_cols / total_cols) * 100
        else:
            desc_perc = 0; notes_perc = 0

        count_info = overview_counts.get(name, {})
        row_count_val = count_info.get("count", "N/A")
        timestamp_val = count_info.get("timestamp")

        row_count_display = row_count_val
        raw_count = np.nan
        if isinstance(row_count_val, int) and row_count_val >= 0:
             row_count_display = f"{row_count_val:,}".replace(",", ".")
             raw_count = row_count_val
        elif isinstance(row_count_val, str) and row_count_val.startswith("Erro"):
            row_count_display = "Erro"
        elif row_count_val == "N/A":
             row_count_display = "N/A"

        timestamp_display = "-"
        if timestamp_val:
            try:
                dt_obj = datetime.datetime.fromisoformat(timestamp_val)
                timestamp_display = dt_obj.strftime("%d/%m/%y %H:%M")
            except ValueError:
                 timestamp_display = "Inválido"

        overview_data.append({
            'Objeto': name,
            'Tipo': object_type,
            'Descrição?': "✅" if obj_desc_exists else "❌",
            'Total Colunas': total_cols,
            'Linhas (Cache)': row_count_display,
            'Contagem Em': timestamp_display,
            'Col. Descritas': described_cols,
            '% Descritas': f"{desc_perc:.1f}%",
            'Col. c/ Notas': noted_cols,
            '% c/ Notas': f"{notes_perc:.1f}%",
            '_Linhas_Raw': raw_count
        })

    df_overview = pd.DataFrame(overview_data)
    if not df_overview.empty:
        df_overview['_Linhas_Raw'] = pd.to_numeric(df_overview['_Linhas_Raw'], errors='coerce')
        cols_order = ['Objeto', 'Tipo', 'Descrição?', 'Total Colunas', 'Linhas (Cache)', 'Contagem Em',
                      'Col. Descritas', '% Descritas', 'Col. c/ Notas', '% c/ Notas']
        cols_order = [col for col in cols_order if col in df_overview.columns]
        df_overview = df_overview.sort_values(
            by=['_Linhas_Raw', 'Tipo', 'Objeto'], 
            ascending=[False, True, True],
            na_position='last'
        ).reset_index(drop=True)
        df_overview_display = df_overview[cols_order]
    else:
        cols_order = ['Objeto', 'Tipo', 'Descrição?', 'Total Colunas', 'Linhas (Cache)', 'Contagem Em',
                      'Col. Descritas', '% Descritas', 'Col. c/ Notas', '% c/ Notas']
        df_overview_display = pd.DataFrame(columns=cols_order)
        
    logger.info(f"Visão geral gerada. Shape: {df_overview_display.shape}")
    return df_overview_display 