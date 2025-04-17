import pandas as pd
from tqdm import tqdm
import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)

def generate_schema_dataframe(schema_data):
    """
    Converte os dados do schema (do combined_schema_details.json) em um DataFrame Pandas achatado.

    Args:
        schema_data (dict): O dicionário carregado do arquivo JSON do schema combinado.

    Returns:
        pd.DataFrame: Um DataFrame onde cada linha representa uma coluna de um objeto (tabela/view),
                      contendo informações técnicas e de metadados.
                      Retorna um DataFrame vazio se a entrada for inválida ou vazia.
    """
    all_rows = []
    if not schema_data or not isinstance(schema_data, dict):
        logger.warning("Dados de schema inválidos ou vazios fornecidos.")
        return pd.DataFrame()

    # Itera sobre os objetos (tabelas/views) com barra de progresso
    # Usamos list(schema_data.items()) para poder usar tqdm com dicionários
    total_objects = len([k for k in schema_data if k not in ['_metadata_info', 'fk_reference_counts']])
    
    with tqdm(total=total_objects, desc="Processing Objects", unit="obj") as pbar:
        for object_name, object_data in schema_data.items():
            # Pula chaves internas
            if object_name in ['_metadata_info', 'fk_reference_counts']:
                continue
            # Garante que é um dicionário válido de objeto
            if not isinstance(object_data, dict):
                logger.warning(f"Item inesperado no schema: '{object_name}' não é um dicionário. Pulando.")
                pbar.update(1) # Atualiza mesmo se pular
                continue

            object_type = object_data.get('object_type', 'Desconhecido')
            object_description = object_data.get('business_description')
            constraints = object_data.get('constraints', {})
            pk_cols = set()
            fk_details = {} # {col_name: "references_table.references_column"}

            # Processa constraints para fácil lookup
            for pk in constraints.get('primary_key', []):
                pk_cols.update(pk.get('columns', []))
            for fk in constraints.get('foreign_keys', []):
                ref_table = fk.get('references_table', '?')
                ref_cols = fk.get('references_columns', [])
                local_cols = fk.get('columns', [])
                for i, local_col in enumerate(local_cols):
                    ref_col = ref_cols[i] if i < len(ref_cols) else '?'
                    fk_details[local_col] = f"{ref_table}.{ref_col}"

            columns = object_data.get('columns', [])
            if not columns:
                # Adiciona uma linha para o objeto mesmo sem colunas? Ou ignora?
                # Por ora, vamos ignorar objetos sem colunas no DF achatado.
                logger.debug(f"Objeto '{object_name}' não possui colunas. Pulando no DataFrame.")
                pbar.update(1)
                continue

            for col_data in columns:
                if not isinstance(col_data, dict):
                    logger.warning(f"Item de coluna inválido em '{object_name}'. Pulando coluna.")
                    continue
                
                col_name = col_data.get('name')
                if not col_name:
                    logger.warning(f"Coluna sem nome encontrada em '{object_name}'. Pulando coluna.")
                    continue

                row = OrderedDict()
                row['ObjectName'] = object_name
                row['ObjectType'] = object_type
                row['ColumnName'] = col_name
                row['ColumnType'] = col_data.get('type')
                row['Length'] = col_data.get('length')
                row['Precision'] = col_data.get('precision')
                row['Scale'] = col_data.get('scale')
                row['Nullable'] = col_data.get('nullable')
                row['IsPrimaryKey'] = col_name in pk_cols
                row['IsForeignKey'] = col_name in fk_details
                row['ForeignKeyReference'] = fk_details.get(col_name)
                row['ObjectDescription'] = object_description
                row['ColumnDescription'] = col_data.get('business_description')
                row['MappingNotes'] = col_data.get('value_mapping_notes')
                # Adicionar Sample Data seria complexo em um DF achatado. Poderíamos incluir?
                # row['SampleValue'] = col_data.get('sample_data_for_column') # Exigiria pré-processamento
                
                all_rows.append(row)
            
            pbar.update(1) # Atualiza a barra de progresso para cada objeto processado

    if not all_rows:
        logger.warning("Nenhuma linha de coluna válida gerada para o DataFrame.")
        return pd.DataFrame()

    logger.info(f"DataFrame gerado com {len(all_rows)} linhas (colunas).")
    return pd.DataFrame(all_rows) 