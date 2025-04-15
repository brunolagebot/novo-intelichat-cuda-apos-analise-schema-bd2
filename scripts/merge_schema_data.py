import json
import os
import logging
from copy import deepcopy

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

TECHNICAL_SCHEMA_FILE = 'data/technical_schema_details.json'
METADATA_FILE = 'etapas-sem-gpu/schema_metadata.json' # Usando o arquivo fornecido
OUTPUT_COMBINED_FILE = 'data/combined_schema_details.json'

def load_json_safe(filename):
    """Carrega um arquivo JSON com tratamento de erros."""
    if not os.path.exists(filename):
        logger.error(f"Arquivo não encontrado: {filename}")
        return None
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON do arquivo {filename}: {e}")
        return None
    except IOError as e:
        logger.error(f"Erro ao ler o arquivo {filename}: {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar {filename}: {e}", exc_info=True)
        return None

def find_metadata_entry(metadata, object_name):
    """Encontra a entrada de metadados para uma tabela/view."""
    # Verifica em TABLES, VIEWS e DESCONHECIDOS
    for category in ["TABLES", "VIEWS", "DESCONHECIDOS"]:
        if object_name in metadata.get(category, {}):
            return metadata[category][object_name]
    return None

def merge_schema_data(technical_data, metadata):
    """Mescla dados técnicos com metadados (descrições, notas)."""
    if not technical_data or not metadata:
        logger.error("Dados técnicos ou de metadados não carregados. Abortando a mesclagem.")
        return None

    combined_schema = deepcopy(technical_data) # Começa com cópia profunda dos dados técnicos
    objects_without_metadata = []
    columns_without_metadata = []

    logger.info("Iniciando a mesclagem dos dados do schema...")

    for object_name, tech_details in combined_schema.items():
        meta_entry = find_metadata_entry(metadata, object_name)

        if not meta_entry:
            logger.warning(f"Metadados não encontrados para o objeto: {object_name}")
            objects_without_metadata.append(object_name)
            # Garante que os campos existam mesmo vazios (opcional, mas bom para consistência)
            tech_details["business_description"] = None 
            for col in tech_details.get("columns", []):
                 col["business_description"] = None
                 col["value_mapping_notes"] = None
            continue # Pula para o próximo objeto

        # Adiciona descrição do objeto (tabela/view)
        tech_details["business_description"] = meta_entry.get("description", None)
        
        # Itera sobre as colunas técnicas para adicionar metadados das colunas
        meta_columns = meta_entry.get("COLUMNS", {}) # Note a chave 'COLUMNS' no metadata.json
        for tech_col in tech_details.get("columns", []):
            col_name = tech_col["name"] # Nome da coluna técnica
            meta_col_entry = meta_columns.get(col_name)

            if not meta_col_entry:
                logger.warning(f"Metadados não encontrados para a coluna: {object_name}.{col_name}")
                columns_without_metadata.append(f"{object_name}.{col_name}")
                # Garante que os campos existam mesmo vazios
                tech_col["business_description"] = None
                tech_col["value_mapping_notes"] = None
            else:
                # Adiciona descrição e notas da coluna
                tech_col["business_description"] = meta_col_entry.get("description", None)
                tech_col["value_mapping_notes"] = meta_col_entry.get("value_mapping_notes", None)

    logger.info(f"Mesclagem concluída. {len(objects_without_metadata)} objetos sem metadados e {len(columns_without_metadata)} colunas sem metadados.")
    if objects_without_metadata:
        logger.debug(f"Objetos sem metadados: {objects_without_metadata}")
    if columns_without_metadata:
         logger.debug(f"Colunas sem metadados: {columns_without_metadata}")
         
    return combined_schema

def save_combined_data(schema_data, filename):
    """Salva o schema combinado em um arquivo JSON."""
    if not schema_data:
        logger.error("Nenhum dado combinado para salvar.")
        return
    logger.info(f"Salvando schema combinado em {filename}...")
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, indent=4, ensure_ascii=False)
        logger.info("Schema combinado salvo com sucesso.")
    except IOError as e:
        logger.error(f"Erro ao salvar o arquivo JSON combinado: {e}")
    except Exception as e:
         logger.exception("Erro inesperado ao salvar o JSON combinado:")

# --- Execução Principal ---
if __name__ == "__main__":
    logger.info(f"Carregando schema técnico de: {TECHNICAL_SCHEMA_FILE}")
    technical_data = load_json_safe(TECHNICAL_SCHEMA_FILE)

    logger.info(f"Carregando metadados de: {METADATA_FILE}")
    metadata = load_json_safe(METADATA_FILE)

    if technical_data and metadata:
        combined_schema = merge_schema_data(technical_data, metadata)
        if combined_schema:
             # Mantém as contagens do arquivo técnico original, se existirem
             if 'fk_reference_counts' in technical_data:
                 combined_schema['fk_reference_counts'] = technical_data['fk_reference_counts']
                 logger.info("Chave 'fk_reference_counts' preservada do schema técnico.")
             else:
                 logger.warning("Chave 'fk_reference_counts' não encontrada no schema técnico para preservar.")
             save_combined_data(combined_schema, OUTPUT_COMBINED_FILE)
    else:
        logger.error("Falha ao carregar um ou ambos os arquivos de schema. Mesclagem abortada.")

    print(f"\nProcesso de mesclagem concluído. Resultado em {OUTPUT_COMBINED_FILE}") 