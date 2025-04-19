import json
import os
import sys
import logging
from copy import deepcopy
import datetime # NOVO: Para timestamp

# Adiciona o diretório raiz ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.config import OUTPUT_COMBINED_FILE, TECHNICAL_SCHEMA_FILE, METADATA_FILE # Importar
from src.core.log_utils import setup_logging # Importar setup_logging

# Configurar logging
setup_logging() # Chamar a função de configuração
logger = logging.getLogger(__name__)

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
    """Mescla dados técnicos com metadados e realiza validação/contagem."""
    if not technical_data or not metadata:
        logger.error("Dados técnicos ou de metadados não carregados. Abortando a mesclagem.")
        # Retorna informações de validação e contagem vazias/erro
        return None, 0, 0, 'Error', [], {}

    combined_schema = deepcopy(technical_data)
    total_column_count = 0
    manual_metadata_column_count = 0 # NOVO contador
    missing_objects = []
    missing_columns = {}
    is_complete = True

    logger.info("Iniciando a mesclagem, contagem e validação dos dados do schema...")

    # --- Etapa 1: Mesclar metadados e contar colunas com metadados manuais --- #
    for object_name, tech_details in combined_schema.items():
        if object_name == 'fk_reference_counts': continue
        if not isinstance(tech_details, dict):
            logger.warning(f"Item inesperado no nível raiz do schema técnico: '{object_name}' (tipo: {type(tech_details)}). Pulando.")
            continue
            
        meta_entry = find_metadata_entry(metadata, object_name)
        tech_details["business_description"] = meta_entry.get("description", None) if meta_entry else None
        columns_in_object = tech_details.get("columns", [])
        meta_columns = meta_entry.get("COLUMNS", {}) if meta_entry else {}
        
        for tech_col in columns_in_object:
            total_column_count += 1
            col_name = tech_col.get("name")
            if not col_name: continue
            
            meta_col_entry = meta_columns.get(col_name)
            
            # Mescla descrição e notas
            business_desc = meta_col_entry.get("description", None) if meta_col_entry else None
            mapping_notes = meta_col_entry.get("value_mapping_notes", None) if meta_col_entry else None
            tech_col["business_description"] = business_desc
            tech_col["value_mapping_notes"] = mapping_notes
            
            # Verifica se tem metadado manual (descrição OU nota)
            has_manual_desc = bool((business_desc or "").strip())
            has_manual_notes = bool((mapping_notes or "").strip())
            if has_manual_desc or has_manual_notes:
                manual_metadata_column_count += 1

    # --- Etapa 2: Validar completude --- #
    logger.info("Validando completude do schema combinado...")
    for tech_obj_name, tech_obj_data in technical_data.items():
        if tech_obj_name == 'fk_reference_counts': continue # Ignora chave interna
        if not isinstance(tech_obj_data, dict):
            logger.warning(f"Item inesperado encontrado durante validação no schema técnico: '{tech_obj_name}'.")
            continue

        if tech_obj_name not in combined_schema:
            logger.error(f"VALIDATION ERROR: Objeto técnico '{tech_obj_name}' está faltando no schema combinado!")
            missing_objects.append(tech_obj_name)
            is_complete = False
        else:
            combined_obj_data = combined_schema[tech_obj_name]
            technical_columns = {col.get('name') for col in tech_obj_data.get('columns', []) if col.get('name')} # Set de nomes
            combined_columns = {col.get('name') for col in combined_obj_data.get('columns', []) if col.get('name')} # Set de nomes
            
            missing_in_combined = technical_columns - combined_columns
            if missing_in_combined:
                logger.error(f"VALIDATION ERROR: Colunas técnicas faltando em '{tech_obj_name}' no schema combinado: {missing_in_combined}")
                missing_columns[tech_obj_name] = sorted(list(missing_in_combined))
                is_complete = False
                
            # Opcional: Verificar colunas extras no combinado (geralmente não deveria acontecer)
            extra_in_combined = combined_columns - technical_columns
            if extra_in_combined:
                 logger.warning(f"VALIDATION WARNING: Colunas extras encontradas em '{tech_obj_name}' no schema combinado (não presentes no técnico): {extra_in_combined}")

    validation_status = 'OK' if is_complete else 'Incomplete'
    logger.info(f"Validação concluída. Status: {validation_status}")
    if not is_complete:
        logger.warning(f"  Objetos faltando: {missing_objects}")
        logger.warning(f"  Colunas faltando: {missing_columns}")
        
    # Calcula colunas sem metadados manuais
    missing_manual_metadata_count = total_column_count - manual_metadata_column_count

    logger.info(f"Contagem: Total={total_column_count}, Com Manual={manual_metadata_column_count}, Sem Manual={missing_manual_metadata_count}")
    
    # Retorna todos os resultados
    return combined_schema, total_column_count, manual_metadata_column_count, missing_manual_metadata_count, validation_status, missing_objects, missing_columns

def save_combined_data(schema_data, filename, validation_info):
    """Salva o schema combinado em um arquivo JSON, incluindo informações de validação e contagem."""
    if not schema_data:
        logger.error("Nenhum dado combinado para salvar.")
        return
        
    schema_data['_metadata_info'] = validation_info
    
    logger.info(f"Salvando schema combinado (com info de validação/contagem) em {filename}...")
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, indent=4, ensure_ascii=False)
        logger.info("Schema combinado salvo com sucesso.")
    except IOError as e:
        logger.error(f"Erro ao salvar o arquivo JSON combinado: {e}")
    except Exception as e:
         logger.exception("Erro inesperado ao salvar o JSON combinado:")

def main():
    # Ler os arquivos JSON
    logger.info(f"Lendo esquema técnico de {TECHNICAL_SCHEMA_FILE}")
    try:
        with open(TECHNICAL_SCHEMA_FILE, 'r', encoding='utf-8') as f:
            technical_data = json.load(f)
    except FileNotFoundError:
        logger.error(f"Erro: Arquivo {TECHNICAL_SCHEMA_FILE} não encontrado.")
        return
    except json.JSONDecodeError:
        logger.error(f"Erro: Falha ao decodificar JSON em {TECHNICAL_SCHEMA_FILE}.")
        return

    logger.info(f"Lendo metadados manuais de {METADATA_FILE}")
    try:
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    except FileNotFoundError:
        logger.warning(f"Aviso: Arquivo {METADATA_FILE} não encontrado. Continuando sem metadados manuais.")
        metadata = {}
    except json.JSONDecodeError:
        logger.error(f"Erro: Falha ao decodificar JSON em {METADATA_FILE}.")
        # Decidir se deve retornar ou continuar sem metadados
        metadata = {}

    # Mesclar os dados
    logger.info("Mesclando dados técnicos e metadados...")
    combined_data, total_cols, manual_cols, missing_manual_cols, status, missing_obj, missing_col = merge_schema_data(technical_data, metadata)

    # Salvar o resultado combinado
    logger.info(f"Salvando esquema combinado em {OUTPUT_COMBINED_FILE}")
    try:
        # Mantém as contagens do arquivo técnico original, se existirem
        if 'fk_reference_counts' in technical_data:
            combined_data['fk_reference_counts'] = technical_data['fk_reference_counts']
            logger.info("Chave 'fk_reference_counts' preservada do schema técnico.")
        else:
            logger.warning("Chave 'fk_reference_counts' não encontrada no schema técnico para preservar.")
        
        # Prepara informações de validação e contagem para salvar
        validation_info = {
            'total_column_count': total_cols,
            'manual_metadata_column_count': manual_cols, # NOVO
            'missing_manual_metadata_column_count': missing_manual_cols, # NOVO
            'validation_status': status,
            'validation_timestamp': datetime.datetime.now().isoformat(),
            'missing_objects': missing_obj,
            'missing_columns': missing_col
        }
        
        save_combined_data(combined_data, OUTPUT_COMBINED_FILE, validation_info)
    except IOError as e:
        logger.error(f"Erro ao salvar o arquivo {OUTPUT_COMBINED_FILE}: {e}")

if __name__ == "__main__":
    main()

    print(f"\nProcesso de mesclagem e validação concluído. Resultado em {OUTPUT_COMBINED_FILE}") 