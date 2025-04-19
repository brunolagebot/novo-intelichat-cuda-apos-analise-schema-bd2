import json
import os
import sys
import logging
from collections import defaultdict
from pathlib import Path

# Adiciona o diretório raiz ao sys.path para importações de módulos
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importações de configuração ou logging, se necessário
try:
    from src.core.log_utils import setup_logging
    setup_logging() # Configura o logger conforme definido no projeto
except ImportError:
    # Fallback para configuração básica de logging se o módulo não for encontrado
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    print("AVISO: src.core.log_utils.setup_logging não encontrado. Usando config básica.")

logger = logging.getLogger(__name__)

# --- Definição de Caminhos ---
# Ajuste os caminhos conforme a estrutura real do seu projeto
BASE_SCHEMA_PATH = Path("data/metadata/technical_schema_from_db.json")
AI_DESCRIPTIONS_PATH = Path("data/metadata/ai_generated_descriptions_openai_35turbo.json")
MANUAL_METADATA_PATH = Path("data/metadata/metadata_schema_manual.json")
OUTPUT_SCHEMA_PATH = Path("data/processed/merged_schema_for_embeddings.json")

# --- Funções Auxiliares ---

def load_json_file(file_path: Path, description: str):
    """Carrega um arquivo JSON com tratamento de erro."""
    if not file_path.exists():
        logger.error(f"Erro: Arquivo {description} não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON de {description} em '{file_path}': {e}")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar {description} de '{file_path}': {e}", exc_info=True)
        return None

def build_ai_lookup(ai_data):
    """Cria um dicionário de lookup para descrições de IA."""
    lookup = {}
    if not isinstance(ai_data, list):
        logger.warning("Dados de IA não estão no formato esperado (lista). Lookup ficará vazio.")
        return lookup
        
    for item in ai_data:
        if isinstance(item, dict) and 'object_name' in item and 'column_name' in item:
            table_name = item['object_name'].strip().upper() # Normaliza para maiúsculas
            column_name = item['column_name'].strip().upper() # Normaliza para maiúsculas
            key = (table_name, column_name)
            lookup[key] = {
                "generated_description": item.get("generated_description"),
                "model_used": item.get("model_used"),
                "generation_timestamp": item.get("generation_timestamp")
            }
        else:
            logger.warning(f"Item de descrição de IA ignorado por falta de chaves ou formato inválido: {item}")
    logger.info(f"Lookup de descrições de IA criado com {len(lookup)} entradas.")
    return lookup

def build_manual_lookup(manual_data):
    """Cria um dicionário de lookup para metadados manuais (apenas colunas)."""
    lookup = {}
    if not isinstance(manual_data, dict) or 'schema_objects' not in manual_data:
        logger.warning("Dados manuais não estão no formato esperado (dicionário com 'schema_objects'). Lookup ficará vazio.")
        return lookup

    schema_objects = manual_data.get('schema_objects', [])
    if not isinstance(schema_objects, list):
         logger.warning("Chave 'schema_objects' nos dados manuais não é uma lista. Lookup ficará vazio.")
         return lookup

    for item in schema_objects:
        if isinstance(item, dict) and 'name' in item and 'columns' in item:
            table_name = item['name'].strip().upper() # Normaliza para maiúsculas
            if isinstance(item['columns'], list):
                for col in item['columns']:
                    if isinstance(col, dict) and 'name' in col:
                        column_name = col['name'].strip().upper() # Normaliza para maiúsculas
                        key = (table_name, column_name)
                        lookup[key] = {
                            "business_description": col.get("business_description"),
                            "value_mapping_notes": col.get("value_mapping_notes")
                        }
                    else:
                         logger.warning(f"Coluna em '{table_name}' ignorada por falta de nome ou formato inválido: {col}")
            else:
                logger.warning(f"Chave 'columns' em '{table_name}' nos dados manuais não é uma lista. Colunas ignoradas.")
        else:
            logger.warning(f"Objeto de schema manual ignorado por falta de nome, colunas ou formato inválido: {item}")
            
    logger.info(f"Lookup de metadados manuais (colunas) criado com {len(lookup)} entradas.")
    return lookup

def save_json_file(data, file_path: Path, description: str):
    """Salva dados em um arquivo JSON com tratamento de erro."""
    try:
        # Garante que o diretório de saída exista
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"{description} salvo com sucesso em '{file_path}'")
        return True
    except Exception as e:
        logger.error(f"Erro ao salvar {description} em '{file_path}': {e}", exc_info=True)
        return False

# --- Função Principal ---

def main():
    logger.info("--- Iniciando Merge de Metadados para Embeddings ---")

    # 1. Carregar Arquivos
    logger.info(f"Carregando schema base de: {BASE_SCHEMA_PATH}")
    base_schema = load_json_file(BASE_SCHEMA_PATH, "Schema Base")
    if base_schema is None:
        logger.critical("Não foi possível carregar o schema base. Abortando.")
        sys.exit(1)
        
    # Copiar para não modificar o original carregado em memória (opcional, mas seguro)
    merged_schema = json.loads(json.dumps(base_schema)) # Deep copy via JSON serialization

    logger.info(f"Carregando descrições de IA de: {AI_DESCRIPTIONS_PATH}")
    ai_data = load_json_file(AI_DESCRIPTIONS_PATH, "Descrições IA")
    ai_lookup = build_ai_lookup(ai_data if ai_data else []) # Passa lista vazia se falhar

    logger.info(f"Carregando metadados manuais de: {MANUAL_METADATA_PATH}")
    manual_data = load_json_file(MANUAL_METADATA_PATH, "Metadados Manuais")
    manual_lookup = build_manual_lookup(manual_data if manual_data else {}) # Passa dict vazio se falhar

    # 2. Lógica de Merge
    logger.info("Iniciando processo de merge...")
    merge_count_ai = 0
    merge_count_manual = 0
    processed_columns = 0

    # Iterar sobre a estrutura do schema base (que é um dicionário)
    if isinstance(merged_schema, dict):
        for table_name_orig, table_data in merged_schema.items():
            table_name = table_name_orig.strip().upper() # Normaliza
            if isinstance(table_data, dict) and 'columns' in table_data and isinstance(table_data['columns'], list):
                for column_data in table_data['columns']:
                     processed_columns += 1
                     if isinstance(column_data, dict) and 'name' in column_data:
                         column_name = column_data['name'].strip().upper() # Normaliza
                         key = (table_name, column_name)

                         # Merge dados de IA
                         if key in ai_lookup:
                             ai_info = ai_lookup[key]
                             # Verifica se as chaves de destino existem antes de atribuir
                             if 'ai_generated_description' in column_data:
                                column_data['ai_generated_description'] = ai_info.get('generated_description')
                             if 'ai_model_used' in column_data:
                                column_data['ai_model_used'] = ai_info.get('model_used')
                             if 'ai_generation_timestamp' in column_data:
                                column_data['ai_generation_timestamp'] = ai_info.get('generation_timestamp')
                             merge_count_ai += 1
                             logger.debug(f"Dados IA mesclados para {table_name}.{column_name}")

                         # Merge dados manuais (sobrescreve se necessário)
                         if key in manual_lookup:
                             manual_info = manual_lookup[key]
                             # Verifica se as chaves de destino existem antes de atribuir
                             if 'business_description' in column_data:
                                column_data['business_description'] = manual_info.get('business_description')
                             if 'value_mapping_notes' in column_data:
                                column_data['value_mapping_notes'] = manual_info.get('value_mapping_notes')
                             merge_count_manual += 1
                             logger.debug(f"Dados Manuais mesclados para {table_name}.{column_name}")
                     else:
                         logger.warning(f"Coluna em '{table_name_orig}' ignorada no merge por falta de nome ou formato inválido: {column_data}")
            else:
                 logger.warning(f"Objeto '{table_name_orig}' ignorado no merge por não conter lista 'columns' válida.")
    else:
        logger.error("Schema base não é um dicionário. Merge não pode ser realizado.")
        sys.exit(1)
        
    logger.info(f"Merge concluído. Total de colunas processadas: {processed_columns}.")
    logger.info(f"Atualizações de IA realizadas: {merge_count_ai}")
    logger.info(f"Atualizações Manuais realizadas: {merge_count_manual}")

    # 3. Salvar Resultado
    logger.info(f"Salvando schema mesclado em: {OUTPUT_SCHEMA_PATH}")
    if not save_json_file(merged_schema, OUTPUT_SCHEMA_PATH, "Schema Mesclado"):
        logger.error("Falha ao salvar o schema mesclado.")
        sys.exit(1)

    logger.info("--- Merge de Metadados Concluído com Sucesso ---")

if __name__ == "__main__":
    main() 