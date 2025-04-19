import fdb
import json
import logging
import os
import sys
from pathlib import Path
from collections import OrderedDict
from dotenv import load_dotenv

# --- Configuração ---
load_dotenv()
# Assuming setup_logging is defined elsewhere or configure basic logging
try:
    from src.core.logging_config import setup_logging
    setup_logging() # Use your project's logging setup
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    print("AVISO: src.core.logging_config.setup_logging não encontrado. Usando config básica.")

logger = logging.getLogger(__name__)

ORIGINAL_METADATA_PATH = Path("data/metadata/schema_metadata.json")
NEW_SCHEMA_PATH = Path("data/metadata/generated_schema_structure.json")
DEFAULT_STRING = ""
DEFAULT_BOOL = False
DEFAULT_FK_REFS = None # Will store dict like {"references_table": T, "references_column": C}

# --- Conexão Firebird (adaptado de test_firebird_connection.py) ---
FIREBIRD_HOST = os.getenv("FIREBIRD_HOST", "localhost")
FIREBIRD_PORT = int(os.getenv("FIREBIRD_PORT", "3050"))
FIREBIRD_DB_PATH = os.getenv("FIREBIRD_DB_PATH")
FIREBIRD_USER = os.getenv("FIREBIRD_USER", "SYSDBA")
FIREBIRD_PASSWORD = os.getenv("FIREBIRD_PASSWORD")
FIREBIRD_CHARSET = os.getenv("FIREBIRD_CHARSET", "WIN1252") # Ajuste se necessário

# --- Mapeamento de Tipos de Dados Firebird (Simplificado - pode precisar de ajustes) ---
# Baseado em: http://www.firebirdsql.org/file/documentation/reference_manuals/fblangref25/fblangref25-appx-datatypes.html
# E RDB$FIELD_TYPES: https://firebirdsql.org/refdocs/fblangref25-appx01-fieldtypes.html
FIREBIRD_TYPE_MAP = {
    7: "SMALLINT",
    8: "INTEGER",
    10: "FLOAT",
    11: "D_FLOAT", # Firebird 1.5+ specific? Treat as DOUBLE PRECISION
    12: "DATE",
    13: "TIME",
    14: "CHAR",
    16: "BIGINT", # (Dialect 3)
    27: "DOUBLE PRECISION",
    35: "TIMESTAMP",
    37: "VARCHAR",
    40: "CSTRING", # Obsoleto, tratar como VARCHAR?
    45: "BLOB_ID", # Não é um tipo de dado direto
    261: "BLOB",
    # Tipos Decimais/Numéricos (usar SUB_TYPE para precisão/escala se necessário)
    # SUB_TYPE 1: NUMERIC, SUB_TYPE 2: DECIMAL
    # Se type=7/8/16 e sub_type=1 or 2 -> DECIMAL/NUMERIC
    # Considerar lógica adicional aqui se necessário. Por enquanto, simplificado.
}

def get_firebird_type_name(field_type, sub_type, field_length=None, scale=None):
    """Mapeia tipo numérico do Firebird para nome textual."""
    # Handle NUMERIC/DECIMAL stored in INT/BIGINT types
    if field_type in [7, 8, 16] and sub_type in [1, 2]:
        precision = field_length # Approximated, might need better way
        scale_str = f", {abs(scale)}" if scale is not None else ""
        type_name = "NUMERIC" if sub_type == 1 else "DECIMAL"
        # Precision/Scale not directly available for these types easily from RDB$FIELDS
        # May need RDB$TYPES or parse from field definition. Simplification:
        return f"{type_name}" # return f"{type_name}({precision}{scale_str})" if precision else type_name

    base_type = FIREBIRD_TYPE_MAP.get(field_type, f"UNKNOWN({field_type})")

    if base_type in ["CHAR", "VARCHAR", "CSTRING"] and field_length is not None:
        return f"{base_type}({field_length})"
    elif base_type == "BLOB":
        # Subtipos de BLOB: 0=Segmented, 1=Text, 2=BLR, ...
        blob_subtype_name = {0: "BINARY", 1: "TEXT"}.get(sub_type, f"SUBTYPE_{sub_type}")
        return f"BLOB {blob_subtype_name}"
    return base_type


def load_original_descriptions(file_path):
    """Carrega descrições do JSON original para um lookup rápido."""
    descriptions = {}
    if not file_path.exists():
        logger.warning(f"Arquivo de metadados original não encontrado em {file_path}. Nenhuma descrição será mesclada.")
        return descriptions
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        for top_level_key in ["TABLES", "VIEWS"]:
            if top_level_key in metadata and isinstance(metadata[top_level_key], dict):
                for item_name, item_data in metadata[top_level_key].items():
                    if isinstance(item_data, dict):
                        # Descrições a nível de Tabela/View
                        tbl_key = (item_name.strip().upper(), None) # Usa None para coluna e upper para case-insensitivity
                        descriptions[tbl_key] = {
                            "business": item_data.get("business_description", DEFAULT_STRING),
                            "source": item_data.get("source_description", DEFAULT_STRING),
                            "value": DEFAULT_STRING # Não existe no nível da tabela original
                        }
                        # Descrições a nível de Coluna
                        if "COLUMNS" in item_data and isinstance(item_data["COLUMNS"], dict):
                            for col_name, col_data in item_data["COLUMNS"].items():
                                if isinstance(col_data, dict):
                                     col_key = (item_name.strip().upper(), col_name.strip().upper()) # Upper para case-insensitivity
                                     descriptions[col_key] = {
                                         "business": col_data.get("business_description", DEFAULT_STRING),
                                         "source": col_data.get("source_description", DEFAULT_STRING),
                                         "value": col_data.get("value_mapping_notes", DEFAULT_STRING)
                                     }
        logger.info(f"Descrições carregadas do arquivo original: {len(descriptions)} entradas.")
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON de {file_path}: {e}. Nenhuma descrição será mesclada.")
    except Exception as e:
        logger.error(f"Erro ao ler ou processar {file_path}: {e}. Nenhuma descrição será mesclada.")
    return descriptions

def fetch_firebird_schema(conn):
    """Busca metadados estruturais do Firebird."""
    schema = OrderedDict() # Use OrderedDict para manter a ordem das tabelas/views
    cursor = conn.cursor()
    logger.info("Buscando tabelas e views do Firebird...")

    # 1. Fetch Tables and Views (Sorted)
    try:
        cursor.execute("""
            SELECT TRIM(RDB$RELATION_NAME), RDB$RELATION_TYPE, RDB$VIEW_BLR
            FROM RDB$RELATIONS
            WHERE RDB$SYSTEM_FLAG = 0
            ORDER BY RDB$RELATION_NAME;
        """)
        for row in cursor.fetchall():
            name = row[0].strip().upper() # Padroniza para maiúsculas
            rel_type = 'VIEW' if row[2] is not None else 'TABLE'
            schema[name] = {'type': rel_type, 'columns': OrderedDict(), 'pk_columns': set(), 'fk_constraints': {}} # fk_constraints[col] = {refs_table, refs_col}

        logger.info(f"Encontradas {len(schema)} tabelas/views.")

        # 2. Fetch Columns (Sorted by Position)
        logger.info("Buscando colunas...")
        cursor.execute("""
            SELECT
                TRIM(rf.RDB$RELATION_NAME),
                TRIM(rf.RDB$FIELD_NAME),
                rf.RDB$FIELD_POSITION,
                f.RDB$FIELD_TYPE,
                f.RDB$FIELD_SUB_TYPE,
                f.RDB$FIELD_LENGTH,
                f.RDB$FIELD_PRECISION, -- Usually 0 for non-numeric?
                f.RDB$FIELD_SCALE,
                rf.RDB$NULL_FLAG -- 1 if NOT NULL, NULL or 0 if NULLABLE
            FROM RDB$RELATION_FIELDS rf
            JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
            JOIN RDB$RELATIONS r ON rf.RDB$RELATION_NAME = r.RDB$RELATION_NAME -- Ensure relation is user-defined
            WHERE rf.RDB$SYSTEM_FLAG = 0 AND r.RDB$SYSTEM_FLAG = 0
            ORDER BY rf.RDB$RELATION_NAME, rf.RDB$FIELD_POSITION;
        """)
        for row in cursor.fetchall():
            rel_name, col_name, _, f_type, f_subtype, f_len, f_prec, f_scale, null_flag = row
            rel_name = rel_name.strip().upper() # Padroniza para maiúsculas
            col_name = col_name.strip().upper() # Padroniza para maiúsculas

            if rel_name in schema:
                col_type_name = get_firebird_type_name(f_type, f_subtype, f_len, f_scale)
                is_not_null = null_flag == 1
                schema[rel_name]['columns'][col_name] = {
                    'type': col_type_name,
                    'is_not_null': is_not_null,
                    'is_pk': False, # Default, update later
                    'is_fk': False, # Default, update later
                    'fk_references': None # Default, update later
                }
        logger.info("Colunas buscadas.")

        # 3. Fetch Primary Keys
        logger.info("Buscando chaves primárias...")
        cursor.execute("""
            SELECT TRIM(idx.RDB$RELATION_NAME), TRIM(seg.RDB$FIELD_NAME)
            FROM RDB$RELATION_CONSTRAINTS rc
            JOIN RDB$INDICES idx ON rc.RDB$INDEX_NAME = idx.RDB$INDEX_NAME
            JOIN RDB$INDEX_SEGMENTS seg ON idx.RDB$INDEX_NAME = seg.RDB$INDEX_NAME
            WHERE rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY' AND idx.RDB$RELATION_NAME IS NOT NULL
            AND idx.RDB$RELATION_NAME IN (SELECT r.RDB$RELATION_NAME FROM RDB$RELATIONS r WHERE r.RDB$SYSTEM_FLAG=0) -- Ensure it's a user table index
        """)
        for row in cursor.fetchall():
            rel_name, col_name = row[0].strip().upper(), row[1].strip().upper() # Padroniza para maiúsculas
            if rel_name in schema and col_name in schema[rel_name]['columns']:
                schema[rel_name]['columns'][col_name]['is_pk'] = True
                schema[rel_name]['pk_columns'].add(col_name)
        logger.info("Chaves primárias buscadas.")

        # 4. Fetch Foreign Keys (More complex: needs referenced table/column)
        logger.info("Buscando chaves estrangeiras...")
        # This query attempts to correctly map composite FKs by matching segment positions
        cursor.execute("""
            SELECT
                TRIM(rc.RDB$RELATION_NAME) AS FK_TABLE,
                TRIM(fk_seg.RDB$FIELD_NAME) AS FK_COLUMN,
                TRIM(pk_rc.RDB$RELATION_NAME) AS PK_TABLE,
                TRIM(pk_seg.RDB$FIELD_NAME) AS PK_COLUMN
            FROM RDB$RELATION_CONSTRAINTS rc
            JOIN RDB$INDICES fk_idx ON rc.RDB$INDEX_NAME = fk_idx.RDB$INDEX_NAME
            JOIN RDB$INDEX_SEGMENTS fk_seg ON fk_idx.RDB$INDEX_NAME = fk_seg.RDB$INDEX_NAME
            JOIN RDB$REF_CONSTRAINTS refc ON rc.RDB$CONSTRAINT_NAME = refc.RDB$CONSTRAINT_NAME
            JOIN RDB$RELATION_CONSTRAINTS pk_rc ON refc.RDB$CONST_NAME_UQ = pk_rc.RDB$CONSTRAINT_NAME
            JOIN RDB$INDICES pk_idx ON pk_rc.RDB$INDEX_NAME = pk_idx.RDB$INDEX_NAME
            JOIN RDB$INDEX_SEGMENTS pk_seg ON pk_idx.RDB$INDEX_NAME = pk_seg.RDB$INDEX_NAME
            WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY'
              AND rc.RDB$RELATION_NAME IN (SELECT r.RDB$RELATION_NAME FROM RDB$RELATIONS r WHERE r.RDB$SYSTEM_FLAG=0) -- Ensure it's a user FK constraint
              AND fk_seg.RDB$FIELD_POSITION = pk_seg.RDB$FIELD_POSITION -- Match segment positions for composite keys
        """)
        for row in cursor.fetchall():
            fk_table, fk_col, pk_table, pk_col = [c.strip().upper() for c in row] # Padroniza para maiúsculas
            if fk_table in schema and fk_col in schema[fk_table]['columns']:
                 schema[fk_table]['columns'][fk_col]['is_fk'] = True
                 # Store only the first reference found for a column if multiple FKs use it (simplification)
                 if schema[fk_table]['columns'][fk_col]['fk_references'] is None:
                     schema[fk_table]['columns'][fk_col]['fk_references'] = {
                         "references_table": pk_table,
                         "references_column": pk_col
                     }
                 else:
                    logger.debug(f"Coluna {fk_table}.{fk_col} já tem referência FK. Ignorando referência adicional para {pk_table}.{pk_col}.")

        logger.info("Chaves estrangeiras buscadas.")


    except fdb.Error as e:
        logger.error(f"Erro ao buscar metadados do Firebird: {e}", exc_info=True)
        raise # Re-raise para interromper a execução
    finally:
        if cursor:
            cursor.close()

    return schema


def main():
    """Função principal para extrair schema, mesclar descrições e salvar."""
    logger.info("--- Iniciando Extração e Geração do Novo Schema Firebird ---")

    if not all([FIREBIRD_DB_PATH, FIREBIRD_USER, FIREBIRD_PASSWORD]):
        logger.error("Erro: Configurações essenciais do Firebird (DB_PATH, USER, PASSWORD) não encontradas no ambiente/'.env'. Abortando.")
        sys.exit(1)

    # 1. Carregar Descrições Antigas (Case-insensitive keys)
    original_descriptions = load_original_descriptions(ORIGINAL_METADATA_PATH)

    # 2. Conectar e Buscar Schema do Banco
    conn = None
    db_schema_data = None
    try:
        logger.info(f"Conectando ao Firebird: {FIREBIRD_USER}@{FIREBIRD_HOST}:{FIREBIRD_PORT}/{FIREBIRD_DB_PATH}...")
        conn = fdb.connect(
            host=FIREBIRD_HOST, port=FIREBIRD_PORT, database=FIREBIRD_DB_PATH,
            user=FIREBIRD_USER, password=FIREBIRD_PASSWORD, charset=FIREBIRD_CHARSET
        )
        logger.info("Conexão estabelecida.")
        db_schema_data = fetch_firebird_schema(conn)
    except fdb.Error as e:
        logger.error(f"Falha na conexão ou busca no Firebird: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro inesperado durante a interação com o banco: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if conn:
            conn.close()
            logger.info("Conexão com Firebird fechada.")

    if not db_schema_data:
        logger.error("Nenhum dado de schema foi obtido do banco. Abortando.")
        sys.exit(1)

    # 3. Construir a Estrutura Final JSON
    final_schema = OrderedDict()

    # Processar _GLOBAL_CONTEXT do arquivo original (se existir)
    global_context_data = OrderedDict() # Default to empty ordered dict
    if ORIGINAL_METADATA_PATH.exists():
        try:
            with open(ORIGINAL_METADATA_PATH, 'r', encoding='utf-8') as f:
                original_json = json.load(f)
            global_context_content = original_json.get("_GLOBAL_CONTEXT")
            if isinstance(global_context_content, dict):
                 global_context_data = OrderedDict(sorted(global_context_content.items()))
                 logger.info("_GLOBAL_CONTEXT carregado e ordenado do arquivo original.")
            elif global_context_content is not None:
                 logger.warning(f"_GLOBAL_CONTEXT encontrado no original, mas não é um dicionário (tipo: {type(global_context_content)}). Será um dicionário vazio.")
            else:
                 logger.info("_GLOBAL_CONTEXT não encontrado no arquivo original.")
        except Exception as e:
            logger.error(f"Erro ao processar _GLOBAL_CONTEXT do arquivo original: {e}. Será um dicionário vazio.")
    else:
         logger.warning(f"Arquivo original {ORIGINAL_METADATA_PATH} não encontrado para buscar _GLOBAL_CONTEXT.")

    final_schema["_GLOBAL_CONTEXT"] = global_context_data
    final_schema["schema_objects"] = []

    logger.info("Iniciando merge das descrições com a estrutura do banco...")
    for item_name, item_data in db_schema_data.items(): # item_name is already UPPER
        # Buscar descrições da tabela/view no arquivo original (Case-insensitive key)
        tbl_key = (item_name, None)
        tbl_desc = original_descriptions.get(tbl_key, {})

        new_item_object = OrderedDict([
            ("name", item_name), # Already UPPER from fetch
            ("type", item_data['type']),
            ("business_description", tbl_desc.get("business", DEFAULT_STRING)),
            ("value_mapping_notes", tbl_desc.get("value", DEFAULT_STRING)), # Mapeado aqui
            ("source_description", tbl_desc.get("source", DEFAULT_STRING)),
            ("text_for_embedding", DEFAULT_STRING), # Campo novo
            ("columns", [])
        ])

        for col_name, col_data in item_data['columns'].items(): # col_name is already UPPER
            # Buscar descrições da coluna no arquivo original (Case-insensitive key)
            col_key = (item_name, col_name)
            col_desc = original_descriptions.get(col_key, {})

            new_column_object = OrderedDict([
                ("name", col_name), # Already UPPER from fetch
                ("type", col_data['type']),
                ("is_pk", col_data['is_pk']),
                ("is_fk", col_data['is_fk']),
                ("fk_references", col_data['fk_references']), # Já formatado como dict ou None
                ("business_description", col_desc.get("business", DEFAULT_STRING)),
                ("value_mapping_notes", col_desc.get("value", DEFAULT_STRING)), # Mapeado aqui
                ("source_description", col_desc.get("source", DEFAULT_STRING)),
                ("text_for_embedding", DEFAULT_STRING) # Campo novo
                # Adicionar 'is_not_null' se desejado: ("is_not_null", col_data['is_not_null']),
            ])
            new_item_object["columns"].append(new_column_object)

        final_schema["schema_objects"].append(new_item_object)

    logger.info(f"Merge concluído. Total de {len(final_schema['schema_objects'])} objetos no schema final.")

    # 4. Salvar o Novo Schema
    try:
        logger.info(f"Salvando novo schema em {NEW_SCHEMA_PATH}...")
        os.makedirs(NEW_SCHEMA_PATH.parent, exist_ok=True)
        with open(NEW_SCHEMA_PATH, 'w', encoding='utf-8') as f:
            json.dump(final_schema, f, ensure_ascii=False, indent=4)
        logger.info("Novo schema estruturado salvo com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar o novo schema em {NEW_SCHEMA_PATH}: {e}", exc_info=True)
        sys.exit(1)

    logger.info("--- Geração do Novo Schema Firebird Concluída ---")

if __name__ == "__main__":
    main() 