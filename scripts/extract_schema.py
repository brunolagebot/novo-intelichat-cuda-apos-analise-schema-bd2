import fdb # ADICIONADO: Import da biblioteca Firebird
import logging
import json
import sys
import os
from dotenv import load_dotenv
import getpass # Para senha, se não estiver no .env
from collections import defaultdict

# Carregar variáveis de ambiente
load_dotenv()

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configurações (Lidas do Ambiente ou Padrão) ---
FIREBIRD_HOST = os.getenv("FIREBIRD_HOST", "localhost")
FIREBIRD_PORT = int(os.getenv("FIREBIRD_PORT", "3050"))
FIREBIRD_DB_PATH = os.getenv("FIREBIRD_DB_PATH")
FIREBIRD_USER = os.getenv("FIREBIRD_USER", "SYSDBA")
FIREBIRD_PASSWORD = os.getenv("FIREBIRD_PASSWORD") # Tenta ler do .env primeiro
FIREBIRD_CHARSET = os.getenv("FIREBIRD_CHARSET", "WIN1252") # Usar o que funcionou

OUTPUT_JSON_FILE = 'data/technical_schema_details.json' # Arquivo de saída com detalhes técnicos

# --- Funções de Extração (Adaptadas de util-extract_firebird_schema.py) ---

def get_column_details(cur, relation_name):
    """Busca detalhes das colunas para uma dada tabela/view."""
    # ... (Lógica de get_column_details do util-extract_firebird_schema.py) ...
    sql = """
        SELECT
            TRIM(rf.RDB$FIELD_NAME) AS FIELD_NAME,
            f.RDB$FIELD_TYPE AS FIELD_TYPE,
            f.RDB$FIELD_SUB_TYPE AS FIELD_SUB_TYPE,
            f.RDB$FIELD_LENGTH AS FIELD_LENGTH,
            f.RDB$FIELD_PRECISION AS FIELD_PRECISION,
            f.RDB$FIELD_SCALE AS FIELD_SCALE,
            COALESCE(rf.RDB$DESCRIPTION, f.RDB$DESCRIPTION) AS DESCRIPTION, -- Adicionado: Busca descrição
            COALESCE(rf.RDB$NULL_FLAG, f.RDB$NULL_FLAG, 0) AS NULLABLE -- 0=NOT NULL, 1=NULL
        FROM RDB$RELATION_FIELDS rf
        JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
        WHERE rf.RDB$RELATION_NAME = ?
        ORDER BY rf.RDB$FIELD_POSITION;
    """
    try:
        cur.execute(sql, (relation_name,))
        columns = []
        field_type_map = {
            7: 'SMALLINT', 8: 'INTEGER', 10: 'FLOAT', 12: 'DATE',
            13: 'TIME', 14: 'CHAR', 16: 'BIGINT', 27: 'DOUBLE PRECISION',
            35: 'TIMESTAMP', 37: 'VARCHAR', 261: 'BLOB'
        }

        for row in cur.fetchallmap():
            field_type_code = row['FIELD_TYPE']
            field_type_name = field_type_map.get(field_type_code, f'UNKNOWN({field_type_code})')
            type_details = ""

            if field_type_name in ('CHAR', 'VARCHAR'):
                type_details = f"({row['FIELD_LENGTH']})"
            elif field_type_code == 261: # BLOB
                subtype = row['FIELD_SUB_TYPE']
                if subtype == 1: type_details = "(SUB_TYPE TEXT)"
                else: type_details = f"(SUB_TYPE {subtype})"
            # Verifica se é NUMERIC/DECIMAL (escala negativa)
            elif row['FIELD_SCALE'] is not None and row['FIELD_SCALE'] < 0:
                # Usar RDB$FIELD_PRECISION se disponível, senão aproximar
                precision = row['FIELD_PRECISION'] if row['FIELD_PRECISION'] else row['FIELD_LENGTH'] * 2 # Chute se precisão for 0
                scale = abs(row['FIELD_SCALE'])
                field_type_name = "DECIMAL" # Ou NUMERIC
                type_details = f"({precision},{scale})"
            elif field_type_name in ['FLOAT', 'DOUBLE PRECISION']:
                 pass # Geralmente não mostra precisão/escala

            # Decodifica descrição se for bytes
            description_bytes = row.get('DESCRIPTION')
            description = None
            if description_bytes:
                try:
                    description = description_bytes.decode(FIREBIRD_CHARSET, errors='replace')
                except Exception:
                    logger.warning(f"Não foi possível decodificar descrição para {relation_name}.{row['FIELD_NAME'].strip()}")
                    description = repr(description_bytes) # Mostra representação binária

            col_data = {
                "name": row['FIELD_NAME'].strip(),
                "type": field_type_name + type_details,
                "nullable": bool(row['NULLABLE']),
                "description": description # Adicionado
            }
            columns.append(col_data)
        return columns
    except Exception as e:
        logger.error(f"Erro ao buscar detalhes de coluna para {relation_name}: {e}", exc_info=True)
        return []

def get_constraint_details(cur, relation_name):
    """Busca detalhes das constraints (PK, FK, Unique) para uma dada tabela."""
    # ... (Lógica de get_constraint_details do util-extract_firebird_schema.py) ...
    # Adiciona busca pela descrição da constraint
    sql_constraints = """
        SELECT
            rc.RDB$CONSTRAINT_NAME AS CONSTRAINT_NAME,
            rc.RDB$CONSTRAINT_TYPE AS CONSTRAINT_TYPE,
            rc.RDB$INDEX_NAME AS LOCAL_INDEX_NAME,
            fk.RDB$CONST_NAME_UQ AS REF_CONSTRAINT_NAME,
            fk.RDB$UPDATE_RULE AS FK_UPDATE_RULE,
            fk.RDB$DELETE_RULE AS FK_DELETE_RULE,
            pk.RDB$RELATION_NAME AS FK_TARGET_TABLE,
            pk.RDB$INDEX_NAME AS REF_INDEX_NAME
        FROM RDB$RELATION_CONSTRAINTS rc
        LEFT JOIN RDB$REF_CONSTRAINTS fk ON rc.RDB$CONSTRAINT_NAME = fk.RDB$CONSTRAINT_NAME
        LEFT JOIN RDB$RELATION_CONSTRAINTS pk ON fk.RDB$CONST_NAME_UQ = pk.RDB$CONSTRAINT_NAME
        WHERE rc.RDB$RELATION_NAME = ?
        ORDER BY rc.RDB$CONSTRAINT_NAME;
    """
    sql_index_columns = """ -- Consulta genérica para colunas de um índice
        SELECT TRIM(ix.RDB$FIELD_NAME) AS FIELD_NAME
        FROM RDB$INDEX_SEGMENTS ix
        WHERE ix.RDB$INDEX_NAME = ?
        ORDER BY ix.RDB$FIELD_POSITION;
    """
    constraints = defaultdict(list)
    try:
        cur.execute(sql_constraints, (relation_name,))
        for row in cur.fetchallmap():
            constraint_name = row['CONSTRAINT_NAME'].strip()
            constraint_type = row['CONSTRAINT_TYPE'].strip()
            local_index_name = row['LOCAL_INDEX_NAME'].strip() if row['LOCAL_INDEX_NAME'] else None
            ref_constraint_name = row['REF_CONSTRAINT_NAME'].strip() if row['REF_CONSTRAINT_NAME'] else None
            ref_index_name = row['REF_INDEX_NAME'].strip() if row['REF_INDEX_NAME'] else None

            local_columns = []
            if local_index_name:
                try:
                    cur.execute(sql_index_columns, (local_index_name,))
                    local_columns = [seg['FIELD_NAME'] for seg in cur.fetchallmap()]
                except Exception as e:
                    logger.warning(f"Erro ao buscar colunas locais para índice {local_index_name} da constraint {constraint_name}: {e}")
            
            referenced_columns = []
            if constraint_type == 'FOREIGN KEY' and ref_index_name:
                try:
                    cur.execute(sql_index_columns, (ref_index_name,))
                    referenced_columns = [seg['FIELD_NAME'] for seg in cur.fetchallmap()]
                except Exception as e:
                    logger.warning(f"Erro ao buscar colunas referenciadas para índice {ref_index_name} da FK {constraint_name}: {e}")

            constraint_data = {
                "name": constraint_name,
                "columns": local_columns
            }

            if constraint_type == 'PRIMARY KEY':
                constraints['primary_key'].append(constraint_data)
            elif constraint_type == 'FOREIGN KEY':
                constraint_data['references_table'] = row['FK_TARGET_TABLE'].strip() if row['FK_TARGET_TABLE'] else None
                constraint_data['references_columns'] = referenced_columns 
                constraint_data['update_rule'] = row['FK_UPDATE_RULE'].strip() if row['FK_UPDATE_RULE'] else 'RESTRICT'
                constraint_data['delete_rule'] = row['FK_DELETE_RULE'].strip() if row['FK_DELETE_RULE'] else 'RESTRICT'
                constraints['foreign_keys'].append(constraint_data)
            elif constraint_type == 'UNIQUE':
                constraints['unique'].append(constraint_data)
            # Ignorar NOT NULL e CHECK por enquanto para simplificar
            # elif constraint_type == 'NOT NULL':
            #      constraints['not_null'].append(constraint_data)
            # elif constraint_type == 'CHECK':
            #     constraints['check'].append({"name": constraint_name, "expression": "<CHECK EXPRESSION NOT EXTRACTED>"})
            # else:
            #     constraint_data['type'] = constraint_type
            #     constraints['other'].append(constraint_data)

        return dict(constraints)
    except Exception as e:
         logger.error(f"Erro ao buscar constraints para {relation_name}: {e}", exc_info=True)
         return {}

def extract_technical_schema(conn):
    """Extrai o schema técnico detalhado (tabelas, views, colunas, constraints)."""
    schema = {}
    if not conn:
        return schema
    try:
        cur = conn.cursor()
        logger.info("Extraindo tabelas e views...")
        sql_relations = """
            SELECT TRIM(RDB$RELATION_NAME) as NAME, RDB$VIEW_BLR, RDB$DESCRIPTION
            FROM RDB$RELATIONS
            WHERE RDB$SYSTEM_FLAG = 0 OR RDB$SYSTEM_FLAG IS NULL
            ORDER BY RDB$RELATION_NAME;
        """
        cur.execute(sql_relations)

        for row in cur.fetchallmap():
            relation_name = row['NAME']
            is_view = row['RDB$VIEW_BLR'] is not None
            object_type = "VIEW" if is_view else "TABLE"
            description_bytes = row.get('RDB$DESCRIPTION')
            description = None
            if description_bytes:
                try:
                    description = description_bytes.decode(FIREBIRD_CHARSET, errors='replace')
                except Exception:
                     logger.warning(f"Não foi possível decodificar descrição para {object_type} {relation_name}")
                     description = repr(description_bytes)

            logger.info(f"Processando {object_type}: {relation_name}...")
            schema[relation_name] = {
                "object_type": object_type,
                "description": description,
                "columns": get_column_details(cur, relation_name),
                "constraints": get_constraint_details(cur, relation_name) 
            }
        cur.close()
        logger.info(f"Extração de estrutura concluída. Total de objetos: {len(schema)}")
        return schema
    except Exception as e:
        logger.error(f"Erro durante a extração do schema: {e}", exc_info=True)
        return {}

def calculate_fk_reference_counts(schema_data):
    """Calcula quantas vezes cada tabela/coluna é referenciada por FKs."""
    table_ref_counts = defaultdict(int)
    column_ref_counts_flat = defaultdict(int)
    
    logger.info("Calculando contagens de referência de FK...")
    for relation_name, data in schema_data.items():
        if data.get("object_type") == "TABLE":
            for fk in data.get("constraints", {}).get("foreign_keys", []):
                target_table = fk.get('references_table')
                target_columns = fk.get('references_columns', [])
                if target_table:
                    table_ref_counts[target_table] += 1
                    for i, target_col in enumerate(target_columns):
                        # Tenta mapear coluna local para coluna referenciada pela posição
                        local_col = fk["columns"][i] if i < len(fk["columns"]) else "?"
                        logger.debug(f"FK Ref: {relation_name}.{local_col} -> {target_table}.{target_col}")
                        column_ref_counts_flat[f"{target_table}.{target_col}"] += 1
                        
    logger.info("Cálculo de contagens de referência concluído.")
    # Adiciona as contagens de volta à estrutura do schema para facilitar o acesso
    for table_name, count in table_ref_counts.items():
        if table_name in schema_data:
             schema_data[table_name]["referenced_by_fk_count"] = count
             
    return dict(column_ref_counts_flat)

def save_technical_details(schema_data, filename):
    """Salva os detalhes técnicos (schema e contagens) em um arquivo JSON."""
    logger.info(f"Salvando detalhes técnicos do schema em {filename}...")
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, indent=4, ensure_ascii=False)
        logger.info("Detalhes técnicos salvos com sucesso.")
    except IOError as e:
        logger.error(f"Erro ao salvar o arquivo JSON: {e}")
    except Exception as e:
         logger.exception("Erro inesperado ao salvar o JSON técnico:")

# --- Execução Principal ---
if __name__ == "__main__":
    # Verifica se a senha está no .env, senão pede
    if not FIREBIRD_PASSWORD:
        logger.warning("Senha do Firebird não encontrada no .env. Solicitando...")
        FIREBIRD_PASSWORD = getpass.getpass(f"Digite a senha para o usuário '{FIREBIRD_USER}' em {FIREBIRD_HOST}: ")

    # Verifica DB_PATH
    if not FIREBIRD_DB_PATH:
        logger.error("Erro: Variável FIREBIRD_DB_PATH não definida no .env ou ambiente.")
        sys.exit(1)
        
    conn = None
    try:
        logger.info(f"Conectando a {FIREBIRD_HOST}:{FIREBIRD_DB_PATH}...")
        conn = fdb.connect(
            host=FIREBIRD_HOST,
            port=FIREBIRD_PORT,
            database=FIREBIRD_DB_PATH,
            user=FIREBIRD_USER,
            password=FIREBIRD_PASSWORD,
            charset=FIREBIRD_CHARSET
        )
        logger.info("Conexão estabelecida.")
        
        technical_schema = extract_technical_schema(conn)
        
        if technical_schema:
            fk_counts_dict = calculate_fk_reference_counts(technical_schema)
            if fk_counts_dict:
                technical_schema['fk_reference_counts'] = fk_counts_dict
                logger.info(f"Contagens de FK adicionadas ao schema ({len(fk_counts_dict)} entradas).")
            else:
                logger.warning("Nenhuma contagem de FK foi calculada.")
            save_technical_details(technical_schema, OUTPUT_JSON_FILE)
        else:
            logger.error("Falha ao extrair o schema técnico.")
            sys.exit(1)
            
    except fdb.Error as e:
        logger.error(f"Erro de conexão ou execução Firebird: {e}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erro inesperado no fluxo principal: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if conn and not conn.closed:
            conn.close()
            logger.info("Conexão principal fechada.")

    print(f"\nProcesso concluído. Detalhes técnicos salvos em {OUTPUT_JSON_FILE}")
    print("Próximo passo: Use este arquivo JSON para gerar um template ou adicionar descrições manuais.") 