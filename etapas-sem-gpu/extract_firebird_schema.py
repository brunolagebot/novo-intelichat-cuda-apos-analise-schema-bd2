import fdb
import json
import getpass
import logging
from collections import defaultdict

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configurações (Melhorar com args ou config file no futuro) ---
DB_PATH = r"C:\Projetos\DADOS.FDB"
DB_USER = "SYSDBA"
DB_CHARSET = "WIN1252"
OUTPUT_JSON_FILE = "firebird_schema.json"

def get_column_details(cur, relation_id):
    """Busca detalhes das colunas para uma dada tabela (relation_id)."""
    sql = """
        SELECT
            rf.RDB$FIELD_NAME AS FIELD_NAME,
            f.RDB$FIELD_TYPE AS FIELD_TYPE,
            f.RDB$FIELD_SUB_TYPE AS FIELD_SUB_TYPE,
            f.RDB$FIELD_LENGTH AS FIELD_LENGTH,
            f.RDB$FIELD_PRECISION AS FIELD_PRECISION,
            f.RDB$FIELD_SCALE AS FIELD_SCALE,
            COALESCE(rf.RDB$NULL_FLAG, f.RDB$NULL_FLAG, 0) AS NULLABLE -- 0=NOT NULL, 1=NULL
        FROM RDB$RELATION_FIELDS rf
        JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
        WHERE rf.RDB$RELATION_NAME = ?
        ORDER BY rf.RDB$FIELD_POSITION;
    """
    cur.execute(sql, (relation_id,))
    columns = []
    field_type_map = {
        7: 'SMALLINT', 8: 'INTEGER', 10: 'FLOAT', 12: 'DATE',
        13: 'TIME', 14: 'CHAR', 16: 'BIGINT', 27: 'DOUBLE PRECISION',
        35: 'TIMESTAMP', 37: 'VARCHAR', 261: 'BLOB'
        # Adicionar outros tipos conforme necessário
    }

    for row in cur.fetchallmap():
        field_type_code = row['FIELD_TYPE']
        field_type_name = field_type_map.get(field_type_code, f'UNKNOWN({field_type_code})')

        # Adicionar detalhes específicos do tipo
        type_details = f"({row['FIELD_LENGTH']})" if field_type_name in ['CHAR', 'VARCHAR'] else ""
        if field_type_code == 261: # BLOB
             subtype = row['FIELD_SUB_TYPE']
             if subtype == 1: type_details = "(SUB_TYPE TEXT)"
             else: type_details = f"(SUB_TYPE {subtype})"
        elif field_type_name in ['SMALLINT', 'INTEGER', 'BIGINT'] and row['FIELD_PRECISION']: # Numericos
             precision = row['FIELD_PRECISION']
             scale = abs(row['FIELD_SCALE'] or 0)
             type_details = f"({precision},{scale})"
        elif field_type_name in ['FLOAT', 'DOUBLE PRECISION']:
             pass # Geralmente não mostram precisão/escala assim

        col_data = {
            "name": row['FIELD_NAME'].strip(),
            "type": field_type_name + type_details,
            "nullable": bool(row['NULLABLE'])
        }
        columns.append(col_data)
    return columns

def get_constraint_details(cur, relation_id):
    """Busca detalhes das constraints para uma dada tabela."""
    sql_constraints = """
        SELECT
            rc.RDB$CONSTRAINT_NAME AS CONSTRAINT_NAME,
            rc.RDB$CONSTRAINT_TYPE AS CONSTRAINT_TYPE,
            rc.RDB$INDEX_NAME AS LOCAL_INDEX_NAME, -- Renomeado para clareza
            fk.RDB$CONST_NAME_UQ AS REF_CONSTRAINT_NAME, -- Nome da constraint PK/UQ referenciada
            fk.RDB$UPDATE_RULE AS FK_UPDATE_RULE,
            fk.RDB$DELETE_RULE AS FK_DELETE_RULE,
            pk.RDB$RELATION_NAME AS FK_TARGET_TABLE,
            pk.RDB$INDEX_NAME AS REF_INDEX_NAME -- Nome do índice da PK/UQ referenciada
        FROM RDB$RELATION_CONSTRAINTS rc
        LEFT JOIN RDB$REF_CONSTRAINTS fk ON rc.RDB$CONSTRAINT_NAME = fk.RDB$CONSTRAINT_NAME
        LEFT JOIN RDB$RELATION_CONSTRAINTS pk ON fk.RDB$CONST_NAME_UQ = pk.RDB$CONSTRAINT_NAME
        WHERE rc.RDB$RELATION_NAME = ?
        ORDER BY rc.RDB$CONSTRAINT_NAME;
    """
    sql_index_columns = """ -- Consulta genérica para colunas de um índice
        SELECT ix.RDB$FIELD_NAME AS FIELD_NAME
        FROM RDB$INDEX_SEGMENTS ix
        WHERE ix.RDB$INDEX_NAME = ?
        ORDER BY ix.RDB$FIELD_POSITION;
    """
    cur.execute(sql_constraints, (relation_id,))
    constraints = defaultdict(list)

    for row in cur.fetchallmap():
        constraint_name = row['CONSTRAINT_NAME'].strip()
        constraint_type = row['CONSTRAINT_TYPE'].strip()
        local_index_name = row['LOCAL_INDEX_NAME'].strip() if row['LOCAL_INDEX_NAME'] else None
        ref_constraint_name = row['REF_CONSTRAINT_NAME'].strip() if row['REF_CONSTRAINT_NAME'] else None
        ref_index_name = row['REF_INDEX_NAME'].strip() if row['REF_INDEX_NAME'] else None

        # Busca colunas locais associadas ao índice local da constraint
        local_columns = []
        if local_index_name:
            cur.execute(sql_index_columns, (local_index_name,))
            local_columns = [seg['FIELD_NAME'].strip() for seg in cur.fetchallmap()]
        
        # Busca colunas referenciadas (PK/UQ) associadas ao índice referenciado
        referenced_columns = []
        if constraint_type == 'FOREIGN KEY' and ref_index_name:
            try:
                cur.execute(sql_index_columns, (ref_index_name,))
                referenced_columns = [seg['FIELD_NAME'].strip() for seg in cur.fetchallmap()]
                logger.debug(f"  -> Colunas Referenciadas para FK {constraint_name} ({ref_index_name}): {referenced_columns}")
            except Exception as e:
                logger.warning(f"Erro ao buscar colunas referenciadas para o índice {ref_index_name} da FK {constraint_name}: {e}")

        constraint_data = {
            "name": constraint_name,
            "columns": local_columns
        }

        if constraint_type == 'PRIMARY KEY':
            constraints['primary_key'].append(constraint_data)
        elif constraint_type == 'FOREIGN KEY':
            constraint_data['references_table'] = row['FK_TARGET_TABLE'].strip() if row['FK_TARGET_TABLE'] else None
            # ADICIONADO: Preenche as colunas referenciadas
            constraint_data['references_columns'] = referenced_columns 
            constraint_data['update_rule'] = row['FK_UPDATE_RULE'].strip() if row['FK_UPDATE_RULE'] else 'RESTRICT'
            constraint_data['delete_rule'] = row['FK_DELETE_RULE'].strip() if row['FK_DELETE_RULE'] else 'RESTRICT'
            constraints['foreign_keys'].append(constraint_data)
        elif constraint_type == 'UNIQUE':
            constraints['unique'].append(constraint_data)
        elif constraint_type == 'NOT NULL':
             constraints['not_null'].append(constraint_data)
        elif constraint_type == 'CHECK':
            constraints['check'].append({"name": constraint_name, "expression": "<CHECK EXPRESSION NOT EXTRACTED>"})
        else:
            constraint_data['type'] = constraint_type
            constraints['other'].append(constraint_data)

    # Converte defaultdict para dict normal para o JSON
    return dict(constraints)

def extract_schema(db_path, user, password, charset):
    """Conecta ao banco Firebird e extrai o esquema das tabelas e views de usuário."""
    schema = {}
    conn = None
    try:
        logger.info(f"Conectando ao banco de dados: {db_path}")
        conn = fdb.connect(
            dsn=db_path,
            user=user,
            password=password,
            charset=charset
        )
        cur = conn.cursor()
        logger.info("Conexão bem-sucedida. Extraindo tabelas e views...")

        # Seleciona tabelas e views de usuário (SYSTEM_FLAG = 0 ou NULL)
        # Inclui RDB$VIEW_BLR para identificar views
        sql_relations = """
            SELECT RDB$RELATION_NAME, RDB$VIEW_BLR
            FROM RDB$RELATIONS
            WHERE RDB$SYSTEM_FLAG = 0 OR RDB$SYSTEM_FLAG IS NULL
            ORDER BY RDB$RELATION_NAME;
        """
        cur.execute(sql_relations)

        for table_row in cur.fetchallmap():
            table_name = table_row['RDB$RELATION_NAME'].strip()
            is_view = table_row['RDB$VIEW_BLR'] is not None
            object_type = "VIEW" if is_view else "TABLE"

            logger.info(f"Processando {object_type}: {table_name}...")
            schema[table_name] = {
                "object_type": object_type,
                "columns": get_column_details(cur, table_name),
                # Constraints podem ser menos relevantes ou vazias para views
                "constraints": get_constraint_details(cur, table_name)
            }

        logger.info(f"Extração concluída. Total de tabelas/views encontradas: {len(schema)}")
        return schema

    except fdb.Error as e:
        logger.error(f"Erro do Firebird: {e}")
        return None
    except Exception as e:
        logger.exception("Erro inesperado durante a extração:")
        return None
    finally:
        if conn:
            conn.close()
            logger.info("Conexão de extração fechada.")

def main():
    try:
        # Solicita a senha de forma segura
        db_password = getpass.getpass(f"Digite a senha para o usuário '{DB_USER}': ")

        schema_data = extract_schema(DB_PATH, DB_USER, db_password, DB_CHARSET)

        if schema_data:
            logger.info(f"Salvando esquema em {OUTPUT_JSON_FILE}...")
            try:
                with open(OUTPUT_JSON_FILE, 'w', encoding='utf-8') as f:
                    json.dump(schema_data, f, indent=4, ensure_ascii=False)
                logger.info("Esquema salvo com sucesso.")
            except IOError as e:
                logger.error(f"Erro ao salvar o arquivo JSON: {e}")
            except Exception as e:
                 logger.exception("Erro inesperado ao salvar o JSON:")
        else:
            logger.error("Não foi possível extrair o esquema do banco de dados.")

    except KeyboardInterrupt:
        logger.info("Operação cancelada pelo usuário.")

if __name__ == "__main__":
    main() 