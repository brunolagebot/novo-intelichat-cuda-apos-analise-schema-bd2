import fdb # ADICIONADO: Import da biblioteca Firebird
import logging
import json
import sys
import os
from dotenv import load_dotenv
import getpass # Para senha, se não estiver no .env
from collections import defaultdict, OrderedDict # NOVO: OrderedDict para manter ordem
import toml # NOVO
from tqdm import tqdm # NOVO: Para barra de progresso
import datetime # Adicionado para lidar com datas

# Carregar variáveis de ambiente (ainda útil como fallback)
load_dotenv()

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Configurações --- #

# Caminho para o arquivo de segredos do Streamlit
SECRETS_FILE_PATH = os.path.join(os.path.dirname(__file__), '..', '.streamlit', 'secrets.toml')

# Função auxiliar para carregar segredos
def load_secrets(file_path):
    secrets = {}
    if os.path.exists(file_path):
        try:
            secrets = toml.load(file_path)
            logger.info(f"Segredos carregados de: {file_path}")
        except Exception as e:
            logger.warning(f"Erro ao carregar ou parsear {file_path}: {e}. Usando fallbacks.")
    else:
        logger.info(f"Arquivo {file_path} não encontrado. Usando fallbacks (env vars/prompt).")
    return secrets

# Carrega os segredos
secrets = load_secrets(SECRETS_FILE_PATH)

# Obtém configurações, priorizando secrets.toml, depois .env, depois padrão/prompt
def get_config_value(secret_key, env_var, default=None):
    # Tenta obter da seção [database] do secrets.toml
    value = secrets.get('database', {}).get(secret_key)
    if value is not None:
        logger.debug(f"Config '{secret_key}' obtida de secrets.toml.")
        return value
    # Se não encontrou no secrets, tenta variável de ambiente
    value = os.getenv(env_var)
    if value is not None:
        logger.debug(f"Config '{env_var}' obtida de variáveis de ambiente.")
        return value
    # Se não encontrou em nenhum, usa o padrão
    logger.debug(f"Usando valor padrão '{default}' para '{secret_key}/{env_var}'.")
    return default

FIREBIRD_HOST = get_config_value("host", "FIREBIRD_HOST", "localhost")
FIREBIRD_PORT = int(get_config_value("port", "FIREBIRD_PORT", "3050"))
FIREBIRD_DB_PATH = get_config_value("db_path", "FIREBIRD_DB_PATH")
FIREBIRD_USER = get_config_value("user", "FIREBIRD_USER", "SYSDBA")
# Tratamento especial para senha
FIREBIRD_PASSWORD = secrets.get('database', {}).get("password")
if FIREBIRD_PASSWORD:
     logger.debug("Senha obtida de secrets.toml.")
else:
     FIREBIRD_PASSWORD = os.getenv("FIREBIRD_PASSWORD")
     if FIREBIRD_PASSWORD:
         logger.debug("Senha obtida de variáveis de ambiente.")
     # Senão, será pedida no final
FIREBIRD_CHARSET = get_config_value("charset", "FIREBIRD_CHARSET", "WIN1252")

# NOVO: Nome do arquivo de saída para o schema aprimorado
OUTPUT_JSON_FILE = 'data/enhanced_technical_schema.json'
# LEGACY_OUTPUT_JSON_FILE = 'data/technical_schema_details.json' # Manter se necessário

# --- Funções de Extração Refatoradas ---

def get_column_metadata(cur, relation_name):
    """Busca APENAS os metadados das colunas (sem amostras)."""
    sql = """
        SELECT
            TRIM(rf.RDB$FIELD_NAME) AS FIELD_NAME,
            f.RDB$FIELD_TYPE AS FIELD_TYPE,
            f.RDB$FIELD_SUB_TYPE AS FIELD_SUB_TYPE,
            f.RDB$FIELD_LENGTH AS FIELD_LENGTH,
            f.RDB$FIELD_PRECISION AS FIELD_PRECISION,
            f.RDB$FIELD_SCALE AS FIELD_SCALE,
            COALESCE(rf.RDB$DESCRIPTION, f.RDB$DESCRIPTION) AS DESCRIPTION,
            COALESCE(rf.RDB$NULL_FLAG, f.RDB$NULL_FLAG, 0) AS NULLABLE,
            f.RDB$DEFAULT_SOURCE AS DEFAULT_SOURCE
        FROM RDB$RELATION_FIELDS rf
        JOIN RDB$FIELDS f ON rf.RDB$FIELD_SOURCE = f.RDB$FIELD_NAME
        WHERE rf.RDB$RELATION_NAME = ?
        ORDER BY rf.RDB$FIELD_POSITION;
    """
    columns = []
    try:
        cur.execute(sql, (relation_name,))
        field_type_map = {
            7: 'SMALLINT', 8: 'INTEGER', 10: 'FLOAT', 12: 'DATE',
            13: 'TIME', 14: 'CHAR', 16: 'BIGINT', 27: 'DOUBLE PRECISION',
            35: 'TIMESTAMP', 37: 'VARCHAR', 261: 'BLOB'
        }
        for row in cur.fetchallmap():
            col_name = row['FIELD_NAME'].strip()
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
                    logger.warning(f"Não foi possível decodificar descrição para {relation_name}.{col_name}")
                    description = repr(description_bytes) # Mostra representação binária

            # NOVO: Processar valor padrão
            default_value_source = None
            default_source_raw = row.get('DEFAULT_SOURCE') # Texto como 'DEFAULT 0'
            if default_source_raw:
                 try:
                     # Tenta decodificar e remover o prefixo 'DEFAULT '
                     default_value_source = default_source_raw.decode(FIREBIRD_CHARSET, errors='replace').strip()
                     if default_value_source.upper().startswith('DEFAULT '):
                          default_value_source = default_value_source[8:].strip()
                 except Exception:
                     logger.warning(f"Não foi possível decodificar DEFAULT_SOURCE para {relation_name}.{col_name}")
                     default_value_source = repr(default_source_raw)
            # TODO: Poderia tentar interpretar default_value_internal se source for None, mas é complexo

            col_data = OrderedDict([
                ("name", col_name),
                ("type", field_type_name + type_details),
                ("nullable", bool(row['NULLABLE'])),
                ("default_value", default_value_source),
                ("description", description)
            ])
            columns.append(col_data)
    except Exception as e:
        logger.error(f"Erro ao buscar metadados de coluna para {relation_name}: {e}", exc_info=True)
        # Retorna lista vazia em caso de erro para não quebrar o processo
        return [] 
    return columns

def populate_column_samples(columns_metadata_list, sample_rows):
    """Adiciona a chave 'sample_values' a cada dicionário na lista de metadados de colunas."""
    if sample_rows is None: # Se a busca de amostra falhou
        for col_data in columns_metadata_list:
            col_data['sample_values'] = [] # Adiciona chave vazia
        return

    if not sample_rows: # Se a tabela estava vazia
        for col_data in columns_metadata_list:
            col_data['sample_values'] = [] # Adiciona chave vazia
        return

    # Extrai amostras para cada coluna
    for col_data in columns_metadata_list:
        col_name = col_data['name']
        col_type = col_data['type'] # Obtém o tipo já processado
        sample_values = []
        seen_samples = set()

        # --- NOVO: Pular amostragem para tipos considerados booleanos ---
        # TODO: Ajustar esta lista se outros tipos forem usados como booleanos (ex: INTEGER com check constraint)
        boolean_like_types = ['SMALLINT'] 
        if col_type in boolean_like_types:
            logger.debug(f"Pulando amostragem para coluna booleana (tipo {col_type}): {col_name}")
            col_data['sample_values'] = ["BOOLEAN_SKIPPED"] # Marca como pulado
            continue # Pula para a próxima coluna
        # --- FIM NOVO ---

        try:
            for sample_row in sample_rows:
                if col_name not in sample_row: # Coluna pode não existir na amostra (views complexas?)
                    continue
                value = sample_row[col_name] # Acesso direto após checar existência
                if value is not None:
                    if isinstance(value, bytes):
                        str_value = "[BLOB_DATA]"
                    elif isinstance(value, (datetime.date, datetime.datetime)):
                        str_value = value.isoformat()
                    else:
                        str_value = str(value)

                    if str_value not in seen_samples:
                        # --- ALTERADO: Limite aumentado para 50 ---
                        if len(sample_values) < 50:
                            sample_values.append(str_value)
                            seen_samples.add(str_value)
                        else:
                            break
        except Exception as sample_ex:
            logger.warning(f"Erro ao processar amostra para coluna '{col_name}': {sample_ex}")
            # Deixa sample_values com o que conseguiu coletar até o erro

        col_data['sample_values'] = sample_values # Adiciona a lista (pode ser vazia)

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

            constraint_data = OrderedDict([
                ("name", constraint_name),
                ("columns", local_columns)
            ])

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

def get_index_details(cur, relation_name):
    """Busca detalhes de TODOS os índices (usuário, sistema, PK, FK, Unique)."""
    sql_indexes = """
        SELECT
            TRIM(i.RDB$INDEX_NAME) as INDEX_NAME,
            i.RDB$UNIQUE_FLAG as IS_UNIQUE,
            TRIM(s.RDB$FIELD_NAME) as FIELD_NAME,
            s.RDB$FIELD_POSITION as FIELD_POSITION,
            i.RDB$INDEX_INACTIVE as IS_INACTIVE,
            i.RDB$INDEX_TYPE as INDEX_TYPE, -- 0=ASC, 1=DESC
            i.RDB$SYSTEM_FLAG as IS_SYSTEM, -- NOVO: Captura flag do sistema
            rc.RDB$CONSTRAINT_TYPE as CONSTRAINT_TYPE -- NOVO: Tipo de constraint associada
        FROM RDB$INDICES i
        JOIN RDB$INDEX_SEGMENTS s ON i.RDB$INDEX_NAME = s.RDB$INDEX_NAME
        LEFT JOIN RDB$RELATION_CONSTRAINTS rc ON i.RDB$INDEX_NAME = rc.RDB$INDEX_NAME -- NOVO: JOIN para tipo de constraint
        WHERE i.RDB$RELATION_NAME = ? 
        -- REMOVIDO: AND i.RDB$FOREIGN_KEY IS NULL 
        -- REMOVIDO: AND i.RDB$SYSTEM_FLAG = 0
        ORDER BY i.RDB$INDEX_NAME, s.RDB$FIELD_POSITION;
    """
    # Usar OrderedDict para garantir ordem consistente dos campos
    indexes = defaultdict(lambda: OrderedDict([
        ('name', None), 
        ('purpose', 'USER_DEFINED'), # Default para USER_DEFINED
        ('columns', []), 
        ('is_unique', False), 
        ('is_inactive', False), 
        ('is_system', False)
    ]))
    try:
        cur.execute(sql_indexes, (relation_name,))
        for row in cur.fetchallmap():
            index_name = row['INDEX_NAME']
            indexes[index_name]['name'] = index_name
            indexes[index_name]['columns'].append(row['FIELD_NAME'])
            indexes[index_name]['is_unique'] = bool(row['IS_UNIQUE'])
            indexes[index_name]['is_inactive'] = bool(row['IS_INACTIVE'])
            indexes[index_name]['is_system'] = bool(row['IS_SYSTEM'])
            
            # Determina o propósito
            constraint_type = row['CONSTRAINT_TYPE']
            if constraint_type:
                constraint_type = constraint_type.strip()
                # Mapeia o tipo da constraint para o propósito do índice
                if constraint_type == 'PRIMARY KEY':
                    indexes[index_name]['purpose'] = 'PRIMARY KEY'
                elif constraint_type == 'FOREIGN KEY':
                     indexes[index_name]['purpose'] = 'FOREIGN KEY'
                elif constraint_type == 'UNIQUE':
                     indexes[index_name]['purpose'] = 'UNIQUE'
                # Ignora outros tipos de constraint (CHECK, NOT NULL) que podem usar índices
                # Se não for PK/FK/UNIQUE, mas tiver constraint, mantém como USER_DEFINED por ora
            elif bool(row['IS_SYSTEM']):
                 indexes[index_name]['purpose'] = 'SYSTEM'
            # Senão, permanece 'USER_DEFINED'
            
            # Poderia adicionar direção ASC/DESC com base em INDEX_TYPE se necessário

        return list(indexes.values()) # Retorna lista de dicionários ordenados
    except Exception as e:
         logger.error(f"Erro ao buscar índices para {relation_name}: {e}", exc_info=True)
         return []

def extract_technical_schema(conn):
    """Extrai o schema técnico em etapas lógicas."""
    schema = OrderedDict()
    if not conn:
        return schema
        
    cur = None 
    pbar = None 
    try:
        cur = conn.cursor()
        
        # --- Contagem Inicial (para info e talvez progresso) ---
        logger.info("Buscando lista de tabelas e views...")
        sql_relations = """
            SELECT TRIM(RDB$RELATION_NAME) as NAME, RDB$VIEW_BLR, RDB$DESCRIPTION
            FROM RDB$RELATIONS
            WHERE RDB$SYSTEM_FLAG = 0 OR RDB$SYSTEM_FLAG IS NULL
            ORDER BY RDB$RELATION_NAME;
        """
        cur.execute(sql_relations)
        all_relations = cur.fetchallmap()
        total_relations = len(all_relations)
        logger.info(f"Encontradas {total_relations} tabelas/views.")
        # Opcional: contar colunas totais aqui se desejado para log, mas a barra será por relação
        # total_columns = ... (query de contagem)
        # logger.info(f"Total de colunas estimado: {total_columns}")

        # --- Loop Principal por Relação (com tqdm) ---
        pbar = tqdm(total=total_relations, desc="Extraindo Schema", unit=" Relação")
        
        for row in all_relations:
            relation_name = row['NAME']
            is_view = row['RDB$VIEW_BLR'] is not None
            object_type = "VIEW" if is_view else "TABLE"
            tqdm.write(f"\nProcessando {object_type}: {relation_name}...") # \n para garantir linha nova
            
            # Etapa 1: Infos Básicas
            description_bytes = row.get('RDB$DESCRIPTION')
            description = None
            if description_bytes:
                try: description = description_bytes.decode(FIREBIRD_CHARSET, errors='replace')
                except Exception: description = repr(description_bytes)
                
            schema[relation_name] = OrderedDict([
                ("object_type", object_type),
                ("description", description),
                ("columns", []), # Inicializa lista vazia
                ("constraints", {}),
                ("indexes", [])
            ])

            # Etapa 2: Metadados Colunas
            tqdm.write(f"  - Buscando metadados de colunas...")
            columns_metadata = get_column_metadata(cur, relation_name)
            schema[relation_name]["columns"] = columns_metadata
            
            # Etapa 3: Amostra de Dados
            tqdm.write(f"  - Buscando amostra de dados (TOP 50)...")
            sample_rows = None
            try:
                sample_sql = f'SELECT FIRST 50 * FROM "{relation_name}";' 
                cur.execute(sample_sql)
                sample_rows = cur.fetchallmap()
                tqdm.write(f"    -> Amostra OK ({len(sample_rows)} linhas).")
            except fdb.DatabaseError as db_err:
                 if hasattr(db_err, 'sqlcode') and db_err.sqlcode in [-804, -607, -204, -551, -901]: # Adicionado -901 (GTT?)
                     tqdm.write(f"    -> AVISO: Não foi possível buscar amostra (SQLCODE: {db_err.sqlcode}). Pulando amostras.")
                     logger.warning(f"Não foi possível buscar amostra para {relation_name} (SQLCODE: {db_err.sqlcode}). Provavelmente GTT, permissão ou objeto inacessível.")
                 else:
                     tqdm.write(f"    -> ERRO DB: Falha ao buscar amostra: {db_err}")
                     logger.error(f"Erro de banco ao buscar amostra para {relation_name}: {db_err}")
            except Exception as sample_err:
                tqdm.write(f"    -> ERRO: Falha inesperada ao buscar amostra: {sample_err}")
                logger.error(f"Erro inesperado ao buscar amostra para {relation_name}: {sample_err}")
            
            # Etapa 4: Popular Amostras (se a busca foi OK)
            if columns_metadata: # Só popula se conseguiu metadados
                 tqdm.write(f"  - Processando amostras para {len(columns_metadata)} colunas...")
                 populate_column_samples(columns_metadata, sample_rows)

            # Etapa 5: Constraints
            tqdm.write(f"  - Buscando constraints...")
            constraints_data = get_constraint_details(cur, relation_name)
            schema[relation_name]["constraints"] = constraints_data
            
            # Etapa 6: Índices
            tqdm.write(f"  - Buscando índices...")
            indexes_data = get_index_details(cur, relation_name)
            schema[relation_name]["indexes"] = indexes_data
            
            # Atualiza barra de progresso
            if pbar: pbar.update(1)
            
        if pbar: pbar.close() 
        logger.info(f"Extração de estrutura concluída. Total de objetos processados: {len(schema)}")
        
        # Etapa Final: Calcular FK Counts
        fk_counts = calculate_fk_reference_counts(schema)
        schema['fk_reference_counts'] = fk_counts
        logger.info("Contagens de referência FK calculadas e adicionadas.")
        
        return schema
        
    except Exception as e:
        if pbar: pbar.close() 
        logger.error(f"Erro durante a extração do schema: {e}", exc_info=True)
        return {}
    finally:
         # Garante que o cursor seja fechado se foi criado
         if cur:
             try: 
                 cur.close()
                 logger.debug("Cursor fechado no finally.")
             except Exception as close_err:
                 logger.error(f"Erro ao fechar cursor no finally: {close_err}")

def calculate_fk_reference_counts(schema_data):
    """Calcula quantas vezes cada tabela/coluna é referenciada por FKs."""
    column_ref_counts_flat = defaultdict(int)
    logger.info("Calculando contagens de referência de FK...")
    for relation_name, data in schema_data.items():
        # Ignora a chave interna que vamos adicionar
        if relation_name == 'fk_reference_counts': continue
        if isinstance(data, dict) and data.get("object_type") in ["TABLE", "VIEW"]:
            for fk in data.get("constraints", {}).get("foreign_keys", []):
                target_table = fk.get('references_table')
                target_columns = fk.get('references_columns', [])
                if target_table and target_columns:
                    for i, target_col in enumerate(target_columns):
                        column_ref_counts_flat[f"{target_table}.{target_col}"] += 1
    logger.info("Cálculo de contagens de referência concluído.")
    return dict(column_ref_counts_flat)

def save_technical_details(schema_data, filename):
    """Salva o dicionário do schema em um arquivo JSON."""
    logger.info(f"Salvando detalhes técnicos aprimorados em {filename}...")
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(schema_data, f, indent=4, ensure_ascii=False)
        logger.info("Detalhes técnicos aprimorados salvos com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar o JSON: {e}", exc_info=True)

# --- Execução Principal ---
if __name__ == "__main__":
    # Verifica se a senha foi obtida (secrets ou env var), senão pede
    if not FIREBIRD_PASSWORD:
        logger.warning("Senha do Firebird não encontrada em secrets.toml ou variáveis de ambiente. Solicitando...")
        FIREBIRD_PASSWORD = getpass.getpass(f"Digite a senha para o usuário '{FIREBIRD_USER}' em {FIREBIRD_HOST}: ")

    # Verifica DB_PATH
    if not FIREBIRD_DB_PATH:
        logger.error("Erro: Caminho do banco (db_path/FIREBIRD_DB_PATH) não definido em secrets.toml ou ambiente.")
        sys.exit(1)
        
    conn = None
    try:
        logger.info(f"Conectando a {FIREBIRD_HOST}:{FIREBIRD_DB_PATH}...")
        # Log dos parâmetros usados (exceto senha direta)
        logger.debug(f"  Usando Host: {FIREBIRD_HOST}, Porta: {FIREBIRD_PORT}, DB: {FIREBIRD_DB_PATH}, User: {FIREBIRD_USER}, Charset: {FIREBIRD_CHARSET}")
        conn = fdb.connect(
            host=FIREBIRD_HOST,
            port=FIREBIRD_PORT,
            database=FIREBIRD_DB_PATH,
            user=FIREBIRD_USER,
            password=FIREBIRD_PASSWORD, # Usa a senha obtida
            charset=FIREBIRD_CHARSET
        )
        logger.info("Conexão estabelecida.")
        
        technical_schema = extract_technical_schema(conn)
        
        if technical_schema:
            save_technical_details(technical_schema, OUTPUT_JSON_FILE)
        else:
            logger.error("Falha ao extrair o schema técnico.")
            sys.exit(1)
            
    except fdb.Error as e:
        # Verifica se o erro é especificamente -902 (usuário/senha inválido)
        if hasattr(e, 'sqlcode') and e.sqlcode == -902:
             logger.error(f"Erro de Autenticação Firebird (SQLCODE: -902). Verifique usuário/senha em secrets.toml ou variáveis de ambiente.")
        else:
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