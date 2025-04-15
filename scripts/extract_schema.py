import fdb
import logging
import json
import sys

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configurações da Conexão Firebird ---
# TODO: Considerar mover para um arquivo .env ou configuração externa
FIREBIRD_HOST = 'localhost'
FIREBIRD_PORT = 3050
FIREBIRD_DB_PATH = 'C:/Projetos/Dados.fdb' # Use o caminho do seu banco!
FIREBIRD_USER = 'SYSDBA'
FIREBIRD_PASSWORD = 'M@nagers2023' # Use sua senha!
FIREBIRD_CHARSET = 'WIN1252' # Ajuste conforme seu banco

# --- Configuração de Saída ---
OUTPUT_FILE = 'data/schema_dataset.jsonl' # Arquivo para o dataset de fine-tuning

def get_firebird_connection():
    """Estabelece e retorna uma conexão com o banco Firebird."""
    try:
        logging.info(f"Conectando a {FIREBIRD_HOST}:{FIREBIRD_DB_PATH}...")
        conn = fdb.connect(
            host=FIREBIRD_HOST,
            port=FIREBIRD_PORT,
            database=FIREBIRD_DB_PATH,
            user=FIREBIRD_USER,
            password=FIREBIRD_PASSWORD,
            charset=FIREBIRD_CHARSET
        )
        logging.info("Conexão estabelecida.")
        return conn
    except fdb.Error as e:
        logging.error(f"Erro ao conectar ao Firebird: {e}", exc_info=True)
        return None

def get_schema_metadata(conn):
    """Extrai metadados do schema (tabelas, views, colunas) do Firebird."""
    schema = {"tables": {}, "views": {}}
    if not conn:
        return schema

    try:
        cur = conn.cursor()

        # Buscar Tabelas de Usuário
        logging.info("Buscando tabelas de usuário...")
        cur.execute("""
            SELECT TRIM(rdb$relation_name) 
            FROM rdb$relations 
            WHERE rdb$system_flag = 0 AND rdb$view_blr IS NULL
            ORDER BY rdb$relation_name;
        """)
        tables = [row[0] for row in cur.fetchall()]
        logging.info(f"Encontradas {len(tables)} tabelas.")

        # Buscar Views de Usuário
        logging.info("Buscando views...")
        cur.execute("""
            SELECT TRIM(rdb$relation_name) 
            FROM rdb$relations 
            WHERE rdb$system_flag = 0 AND rdb$view_blr IS NOT NULL
            ORDER BY rdb$relation_name;
        """)
        views = [row[0] for row in cur.fetchall()]
        logging.info(f"Encontradas {len(views)} views.")

        # Para cada Tabela, buscar colunas
        for table_name in tables:
            logging.debug(f"Buscando colunas para tabela: {table_name}")
            # Query para buscar nome da coluna, tipo e se é NOT NULL
            # Nota: Determinar o tipo exato (VARCHAR(size), DECIMAL(p,s)) pode ser complexo
            # Vamos simplificar pegando o nome do tipo base por enquanto
            cur.execute("""
                SELECT 
                    TRIM(rf.rdb$field_name), 
                    CASE f.rdb$field_type 
                        WHEN 7 THEN 'SMALLINT' WHEN 8 THEN 'INTEGER' WHEN 10 THEN 'FLOAT' 
                        WHEN 12 THEN 'DATE' WHEN 13 THEN 'TIME' WHEN 14 THEN 'CHAR' 
                        WHEN 16 THEN 'BIGINT' WHEN 27 THEN 'DOUBLE PRECISION' 
                        WHEN 35 THEN 'TIMESTAMP' WHEN 37 THEN 'VARCHAR' 
                        WHEN 261 THEN 'BLOB' ELSE 'UNKNOWN' 
                    END, 
                    f.rdb$field_length, 
                    f.rdb$field_scale, 
                    f.rdb$field_sub_type, 
                    IIF(rf.rdb$null_flag = 1, 'NOT NULL', 'NULLABLE')
                FROM rdb$relation_fields rf 
                JOIN rdb$fields f ON rf.rdb$field_source = f.rdb$field_name 
                WHERE rf.rdb$relation_name = ? 
                ORDER BY rf.rdb$field_position;
            """, (table_name,))
            
            columns = []
            for row in cur.fetchall():
                col_name = row[0]
                base_type = row[1]
                length = row[2]
                scale = row[3]
                sub_type = row[4]
                nullable = row[5]
                
                # Formatar tipo detalhado
                col_type_details = base_type
                if base_type in ('VARCHAR', 'CHAR'):
                    col_type_details += f"({length})"
                elif base_type == 'BLOB' and sub_type == 1:
                     col_type_details = 'BLOB SUB_TYPE 1 (TEXT)'
                elif base_type == 'BLOB':
                     col_type_details += f" SUB_TYPE {sub_type}"
                elif base_type in ('SMALLINT', 'INTEGER', 'BIGINT', 'DOUBLE PRECISION', 'FLOAT') and scale < 0:
                    # Firebird usa escala negativa para DECIMAL/NUMERIC armazenado como inteiro
                    precision = length # Aproximação, cálculo exato é mais complexo
                    col_type_details = f"DECIMAL({precision}, {-scale})" 
                
                columns.append({"name": col_name, "type": col_type_details, "nullable": nullable})
            schema["tables"][table_name] = columns
            logging.debug(f"Colunas para {table_name}: {columns}")

        # Para cada View, buscar colunas (mesma lógica)
        for view_name in views:
            logging.debug(f"Buscando colunas para view: {view_name}")
            # A query é a mesma, pois views aparecem em rdb$relation_fields
            cur.execute("""
                SELECT 
                    TRIM(rf.rdb$field_name), 
                    CASE f.rdb$field_type 
                        WHEN 7 THEN 'SMALLINT' WHEN 8 THEN 'INTEGER' WHEN 10 THEN 'FLOAT' 
                        WHEN 12 THEN 'DATE' WHEN 13 THEN 'TIME' WHEN 14 THEN 'CHAR' 
                        WHEN 16 THEN 'BIGINT' WHEN 27 THEN 'DOUBLE PRECISION' 
                        WHEN 35 THEN 'TIMESTAMP' WHEN 37 THEN 'VARCHAR' 
                        WHEN 261 THEN 'BLOB' ELSE 'UNKNOWN' 
                    END, 
                    f.rdb$field_length, 
                    f.rdb$field_scale, 
                    f.rdb$field_sub_type, 
                    IIF(rf.rdb$null_flag = 1, 'NOT NULL', 'NULLABLE')
                FROM rdb$relation_fields rf 
                JOIN rdb$fields f ON rf.rdb$field_source = f.rdb$field_name 
                WHERE rf.rdb$relation_name = ? 
                ORDER BY rf.rdb$field_position;
            """, (view_name,))
            
            columns = []
            for row in cur.fetchall():
                col_name = row[0]
                base_type = row[1]
                length = row[2]
                scale = row[3]
                sub_type = row[4]
                nullable = row[5]
                
                col_type_details = base_type
                if base_type in ('VARCHAR', 'CHAR'):
                    col_type_details += f"({length})"
                elif base_type == 'BLOB' and sub_type == 1:
                     col_type_details = 'BLOB SUB_TYPE 1 (TEXT)'
                elif base_type == 'BLOB':
                     col_type_details += f" SUB_TYPE {sub_type}"
                elif base_type in ('SMALLINT', 'INTEGER', 'BIGINT', 'DOUBLE PRECISION', 'FLOAT') and scale < 0:
                    precision = length
                    col_type_details = f"DECIMAL({precision}, {-scale})"
                
                columns.append({"name": col_name, "type": col_type_details, "nullable": nullable})
            schema["views"][view_name] = columns
            logging.debug(f"Colunas para {view_name}: {columns}")

        cur.close()
        logging.info("Extração de metadados do schema concluída.")

    except fdb.Error as e:
        logging.error(f"Erro ao buscar metadados do Firebird: {e}", exc_info=True)
    finally:
        if conn and not conn.closed:
            conn.close()
            logging.info("Conexão fechada.")

    return schema

def format_schema_for_finetuning(schema):
    """Formata os metadados do schema extraído em formato JSONL para SFTTrainer."""
    dataset_entries = []

    # 1. Perguntas gerais sobre tabelas e views
    if schema["tables"]:
        table_list = ", ".join(schema["tables"].keys())
        dataset_entries.append({
            "messages": [
                {"role": "user", "content": "Quais são as tabelas de usuário neste banco de dados?"},
                {"role": "assistant", "content": f"As tabelas de usuário são: {table_list}."}
            ]
        })
    if schema["views"]:
        view_list = ", ".join(schema["views"].keys())
        dataset_entries.append({
            "messages": [
                {"role": "user", "content": "Quais views existem neste banco de dados?"},
                {"role": "assistant", "content": f"As views existentes são: {view_list}."}
            ]
        })
    if schema["tables"] and schema["views"]:
        all_list = ", ".join(list(schema["tables"].keys()) + list(schema["views"].keys()))
        dataset_entries.append({
            "messages": [
                {"role": "user", "content": "Liste todas as tabelas e views de usuário."},
                {"role": "assistant", "content": f"As tabelas e views são: {all_list}."}
            ]
        })
        
    # 2. Perguntas sobre a estrutura de cada tabela
    for table_name, columns in schema["tables"].items():
        col_descriptions = []
        for col in columns:
            desc = f"{col['name']} ({col['type']}, {col['nullable']})"
            col_descriptions.append(desc)
        structure_desc = f"A tabela {table_name} possui as seguintes colunas: {', '.join(col_descriptions)}."
        
        # Pergunta sobre a descrição da tabela
        dataset_entries.append({
            "messages": [
                {"role": "user", "content": f"Descreva a estrutura da tabela {table_name}."},
                {"role": "assistant", "content": structure_desc}
            ]
        })
        # Pergunta sobre quais colunas existem
        column_names = ", ".join([col['name'] for col in columns])
        dataset_entries.append({
            "messages": [
                {"role": "user", "content": f"Quais colunas existem na tabela {table_name}?"},
                {"role": "assistant", "content": f"A tabela {table_name} contém as colunas: {column_names}."}
            ]
        })

    # 3. Perguntas sobre a estrutura de cada view
    for view_name, columns in schema["views"].items():
        col_descriptions = []
        for col in columns:
            desc = f"{col['name']} ({col['type']}, {col['nullable']})"
            col_descriptions.append(desc)
        structure_desc = f"A view {view_name} possui as seguintes colunas: {', '.join(col_descriptions)}."
        
        # Pergunta sobre a descrição da view
        dataset_entries.append({
            "messages": [
                {"role": "user", "content": f"Descreva a estrutura da view {view_name}."},
                {"role": "assistant", "content": structure_desc}
            ]
        })
        # Pergunta sobre quais colunas existem
        column_names = ", ".join([col['name'] for col in columns])
        dataset_entries.append({
            "messages": [
                {"role": "user", "content": f"Quais colunas existem na view {view_name}?"},
                {"role": "assistant", "content": f"A view {view_name} contém as colunas: {column_names}."}
            ]
        })
        
    logging.info(f"Geradas {len(dataset_entries)} entradas para o dataset de fine-tuning.")
    return dataset_entries

def save_dataset_to_jsonl(dataset, filename):
    """Salva o dataset formatado em um arquivo JSON Lines."""
    try:
        # Garante que o diretório de dados exista
        import os
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        with open(filename, 'w', encoding='utf-8') as f:
            for entry in dataset:
                json.dump(entry, f, ensure_ascii=False) # ensure_ascii=False para UTF-8 correto
                f.write('\n')
        logging.info(f"Dataset salvo com sucesso em {filename}")
    except IOError as e:
        logging.error(f"Erro ao salvar o dataset em {filename}: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"Erro inesperado ao salvar o dataset: {e}", exc_info=True)

# --- Execução Principal ---
if __name__ == "__main__":
    connection = get_firebird_connection()
    if connection:
        schema_data = get_schema_metadata(connection)
        if schema_data["tables"] or schema_data["views"]:
            formatted_dataset = format_schema_for_finetuning(schema_data)
            if formatted_dataset:
                save_dataset_to_jsonl(formatted_dataset, OUTPUT_FILE)
            else:
                logging.warning("Nenhum dado formatado para salvar.")
        else:
            logging.warning("Nenhuma tabela ou view de usuário encontrada no banco de dados. O dataset não será gerado.")
    else:
        logging.error("Não foi possível conectar ao banco. Saindo.")
        sys.exit(1)

    print(f"\nProcesso concluído. Verifique o arquivo {OUTPUT_FILE}") 