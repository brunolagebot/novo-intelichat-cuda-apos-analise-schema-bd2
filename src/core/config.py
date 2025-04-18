# Arquivo de configurações e constantes 

# --- Caminhos de Arquivos ---
METADATA_FILE = 'data/schema_metadata.json'
TECHNICAL_SCHEMA_FILE = 'data/combined_schema_details.json' # Fallback schema without embeddings
EMBEDDED_SCHEMA_FILE = 'data/schema_with_embeddings.json' # Schema WITH embeddings
OVERVIEW_COUNTS_FILE = 'data/overview_counts.json'
OUTPUT_COMBINED_FILE = 'data/combined_schema_details.json' # Usado para mensagem no merge
CHAT_HISTORY_FILE = 'data/chat_history.json'
CHAT_FEEDBACK_FILE = 'data/chat_feedback.json'

# --- Configurações Padrão de Conexão DB ---
DEFAULT_DB_PATH = r"C:\Projetos\DADOS.FDB" # Use raw string
DEFAULT_DB_USER = "SYSDBA"
# A SENHA NÃO FICA AQUI! É gerenciada via st.secrets ou variável de ambiente FIREBIRD_PASSWORD
DEFAULT_DB_CHARSET = "WIN1252"

# --- Dicionário de Explicações de Tipos SQL ---
TYPE_EXPLANATIONS = {
    "INTEGER": "Número inteiro (sem casas decimais).",
    "VARCHAR": "Texto de tamanho variável.",
    "CHAR": "Texto de tamanho fixo.",
    "DATE": "Data (ano, mês, dia).",
    "TIMESTAMP": "Data e hora.",
    "BLOB": "Dados binários grandes (ex: imagem, texto longo).",
    "SMALLINT": "Número inteiro pequeno.",
    "BIGINT": "Número inteiro grande.",
    "FLOAT": "Número de ponto flutuante (aproximado).",
    "DOUBLE PRECISION": "Número de ponto flutuante com maior precisão.",
    "NUMERIC": "Número decimal exato (precisão definida).",
    "DECIMAL": "Número decimal exato (precisão definida).",
    "TIME": "Hora (hora, minuto, segundo)."
}

# --- Constantes FAISS ---
FAISS_INDEX_FILE = 'data/faiss_column_index.idx'
EMBEDDING_DIMENSION = 768 # Ajuste conforme a dimensão do seu modelo ('nomic-embed-text' usa 768)

# --- Constantes de UI / App ---
AUTO_SAVE_INTERVAL_SECONDS = 300 # 5 minutos 