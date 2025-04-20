# Arquivo de configurações e constantes 

# --- Caminhos de Arquivos ---
LOG_FILE = 'data/logs/app.log' # Caminho do arquivo de log
METADATA_FILE = 'data/metadata/metadata_schema_manual.json'
TECHNICAL_SCHEMA_FILE = 'data/metadata/technical_schema_from_db.json'
# AI_DESCRIPTIONS_FILE = 'data/metadata/ai_generated_descriptions_openai_35turbo.json' # Comentado: Gerenciado via args nos scripts
EMBEDDED_SCHEMA_FILE = 'data/embeddings/schema_with_embeddings.json' # Atualizado: Nome simplificado
OVERVIEW_COUNTS_FILE = 'data/metadata/overview_counts.json' # Contagem de linhas cacheada
# OUTPUT_COMBINED_FILE removido - Usar MERGED_SCHEMA_FOR_EMBEDDINGS_FILE ou carregar separadamente na UI
MERGED_SCHEMA_FOR_EMBEDDINGS_FILE = 'data/processed/schema_enriched_for_embedding.json' # Schema final mesclado, pronto para embeddings
CHAT_HISTORY_FILE = 'data/chat/chat_history.json'
CHAT_FEEDBACK_FILE = 'data/chat/chat_feedback.json'
KEY_ANALYSIS_RESULTS_FILE = 'data/analysis/key_analysis_results.json' # NOVO: Resultados da análise de chaves

# --- Configurações Padrão de Conexão DB ---
DEFAULT_DB_PATH = r"C:\\Projetos\\DADOS.FDB" # Use raw string
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
FAISS_INDEX_FILE = 'data/embeddings/faiss_index.idx' # Atualizado: Nome simplificado
EMBEDDING_DIMENSION = 768 # Ajuste conforme a dimensão do seu modelo ('nomic-embed-text' usa 768)

# --- Constantes de UI / App ---
AUTO_SAVE_INTERVAL_SECONDS = 300 # 5 minutos 