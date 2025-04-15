import sqlite3
import logging
import os
from datetime import datetime

# Define o nome do arquivo do banco de dados
DB_FILE = "chat_history.db"

def get_db_connection():
    """Estabelece conexão com o banco de dados SQLite."""
    conn = None
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row # Retorna linhas como dicionários
        logging.info(f"Conexão com o banco de dados {DB_FILE} estabelecida.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Erro ao conectar ao banco de dados {DB_FILE}: {e}")
        return None

def init_db():
    """Inicializa o BD, adicionando a coluna feedback se necessário."""
    conn = get_db_connection()
    if conn is None: return

    try:
        cursor = conn.cursor()
        # Cria a tabela se não existir (já inclui a coluna nova)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS chat_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT, -- Para agrupar mensagens de uma mesma sessão (opcional)
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                user_message TEXT NOT NULL,
                assistant_message TEXT NOT NULL,
                feedback INTEGER DEFAULT NULL
            )
        """)
        # Tenta adicionar a coluna 'feedback' se ela não existir (para bancos antigos)
        try:
            cursor.execute("ALTER TABLE chat_history ADD COLUMN feedback INTEGER DEFAULT NULL")
            logging.info("Coluna 'feedback' adicionada à tabela 'chat_history'.")
        except sqlite3.OperationalError as e:
            # Ignora o erro se a coluna já existir
            if "duplicate column name" in str(e):
                pass # Coluna já existe, tudo bem
            else:
                raise # Levanta outros erros de alteração
        conn.commit()
        logging.info("Tabela 'chat_history' verificada/atualizada com sucesso.")
    except sqlite3.Error as e:
        logging.error(f"Erro ao inicializar/atualizar a tabela 'chat_history': {e}")
    finally:
        if conn: conn.close()

def save_chat_message(user_message: str, assistant_message: str, session_id: str | None = None) -> int | None:
    """Salva uma interação de chat no BD e retorna o ID da linha inserida."""
    conn = get_db_connection()
    if conn is None: return None

    sql = ''' INSERT INTO chat_history(user_message, assistant_message, session_id)
              VALUES(?,?,?) '''
    last_id = None
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (user_message, assistant_message, session_id))
        conn.commit()
        last_id = cursor.lastrowid # Obtém o ID da última linha inserida
        logging.info(f"Mensagem salva no histórico (ID: {last_id})")
    except sqlite3.Error as e:
        logging.error(f"Erro ao salvar mensagem no histórico: {e}")
    finally:
        if conn: conn.close()
    return last_id # Retorna o ID

def update_feedback(message_id: int, feedback_value: int):
    """Atualiza o feedback para uma mensagem específica."""
    if message_id is None or feedback_value not in [1, -1]:
        logging.warning(f"Tentativa de atualizar feedback com ID inválido ({message_id}) ou valor ({feedback_value})")
        return

    conn = get_db_connection()
    if conn is None: return

    sql = ''' UPDATE chat_history
              SET feedback = ?
              WHERE id = ? '''
    try:
        cursor = conn.cursor()
        cursor.execute(sql, (feedback_value, message_id))
        conn.commit()
        logging.info(f"Feedback ({feedback_value}) atualizado para a mensagem ID: {message_id}")
    except sqlite3.Error as e:
        logging.error(f"Erro ao atualizar feedback para mensagem ID {message_id}: {e}")
    finally:
        if conn: conn.close()

# Chama init_db quando o módulo é importado para garantir que a tabela exista
init_db() 