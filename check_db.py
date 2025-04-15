import sqlite3
from src.database.history import DB_FILE # Reutiliza o nome do arquivo definido

def read_history():
    """Lê e exibe todas as entradas da tabela chat_history."""
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row # Para acessar colunas por nome
        cursor = conn.cursor()

        cursor.execute("SELECT id, session_id, timestamp, user_message, assistant_message, feedback FROM chat_history ORDER BY timestamp DESC")
        rows = cursor.fetchall()

        if not rows:
            print(f"A tabela 'chat_history' no banco '{DB_FILE}' está vazia.")
            return

        print(f"--- Conteúdo de '{DB_FILE}'.'chat_history' ({len(rows)} entradas) ---")
        for row in rows:
            print("-" * 20)
            print(f"ID: {row['id']}")
            print(f"Session ID: {row['session_id']}")
            print(f"Timestamp: {row['timestamp']}")
            print(f"Usuário: {row['user_message']}")
            print(f"Assistente: {row['assistant_message']}")
            print(f"Feedback: {row['feedback']}")
        print("-" * 20)

    except sqlite3.Error as e:
        print(f"Erro ao ler o banco de dados {DB_FILE}: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    read_history() 