# scripts/prepare_finetune_data.py
import sqlite3
import json
import sys
import os
from collections import defaultdict

# Adiciona o diretório raiz ao path para encontrar src.database
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from src.database.history import DB_FILE # Reutiliza a constante
except ImportError:
    print("Erro: Não foi possível importar DB_FILE de src.database.history.")
    print("Certifique-se de que está executando o script da raiz do projeto ou ajuste o path.")
    # Tenta um caminho relativo se a execução for de dentro de scripts/
    try:
        from ..src.database.history import DB_FILE
        print("Importação relativa bem-sucedida.")
    except ImportError:
        print("Falha na importação relativa também. Usando nome de arquivo padrão.")
        DB_FILE = "../chat_history.db" # Supõe que está na raiz

OUTPUT_FILE = "finetune_data.jsonl"
MIN_CONVERSATION_TURNS = 2 # Exigir pelo menos 2 turnos (user+assistant) para considerar

def format_conversation(messages):
    """Formata uma lista de tuplas (user, assistant) no formato SFTTrainer."""
    formatted = []
    for user_msg, assistant_msg in messages:
        # SFTTrainer geralmente espera role/content
        # Verifica se as mensagens não são None ou vazias antes de adicionar
        if user_msg:
            formatted.append({"role": "user", "content": user_msg})
        if assistant_msg: # Só adiciona assistente se houver resposta não vazia
            formatted.append({"role": "assistant", "content": assistant_msg})
    # Retorna None se a formatação resultar em lista vazia (ex: só msg de user sem resposta)
    return {"messages": formatted} if formatted else None

def prepare_data():
    db_path = DB_FILE
    if not os.path.exists(db_path):
        # Tenta encontrar na raiz do projeto se não achar no caminho relativo
        db_path_root = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), os.path.basename(DB_FILE))
        if os.path.exists(db_path_root):
            db_path = db_path_root
        else:
            print(f"ERRO: Arquivo de banco de dados '{DB_FILE}' não encontrado nos caminhos esperados.")
            return
            
    print(f"Lendo dados de '{db_path}'...")
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("""
            SELECT session_id, user_message, assistant_message
            FROM chat_history
            ORDER BY session_id, timestamp ASC
        """)
        rows = cursor.fetchall()
        print(f"Encontradas {len(rows)} interações no total.")

        if not rows:
            print("Nenhum dado encontrado no histórico. Abortando.")
            return

        # Agrupa mensagens por sessão
        sessions = defaultdict(list)
        valid_rows_count = 0
        for row in rows:
            # Ignora linhas onde user ou assistant sejam None ou vazios (pode acontecer por erros)
            if row['session_id'] and row['user_message'] and row['assistant_message']:
                sessions[row['session_id']].append((row['user_message'], row['assistant_message']))
                valid_rows_count += 1
            else:
                 print(f"Linha pulada por dados ausentes: session={row['session_id']}, user={row['user_message']}, assistant={row['assistant_message']}")

        print(f"Agrupado em {len(sessions)} sessões a partir de {valid_rows_count} linhas válidas.")

        # Formata e escreve no arquivo JSONL
        output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), OUTPUT_FILE)
        count = 0
        skipped_sessions = 0
        with open(output_path, 'w', encoding='utf-8') as f:
            for session_id, messages in sessions.items():
                formatted_conv = format_conversation(messages)
                # Verifica se a conversa formatada não é None e tem turnos suficientes
                if formatted_conv and len(formatted_conv['messages']) >= MIN_CONVERSATION_TURNS * 2:
                    f.write(json.dumps(formatted_conv, ensure_ascii=False) + '\n')
                    count += 1
                else:
                    print(f"Sessão {session_id} pulada (formato inválido ou menos de {MIN_CONVERSATION_TURNS} turnos completos).")
                    skipped_sessions += 1
                    
        print(f"Dados preparados e salvos em '{output_path}'.")
        print(f"Total de {count} conversas formatadas e salvas.")
        if skipped_sessions > 0:
            print(f"{skipped_sessions} sessões foram puladas por não atenderem aos critérios.")

    except sqlite3.Error as e:
        print(f"Erro ao acessar o banco de dados: {e}")
    except Exception as e:
        print(f"Erro inesperado durante a preparação dos dados: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    prepare_data() 