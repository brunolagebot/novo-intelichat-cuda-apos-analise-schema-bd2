import sys
import subprocess
import os
import uuid
import time
import platform

# --- Verificação do Ambiente Virtual ---
def check_venv():
    """Verifica se estamos em um ambiente virtual e se é o correto."""
    # Verifica se está em qualquer ambiente virtual
    in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    if not in_venv:
        print("ERRO: Este script deve ser executado dentro do ambiente virtual.")
        print("\nPara configurar o ambiente:")
        print("1. Execute: python setup_env.py")
        print("2. Ative o ambiente:")
        if platform.system() == "Windows":
            print("   .venv\\Scripts\\activate")
        else:
            print("   source .venv/bin/activate")
        print("3. Execute novamente: python app.py")
        sys.exit(1)
    
    # Verifica se é o ambiente virtual correto (deve estar na pasta .venv)
    expected_prefix = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.venv')
    actual_prefix = sys.prefix
    
    if platform.system() == "Windows":
        expected_prefix = expected_prefix.lower()
        actual_prefix = actual_prefix.lower()
    
    if not actual_prefix.startswith(expected_prefix):
        print("ERRO: Ambiente virtual incorreto.")
        print(f"Esperado: {expected_prefix}")
        print(f"Atual: {actual_prefix}")
        print("\nPor favor, use o ambiente virtual da pasta do projeto.")
        sys.exit(1)

# Verifica o ambiente virtual antes de qualquer outra operação
check_venv()

# --- Verificação de Ambiente e Instalação de Dependências --- 
def check_and_install_dependencies():
    """Verifica se está em um venv e instala dependências do requirements.txt."""
    print("--- Verificando ambiente e dependências ---")
    
    # Verifica se o arquivo requirements.txt existe
    requirements_path = 'requirements.txt'
    if not os.path.exists(requirements_path):
        print(f"ERRO: Arquivo {requirements_path} não encontrado.")
        return False

    print(f"Garantindo que as dependências em {requirements_path} estão instaladas...")
    try:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-r', requirements_path])
        print("Dependências verificadas/instaladas com sucesso.")
        print("---------------------------------------------")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERRO: Falha ao instalar dependências: {e}")
        print("Verifique se o pip está funcionando e se o arquivo requirements.txt está correto.")
        print("---------------------------------------------")
        return False
    except FileNotFoundError:
        print("ERRO: Comando 'pip' não encontrado.")
        print("Certifique-se de que Python e pip estão instalados e no PATH.")
        print("---------------------------------------------")
        return False

# Executa a verificação ANTES de tentar importar pacotes instalados
if not check_and_install_dependencies():
    sys.exit(1)

# --- Imports e Lógica Principal do App --- 
# Só importa os pacotes DEPOIS de garantir a instalação
import gradio as gr
from src.ollama_integration.client import chat_completion, get_available_models
from src.database.history import save_chat_message, update_feedback
from typing import List, Tuple, Dict, Any, Generator
from src.core.processing import preprocess_user_input # Importa a função

# Busca a lista de modelos ANTES de definir a interface
available_models = get_available_models()
# Obtém o modelo padrão do .env para pré-selecionar no dropdown
default_model = os.getenv("OLLAMA_DEFAULT_MODEL", "llama3")
# Garante que o default_model esteja na lista, caso contrário usa o primeiro da lista
if default_model not in available_models and available_models:
    default_model_selected = available_models[0]
    print(f"AVISO: Modelo padrão '{default_model}' não encontrado. Usando '{default_model_selected}' como padrão na UI.")
elif not available_models:
     # Caso extremo: nenhum modelo encontrado, define um fallback
     default_model_selected = "[Nenhum modelo encontrado]"
     available_models = [default_model_selected]
else:
    default_model_selected = default_model

# Função principal que processa a entrada e gera a resposta
def respond(
    message: str,
    chat_history: List[Tuple[str | None, str | None]],
    selected_model: str,
    session_state: Dict[str, Any]
) -> Generator[Tuple[List[Tuple[str | None, str | None]], Dict[str, Any], str], None, None]:
    """Processa a mensagem do usuário (com pré-processamento), chama o LLM, atualiza o histórico e mostra o tempo.

    Args:
        message: Mensagem atual do usuário.
        chat_history: Histórico atual do componente Chatbot.
        selected_model: Modelo Ollama selecionado.
        session_state: Dicionário de estado da sessão.

    Yields:
        Tupla com (histórico atualizado, estado atualizado, string de tempo).
    """
    start_time = time.time()
    time_str = ""

    # Pré-processa a mensagem do usuário AQUI!
    processed_message = preprocess_user_input(message)
    if not processed_message:
        # Se a mensagem ficar vazia após limpeza, não faz nada
        # Apenas retorna o estado atual sem chamar LLM ou salvar
        yield chat_history, session_state, "(Mensagem vazia após limpeza)"
        return

    # Garante/Obtém session_id e inicializa last_message_id se necessário
    if "session_id" not in session_state:
        session_state["session_id"] = str(uuid.uuid4())
        session_state["last_db_message_id"] = None # Inicializa ID
    session_id = session_state["session_id"]

    # Formata mensagens para a API
    messages = []
    for user_msg, assistant_msg in chat_history:
        if user_msg:
             messages.append({"role": "user", "content": user_msg})
        if assistant_msg:
             messages.append({"role": "assistant", "content": assistant_msg})
    # Adiciona a mensagem PROCESSADA à lista para a API
    messages.append({"role": "user", "content": processed_message})

    # Zera o ID da última mensagem antes de gerar nova resposta
    session_state["last_db_message_id"] = None

    # Adiciona a mensagem PROCESSADA ao histórico da UI
    # (Mostra ao usuário a mensagem como ele verá após a limpeza)
    chat_history.append((processed_message, None))
    yield chat_history, session_state, time_str

    # Chama o LLM com a mensagem processada (implícito, pois está em `messages`)
    response_generator = chat_completion(messages=messages, model=selected_model, stream=True)
    full_response = ""

    try:
        if response_generator:
            for chunk in response_generator:
                full_response += chunk
                # Atualiza a última mensagem usando a processed_message como chave
                chat_history[-1] = (processed_message, full_response)
                yield chat_history, session_state, time_str
        else:
            full_response = "Desculpe, ocorreu um erro ao contatar o modelo."
            chat_history[-1] = (processed_message, full_response)
            yield chat_history, session_state, time_str
    finally:
        end_time = time.time()
        duration = end_time - start_time
        time_str = f"Tempo de resposta: {duration:.2f}s"
        print(time_str)

        # Salva no banco de dados e guarda o ID
        saved_id = None
        if full_response and full_response != "Desculpe, ocorreu um erro ao contatar o modelo.":
             saved_id = save_chat_message(user_message=processed_message, assistant_message=full_response, session_id=session_id)
        
        # Armazena o ID da mensagem salva no estado da sessão
        session_state["last_db_message_id"] = saved_id

    yield chat_history, session_state, time_str

# --- Nova Função para Lidar com Feedback --- 
def handle_feedback(feedback_type: str, session_state: Dict[str, Any]) -> None:
    """Atualiza o feedback no banco de dados para a última mensagem salva."""
    last_message_id = session_state.get("last_db_message_id")
    feedback_value = 1 if feedback_type == "👍" else -1 if feedback_type == "👎" else 0

    if last_message_id is not None and feedback_value != 0:
        print(f"Registrando feedback {feedback_type} para a mensagem ID: {last_message_id}")
        update_feedback(message_id=last_message_id, feedback_value=feedback_value)
        # Poderia adicionar um gr.Info ou gr.Warning aqui para confirmar ao usuário
        # Ex: gr.Info(f"Feedback {feedback_type} registrado!") - mas requer retorno
    elif feedback_value == 0:
        print("Tipo de feedback inválido recebido.")
    else:
        print("Nenhuma mensagem anterior encontrada nesta sessão para registrar feedback.")

# --- Definição da Interface com gr.Blocks --- 
with gr.Blocks(theme=gr.themes.Default(primary_hue="blue", secondary_hue="neutral")) as demo:
    # Estado da sessão (para session_id)
    session_state = gr.State({})

    gr.Markdown("# Meu Chatbot com Ollama")

    # Seletor de Modelo (acima do chat)
    model_selector = gr.Dropdown(
        choices=available_models,
        value=default_model_selected,
        label="Escolha o Modelo Ollama",
        interactive=True
    )

    # Área do Chat
    chatbot = gr.Chatbot(
        label="Chat",
        bubble_full_width=False,
        height=500 # Ajuste a altura conforme necessário
    )

    # Adiciona componente para exibir o tempo
    time_output = gr.Markdown("")

    # Adiciona linha para botões de feedback
    with gr.Row() as feedback_row:
        feedback_label = gr.Markdown("Feedback da última resposta:", visible=True) # Ou False inicialmente
        thumb_up_btn = gr.Button("👍")
        thumb_down_btn = gr.Button("👎")

    # Área de Input
    with gr.Row():
        msg_input = gr.Textbox(
            scale=4,
            show_label=False,
            placeholder="Digite sua mensagem aqui...",
            container=False,
        )
        send_button = gr.Button("Enviar", scale=1)

    # Ações de Limpeza (Opcional)
    # clear_button = gr.ClearButton([msg_input, chatbot])

    # --- Conexão dos Eventos --- 

    # Função para limpar APENAS o input após envio
    def clear_message_input_only():
        return ""

    # Quando o usuário pressiona Enter no Textbox (msg_input)
    msg_input.submit(
        respond, # Função a ser chamada
        [msg_input, chatbot, model_selector, session_state], # Inputs da função
        # Adiciona time_output aos outputs
        [chatbot, session_state, time_output], # Outputs da função (atualiza o chatbot e o state)
        queue=True # Permite processamento em fila
    # Limpa APENAS msg_input após a resposta
    ).then(clear_message_input_only, [], [msg_input])

    # Quando o usuário clica no botão Enviar
    send_button.click(
        respond,
        [msg_input, chatbot, model_selector, session_state],
        [chatbot, session_state, time_output],
        queue=True
    # Limpa APENAS msg_input após a resposta
    ).then(clear_message_input_only, [], [msg_input])

    # Conecta botões de feedback à função handle_feedback
    thumb_up_btn.click(
        handle_feedback,
        inputs=[gr.Textbox("👍", visible=False), session_state], # Passa tipo de feedback escondido
        outputs=None # Não atualiza a UI diretamente, só o BD
    )
    thumb_down_btn.click(
        handle_feedback,
        inputs=[gr.Textbox("👎", visible=False), session_state],
        outputs=None
    )

# Lança a aplicação web
if __name__ == "__main__":
    demo.launch(share=False) 