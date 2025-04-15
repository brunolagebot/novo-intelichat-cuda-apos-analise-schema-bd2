import streamlit as st
import sys
import os

# Adiciona o diretório 'scripts' ao path para importar módulos de lá
sys.path.append(os.path.join(os.path.dirname(__file__), 'scripts'))

# Tenta importar funções dos outros scripts (lidaremos com erros se não carregar)
try:
    # Funções de inferência (ligeiramente adaptadas para Streamlit talvez)
    # from run_inference import load_model_and_tokenizer, generate_response
    # Funções de extração de schema (para o visualizador)
    from extract_schema import get_firebird_connection, get_schema_metadata
    LOAD_SUCCESS = True
except ImportError as e:
    st.error(f"Erro ao importar módulos necessários: {e}\nVerifique se os scripts estão no diretório 'scripts' e se o ambiente está correto.")
    LOAD_SUCCESS = False
except Exception as e:
    st.error(f"Erro inesperado durante a importação: {e}")
    LOAD_SUCCESS = False

# --- Configuração da Página Streamlit ---
st.set_page_config(page_title="Intelichat DB", layout="wide")
st.title("Intelichat com Conhecimento do Banco de Dados")

# --- Carregamento de Recursos (Cache para Eficiência) ---
# Cacheia o carregamento do schema para não buscar no banco toda hora
@st.cache_data(show_spinner="Carregando schema do banco de dados...")
def load_schema():
    if not LOAD_SUCCESS:
        return None
    conn = get_firebird_connection()
    if conn:
        schema = get_schema_metadata(conn)
        return schema
    return None

# # Cacheia o carregamento do modelo e tokenizer (pode consumir muita memória)
# @st.cache_resource(show_spinner="Carregando modelo de IA...")
# def load_language_model():
#     if not LOAD_SUCCESS:
#         return None, None
#     try:
#         # TODO: Ler BASE_MODEL_ID e ADAPTER_PATH do .env ou config
#         BASE_MODEL_ID = os.getenv("BASE_MODEL_NAME", "meta-llama/Meta-Llama-3-8B-Instruct")
#         SCHEMA_ADAPTER_PATH = os.getenv("SCHEMA_ADAPTER_PATH", "./results-llama3-8b-chat-schema-adapter")
#         model, tokenizer = load_model_and_tokenizer(BASE_MODEL_ID, SCHEMA_ADAPTER_PATH)
#         return model, tokenizer
#     except Exception as e:
#         st.error(f"Erro crítico ao carregar o modelo de IA: {e}")
#         return None, None

# --- Estado da Sessão Streamlit ---
# Usado para manter o histórico do chat entre interações
if 'chat_history' not in st.session_state:
    st.session_state['chat_history'] = []

# --- Seleção de Modo (Sidebar ou Abas) ---
modo_selecionado = st.sidebar.selectbox("Selecione o Modo:", ["Chat com IA", "Visualizador de Schema"], key="modo")

# --- Carrega os dados necessários ---
schema_data = load_schema()
# model, tokenizer = load_language_model() # Adiado por enquanto

# --- Lógica Principal Baseada no Modo ---
if not LOAD_SUCCESS:
    st.warning("A aplicação não pôde carregar componentes essenciais. Verifique os erros acima.")

elif modo_selecionado == "Visualizador de Schema":
    st.header("Visualizador de Schema do Banco de Dados")
    if schema_data:
        st.subheader("Tabelas")
        if schema_data.get('tables'):
            for table_name, columns in schema_data['tables'].items():
                with st.expander(f"Tabela: {table_name}"):
                    cols_data = [{"Coluna": col['name'], "Tipo": col['type'], "Nulável": col['nullable']} for col in columns]
                    st.dataframe(cols_data, use_container_width=True)
        else:
            st.write("Nenhuma tabela de usuário encontrada.")

        st.subheader("Views")
        if schema_data.get('views'):
            for view_name, columns in schema_data['views'].items():
                with st.expander(f"View: {view_name}"):
                    cols_data = [{"Coluna": col['name'], "Tipo": col['type'], "Nulável": col['nullable']} for col in columns]
                    st.dataframe(cols_data, use_container_width=True)
        else:
            st.write("Nenhuma view encontrada.")
    else:
        st.error("Não foi possível carregar os dados do schema do banco.")

elif modo_selecionado == "Chat com IA":
    st.header("Chat com IA (Consciente do Schema)")
    
    # Comentado por enquanto - Requer carregamento do modelo
    # if not model or not tokenizer:
    #     st.error("Modelo de IA não carregado. Não é possível iniciar o chat.")
    # else:
    st.warning("Funcionalidade de Chat ainda não implementada nesta versão.")
    st.info("O modelo de IA (Llama 3) precisa ser carregado. Isso pode consumir muita memória.")
    # Placeholder para a interface de chat
    # Exibir histórico
    # for message in st.session_state.chat_history:
    #     with st.chat_message(message["role"]):
    #         st.markdown(message["content"])
    
    # Input do usuário
    # prompt = st.chat_input("Faça uma pergunta sobre o schema ou geral...")
    # if prompt:
    #     st.session_state.chat_history.append({"role": "user", "content": prompt})
    #     with st.chat_message("user"):
    #         st.markdown(prompt)
            
    #     # Gerar resposta da IA
    #     with st.spinner("Pensando..."):
    #         # TODO: Adicionar lógica para talvez incluir schema no prompt?
    #         response = generate_response(model, tokenizer, prompt)
        
    #     st.session_state.chat_history.append({"role": "assistant", "content": response})
    #     with st.chat_message("assistant"):
    #         st.markdown(response) 