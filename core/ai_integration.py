# Funções de integração com IA (Ollama, FAISS)

import logging
import streamlit as st # Necessário para spinners, toasts, session_state, etc.
import numpy as np
import faiss
import os # Necessário para handle_embedding_toggle verificar arquivo

import core.config as config
# TODO: Verificar se essas importações circulares/cruzadas serão um problema
# Talvez load_technical_schema deva estar em data_loader e as outras duas em analysis?
# from core.data_loader import load_technical_schema # REMOVIDA importação de nível superior
from core.analysis import analyze_key_structure

logger = logging.getLogger(__name__)

# --- Funções Ollama (requerem client) --- #
# Assumimos que OLLAMA_AVAILABLE, OLLAMA_EMBEDDING_AVAILABLE, chat_completion, get_embedding são definidos
# no escopo que chama essas funções (atualmente streamlit_app.py)
# TODO: Passar essas variáveis/funções como argumentos ou encontrar forma melhor?

def generate_ai_description(prompt, OLLAMA_AVAILABLE, chat_completion):
    """Chama a API Ollama para gerar uma descrição e limpa a resposta."""
    if not OLLAMA_AVAILABLE:
        st.warning("Funcionalidade de IA não disponível.") # UI Tightly coupled
        return None
        
    logger.debug(f"Enviando prompt para IA: {prompt}")
    messages = [{"role": "user", "content": prompt}]
    try:
        # UI Tightly coupled
        with st.spinner("🧠 Pensando..."):
            response = chat_completion(messages=messages, stream=False)
        if response:
            cleaned_response = response.strip().strip('"').strip('\'').strip()
            logger.debug(f"Resposta da IA (limpa): {cleaned_response}")
            return cleaned_response
        else:
            logger.warning("Falha ao obter descrição da IA (resposta vazia).")
            st.toast("😕 A IA não retornou uma sugestão.") # UI Tightly coupled
            return None
    except Exception as e:
        logger.exception("Erro ao chamar a API Ollama:")
        st.error(f"Erro ao contatar a IA: {e}") # UI Tightly coupled
        return None

def get_query_embedding(text: str, OLLAMA_EMBEDDING_AVAILABLE, get_embedding) -> np.ndarray | None:
    """Gera embedding para um texto usando a função Ollama e trata erros."""
    if not OLLAMA_EMBEDDING_AVAILABLE:
        logger.warning("Tentativa de gerar embedding sem função disponível.")
        return None
    try:
        # UI Tightly coupled
        with st.spinner("Gerando embedding para a pergunta..."): 
            embedding_list = get_embedding(text) # Chama a função externa
        
        if embedding_list and isinstance(embedding_list, list):
            embedding_np = np.array(embedding_list).astype('float32')
            if embedding_np.shape[0] == config.EMBEDDING_DIMENSION:
                logger.info(f"Embedding gerado para a query (Shape: {embedding_np.shape})")
                return embedding_np
            else:
                logger.error(f"Erro: Dimensão do embedding da query ({embedding_np.shape[0]}) diferente da esperada ({config.EMBEDDING_DIMENSION}).")
                # UI Tightly coupled
                st.toast(f"Erro na dimensão do embedding gerado pela IA ({embedding_np.shape[0]} vs {config.EMBEDDING_DIMENSION}).", icon="❌")
                return None
        else:
            logger.error(f"Função get_embedding não retornou uma lista válida: {type(embedding_list)}")
            # UI Tightly coupled
            st.toast("Erro ao gerar embedding da pergunta (resposta inválida da IA).", icon="❌")
            return None
    except Exception as e:
        logger.exception("Erro ao chamar get_embedding:")
        # UI Tightly coupled
        st.toast(f"Erro ao gerar embedding da pergunta: {e}", icon="❌")
        return None

# --- Funções FAISS --- #

@st.cache_resource # Cache do índice FAISS para performance
def build_faiss_index(schema_data):
    logger.info("---> EXECUTANDO build_faiss_index") # Log de diagnóstico
    """Constrói um índice FAISS a partir dos embeddings das colunas no schema_data."""
    embeddings = []
    index_to_key = [] # Mapeia o índice interno do FAISS para (table_name, col_index)

    logger.info("Construindo índice FAISS a partir dos embeddings...")
    items_with_embeddings = 0
    items_without_embeddings = 0

    for obj_name, obj_data in schema_data.items():
        if isinstance(obj_data, dict) and 'columns' in obj_data:
            for i, col_data in enumerate(obj_data['columns']):
                embedding = col_data.get('embedding')
                if embedding and isinstance(embedding, list) and len(embedding) == config.EMBEDDING_DIMENSION:
                    embeddings.append(embedding)
                    index_to_key.append((obj_name, i))
                    items_with_embeddings += 1
                else:
                    items_without_embeddings += 1

    if not embeddings:
        logger.warning("Nenhum embedding válido encontrado para construir o índice FAISS.")
        return None, []

    embeddings_np = np.array(embeddings).astype('float32')
    dimension = embeddings_np.shape[1]
    if dimension != config.EMBEDDING_DIMENSION:
        logger.warning(f"Dimensão dos embeddings ({dimension}) difere da esperada ({config.EMBEDDING_DIMENSION}). Ajuste EMBEDDING_DIMENSION.")

    index = faiss.IndexFlatL2(dimension)
    index.add(embeddings_np)

    logger.info(f"Índice FAISS construído com {index.ntotal} vetores. {items_without_embeddings} colunas ignoradas por falta de embedding.")

    # Opcional: Salvar índice
    # try:
    #     faiss.write_index(index, config.FAISS_INDEX_FILE)
    #     logger.info(f"Índice FAISS salvo em {config.FAISS_INDEX_FILE}")
    # except Exception as e:
    #     logger.error(f"Erro ao salvar índice FAISS: {e}")

    return index, index_to_key

def find_similar_columns(faiss_index, schema_data, index_to_key_map, target_embedding, k=5):
    """Busca as k colunas mais similares no índice FAISS que possuem descrição."""
    if faiss_index is None or not isinstance(target_embedding, np.ndarray):
        return []

    target_embedding_np = target_embedding.astype('float32').reshape(1, -1)
    try:
        distances, indices = faiss_index.search(target_embedding_np, k + 1)
    except Exception as e:
        logger.error(f"Erro durante a busca FAISS: {e}")
        return []

    similar_columns = []
    for i in range(1, len(indices[0])): # Pula o primeiro resultado (ele mesmo)
        idx = indices[0][i]
        if idx == -1:
            continue

        try:
            table_name, col_index = index_to_key_map[idx]
            # Acessa schema_data (que deveria ser o schema técnico com descrições de negócio)
            column_data = schema_data.get(table_name, {}).get('columns', [])[col_index]
            col_name = column_data.get('name', 'N/A')
            # Tenta buscar 'business_description', se não houver, usa 'description' técnica
            description = column_data.get('business_description', '').strip() or column_data.get('description', '').strip()

            if description: # Adiciona apenas se tiver alguma descrição
                similar_columns.append({
                    'table': table_name,
                    'column': col_name,
                    'description': description,
                    'distance': float(distances[0][i])
                })
                if len(similar_columns) == k:
                    break
        except IndexError:
            logger.warning(f"Índice FAISS {idx} fora dos limites do mapeamento index_to_key_map.")
            continue
        except Exception as e:
            logger.error(f"Erro ao processar resultado FAISS com índice {idx}: {e}")
            continue

    return similar_columns

# --- Função de Callback para Toggle de Embeddings --- #
# TODO: Desacoplar do Streamlit e da lógica de carregamento/análise?

def handle_embedding_toggle():
    """Callback para o toggle 'Usar Embeddings'. Carrega/descarrega o schema com embeddings."""
    # Importa a função necessária AQUI dentro
    from core.data_loader import load_technical_schema
    
    use_embeddings = st.session_state.get('use_embeddings', False)
    logger.info(f"Toggle 'Usar Embeddings' mudou para: {use_embeddings}")
    st.spinner_text = "Atualizando schema e estruturas..."
    with st.spinner(st.spinner_text):
        if use_embeddings:
            logger.info(f"Tentando carregar schema com embeddings de: {config.EMBEDDED_SCHEMA_FILE}")
            schema_embedded = load_technical_schema(config.EMBEDDED_SCHEMA_FILE)
            if schema_embedded:
                st.session_state.technical_schema = schema_embedded
                logger.info("Schema com embeddings carregado.")
                build_faiss_index.clear()
                analyze_key_structure.clear()
                logger.info("Caches FAISS e Análise de Chaves limpos.")
                # Chama funções que (por enquanto) estão neste módulo ou em analysis
                st.session_state.faiss_index, st.session_state.index_to_key_map = build_faiss_index(st.session_state.technical_schema)
                st.session_state.key_analysis = analyze_key_structure(st.session_state.technical_schema)
                logger.info("Índice FAISS e Análise de Chaves reconstruídos com embeddings.")
                st.toast("Schema com embeddings carregado e estruturas atualizadas.", icon="✅")
            else:
                logger.error(f"Falha ao carregar schema com embeddings de {config.EMBEDDED_SCHEMA_FILE}.")
                st.error(f"Erro ao carregar {config.EMBEDDED_SCHEMA_FILE}. Verifique os arquivo e os logs. Revertendo para schema base.", icon="❌")
                st.session_state.use_embeddings = False
                st.session_state.technical_schema = load_technical_schema(config.TECHNICAL_SCHEMA_FILE)
                build_faiss_index.clear()
                analyze_key_structure.clear()
                st.session_state.faiss_index, st.session_state.index_to_key_map = build_faiss_index(st.session_state.technical_schema)
                st.session_state.key_analysis = analyze_key_structure(st.session_state.technical_schema)
        else:
            logger.info(f"Carregando schema base de: {config.TECHNICAL_SCHEMA_FILE}")
            schema_base = load_technical_schema(config.TECHNICAL_SCHEMA_FILE)
            if schema_base:
                 st.session_state.technical_schema = schema_base
                 logger.info("Schema base carregado.")
                 build_faiss_index.clear()
                 analyze_key_structure.clear()
                 logger.info("Caches FAISS e Análise de Chaves limpos.")
                 st.session_state.faiss_index, st.session_state.index_to_key_map = build_faiss_index(st.session_state.technical_schema)
                 st.session_state.key_analysis = analyze_key_structure(st.session_state.technical_schema)
                 logger.info("Índice FAISS e Análise de Chaves reconstruídos com schema base.")
                 st.toast("Usando schema base. Busca por similaridade desativada/limitada.", icon="ℹ️")
            else:
                 logger.critical("Falha crítica ao recarregar o schema base! O app pode ficar instável.")
                 st.error("Erro GRAVE ao recarregar o schema base. Verifique os logs.", icon="🚨") 