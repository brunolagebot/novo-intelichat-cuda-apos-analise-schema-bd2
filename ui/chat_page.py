# Código da interface e lógica para o modo 'Chat com Schema'

import streamlit as st
import logging
import uuid
import time

# Importações de módulos core e utils
import src.core.config as config
from src.utils.json_helpers import load_json, save_json
# Importações para busca semântica (se reativada)
from src.core.ai_integration import find_similar_columns, get_query_embedding
import numpy as np

logger = logging.getLogger(__name__)

def display_chat_page(OLLAMA_AVAILABLE, chat_completion, OLLAMA_EMBEDDING_AVAILABLE, get_embedding, technical_schema_data, metadata_dict):
    """Renderiza a página de Chat com Schema."""

    st.header("💬 Chat com Schema")
    st.caption("Faça perguntas sobre o schema documentado. O assistente usará os metadados como contexto.")

    if not OLLAMA_AVAILABLE:
        st.error("Funcionalidade de Chat indisponível. Integração Ollama não carregada.")
        return # Sai da função se Ollama não estiver disponível

    # Inicializa histórico e feedback
    if "messages" not in st.session_state:
        st.session_state.messages = load_json(config.CHAT_HISTORY_FILE, [])
    if "feedback_log" not in st.session_state:
        st.session_state.feedback_log = load_json(config.CHAT_FEEDBACK_FILE, [])
        st.session_state.feedback_ids = {fb['message_id'] for fb in st.session_state.feedback_log}

    # Exibe mensagens do histórico
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            # Botões de feedback para mensagens do assistente
            if message["role"] == "assistant":
                message_id = message.get("message_id")
                if message_id:
                    feedback_given = message_id in st.session_state.get('feedback_ids', set())
                    fb_cols = st.columns(3)
                    ratings = ["Bom", "Médio", "Ruim"]
                    icons = ["👍", "🤔", "👎"]
                    for i, rating in enumerate(ratings):
                        with fb_cols[i]:
                            button_key = f"feedback_{message_id}_{rating}"
                            if st.button(icons[i], key=button_key, help=rating, disabled=feedback_given, use_container_width=True):
                                if not feedback_given:
                                    new_feedback = {"message_id": message_id, "rating": rating, "timestamp": time.time()}
                                    st.session_state.feedback_log.append(new_feedback)
                                    st.session_state.feedback_ids.add(message_id)
                                    if save_json(st.session_state.feedback_log, config.CHAT_FEEDBACK_FILE):
                                        st.toast(f"Feedback '{rating}' registrado!", icon="✍️")
                                    else:
                                        st.toast("Erro ao salvar feedback!", icon="❌")
                                    st.rerun()

    # Input do usuário
    if prompt := st.chat_input("Qual sua dúvida sobre o schema?"):
        user_message_id = str(uuid.uuid4())
        user_message = {"role": "user", "content": prompt, "message_id": user_message_id}
        st.session_state.messages.append(user_message)
        with st.chat_message("user"):
            st.markdown(prompt)

        # Prepara para a resposta do assistente
        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            message_placeholder.markdown("Pensando... 🧠")

            # --- Coleta de Contexto --- #
            context_parts = []
            max_context_tokens = 3000 
            current_context_tokens = 0
            context_limit_reached = False

            # 1. Contexto Global (Usa st.session_state.metadata)
            global_context = st.session_state.metadata.get("_GLOBAL_CONTEXT", "")
            if global_context and not context_limit_reached:
                tokens = len(global_context.split())
                if current_context_tokens + tokens < max_context_tokens:
                     context_parts.append(f"--- Contexto Geral ---\n{global_context}")
                     current_context_tokens += tokens
                else:
                    context_limit_reached = True
                    logger.warning("Limite de contexto atingido ao adicionar Contexto Geral.")

            # 2. Busca por Palavra-chave (Usa technical_schema_data e st.session_state.metadata)
            prompt_lower = prompt.lower()
            for obj_name, obj_data in technical_schema_data.items():
                if context_limit_reached: break
                
                obj_type = obj_data.get('object_type', 'OBJECT')
                obj_meta = st.session_state.metadata.get(obj_type + "S", {}).get(obj_name, {})
                table_context_to_add = []
                table_tokens = 0

                found_table_by_name = obj_name.lower() in prompt_lower
                table_desc = obj_meta.get("description", "").strip()
                
                if found_table_by_name or table_desc:
                    header = f"--- {obj_type.capitalize()}: {obj_name} ---"
                    table_context_to_add.append(header)
                    table_tokens += len(header.split())
                    if table_desc:
                         desc_text = f"Descrição: {table_desc}"
                         table_context_to_add.append(desc_text)
                         table_tokens += len(desc_text.split())

                column_context_parts = []
                column_tokens = 0
                for col_data in obj_data.get('columns', []):
                    if context_limit_reached: break
                    
                    col_name = col_data.get('name')
                    if col_name and col_name.lower() in prompt_lower:
                         col_meta = obj_meta.get("COLUMNS", {}).get(col_name, {})
                         col_desc = col_meta.get("description", "").strip()
                         col_notes = col_meta.get("value_mapping_notes", "").strip()
                         
                         if col_desc or col_notes:
                             col_str_parts = [f"  Coluna: {col_name}"]
                             col_part_tokens = len(col_name.split()) + 2
                             if col_desc: 
                                 desc_text = f"    Descrição: {col_desc}"
                                 col_str_parts.append(desc_text)
                                 col_part_tokens += len(desc_text.split())
                             if col_notes:
                                 notes_text = f"    Notas: {col_notes}"
                                 col_str_parts.append(notes_text)
                                 col_part_tokens += len(notes_text.split())
                             
                             if current_context_tokens + table_tokens + column_tokens + col_part_tokens < max_context_tokens:
                                  column_context_parts.extend(col_str_parts)
                                  column_tokens += col_part_tokens
                             else:
                                  context_limit_reached = True
                                  logger.warning(f"Limite de contexto atingido ao adicionar Coluna '{col_name}'.")
                                  break
                
                if (table_context_to_add or column_context_parts) and not context_limit_reached:
                     if current_context_tokens + table_tokens + column_tokens < max_context_tokens:
                          context_parts.extend(table_context_to_add) 
                          context_parts.extend(column_context_parts)
                          current_context_tokens += table_tokens + column_tokens
                     else:
                          context_limit_reached = True
                          logger.warning(f"Limite de contexto atingido ao adicionar Bloco Tabela '{obj_name}'.")
                elif context_limit_reached:
                    break

            # 3. Busca Semântica (FAISS - Se Habilitado e disponível)
            # A lógica FAISS depende do índice no session_state (construído em ai_integration.py)
            if st.session_state.get('use_embeddings', False) and st.session_state.get('faiss_index') and not context_limit_reached:
                logger.info("Chat: Realizando busca FAISS para contexto adicional.")
                try:
                    # Usa a função get_query_embedding importada
                    query_embedding = get_query_embedding(prompt, OLLAMA_EMBEDDING_AVAILABLE, get_embedding)
                    if query_embedding is not None:
                        similar_cols = find_similar_columns(
                            st.session_state.faiss_index,
                            st.session_state.technical_schema, # Usa schema do estado
                            st.session_state.index_to_key_map, # Usa mapeamento do estado
                            query_embedding,
                            k=5 
                        )
                        if similar_cols:
                            faiss_context = "--- Contexto Similar (Busca Semântica) ---\n"
                            for sim_col in similar_cols:
                                faiss_context += f"Tabela '{sim_col['table']}', Coluna '{sim_col['column']}': {sim_col['description']}\n"
                            tokens_faiss = len(faiss_context.split())
                            if current_context_tokens + tokens_faiss < max_context_tokens:
                                context_parts.append(faiss_context)
                                current_context_tokens += tokens_faiss
                            else:
                                context_limit_reached = True
                                logger.warning("Limite de contexto atingido ao adicionar contexto FAISS.")
                    else:
                         logger.warning("Não foi possível gerar embedding para a query. Busca FAISS pulada.")
                except Exception as e:
                    logger.error(f"Erro durante busca FAISS para chat: {e}")

            # --- Monta o Prompt Final --- #
            final_context = "\n".join(context_parts)
            if not final_context: final_context = "Nenhum contexto relevante encontrado nos metadados."
            
            system_prompt = "Você é um assistente especialista em banco de dados. Responda à pergunta do usuário baseando-se *apenas* e *estritamente* no contexto fornecido sobre o schema. Não invente informações. Se a resposta não estiver no contexto, diga que não encontrou a informação no contexto fornecido."
            user_prompt_for_llm = f"**Contexto do Schema:**\n{final_context}\n\n**Pergunta:**\n{prompt}"
            
            logger.debug(f"Enviando para LLM:\nSystem: {system_prompt}\nUser (parcial): {user_prompt_for_llm[:200]}...")
            
            try:
                # Chama chat_completion passada como argumento
                full_response = chat_completion(
                    messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt_for_llm}],
                    stream=False
                )
                if full_response:
                    message_placeholder.markdown(full_response)
                    assistant_message_id = str(uuid.uuid4())
                    assistant_message = {"role": "assistant", "content": full_response, "message_id": assistant_message_id}
                    st.session_state.messages.append(assistant_message)
                else:
                    fallback_msg = "Desculpe, não consegui obter uma resposta do modelo de IA."
                    message_placeholder.markdown(fallback_msg)
                    assistant_message_id = str(uuid.uuid4())
                    assistant_message = {"role": "assistant", "content": fallback_msg, "message_id": assistant_message_id}
                    st.session_state.messages.append(assistant_message)
                    
                save_json(st.session_state.messages, config.CHAT_HISTORY_FILE)
            
            except Exception as e:
                logger.exception("Erro ao chamar chat_completion no modo Chat com Schema:")
                error_msg = f"Ocorreu um erro ao processar sua pergunta: {e}"
                message_placeholder.markdown(error_msg)
                st.session_state.messages.append({"role": "assistant", "content": error_msg, "message_id": str(uuid.uuid4())})
                save_json(st.session_state.messages, config.CHAT_HISTORY_FILE) 