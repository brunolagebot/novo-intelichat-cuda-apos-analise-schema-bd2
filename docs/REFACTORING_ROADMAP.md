# Roteiro: Desacoplamento da UI em `core/ai_integration.py`

Este roteiro acompanha o processo de refatoração do módulo `core/ai_integration.py` para remover dependências diretas da interface Streamlit (`st.*`), tornando-o mais testável e reutilizável.

- [x] **Etapa 1: Refatorar `generate_ai_description`**
    - Remover chamadas `st.warning`, `st.spinner`, `st.toast`, `st.error`.
    - Ajustar a função para retornar a descrição (string) ou `None` em caso de falha.
    - *Status: Concluído.*
    - *Próximo passo relacionado:* Ajustar o código que chama `generate_ai_description` (em `streamlit_app.py`) para usar `st.spinner`, tratar o retorno e mostrar `st.toast`/`st.error`. *(A fazer)*

- [x] **Etapa 2: Refatorar `get_query_embedding`**
    - Remover chamadas `st.spinner` e `st.toast`.
    - Ajustar a função para retornar o embedding (numpy array) ou `None`. Adicionar comentários sobre exceções.
    - *Status: Concluído.*
    - *Próximo passo relacionado:* Ajustar o código que chama `get_query_embedding` (em `streamlit_app.py`) para usar `st.spinner`, tratar o retorno/exceções e mostrar `st.toast`/`st.error`. *(A fazer)*

- [ ] **Etapa 3: Analisar `build_faiss_index`**
    - Verificar se há chamadas diretas a `st.*` *além* do decorador `@st.cache_resource`.
    - Decidir sobre `@st.cache_resource`: Manter por enquanto (ligado ao gerenciamento de estado/performance do Streamlit). Documentar dependência.
    - *Status: Pendente.*

- [ ] **Etapa 4: Analisar `find_similar_columns`**
    - Verificar se a função está realmente desacoplada (não parece ter chamadas `st.*`).
    - *Status: Pendente.*

- [ ] **Etapa 5: Analisar `handle_embedding_toggle`**
    - Reconhecer acoplamento inerente (callback de UI).
    - Avaliar se chamadas `st.*` (spinner, toast, error) devem ser removidas *desta função específica*.
    - Focar em garantir que as *outras* funções chamadas por ela estejam desacopladas.
    - *Status: Pendente.*

- [ ] **Etapa 6: Ajustar Código Chamador (`streamlit_app.py` ou similar)**
    - Implementar chamadas `st.*` no script principal do Streamlit, envolvendo as funções refatoradas.
    - Tratar retornos/exceções das funções refatoradas.
    - *Status: Pendente (a fazer após cada função refatorada ou ao final).*

- [ ] **Etapa 7: Testes**
    - Testar funcionalidades no Streamlit após refatoração.
    - *Status: Pendente.*

- [ ] **Etapa 8: Commit das Mudanças**
    - Fazer commit das alterações da refatoração na branch `refactor/decouple-ai-ui`.
    - *Status: Pendente.* 