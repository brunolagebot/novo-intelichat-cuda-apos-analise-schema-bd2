# Roteiro de Organização e Refatoração do Projeto

Este documento descreve o processo de reorganização da estrutura de pastas e refatoração do código da aplicação Streamlit de edição de metadados.

## Objetivos

*   Melhorar a organização do código, separando responsabilidades em módulos distintos.
*   Facilitar a manutenção e a adição de novas funcionalidades.
*   Centralizar configurações e lógica reutilizável.

## Estrutura de Destino do Projeto

```
.
├── core/                   # Lógica principal (backend) da aplicação
│   ├── __init__.py
│   ├── config.py           # Constantes e configurações
│   ├── data_loader.py      # Funções de carregamento de dados (JSON, etc.)
│   ├── db_utils.py         # Funções de interação com BD Firebird
│   ├── metadata_logic.py   # Heurísticas e lógica de manipulação de metadados
│   ├── ai_integration.py   # Funções de IA (Ollama, FAISS)
│   └── analysis.py         # Funções de análise estrutural
├── data/                   # Arquivos de dados gerados e utilizados
│   ├── schema_metadata.json # Metadados editados pelo usuário (MOVIDO)
│   ├── combined_schema_details.json
│   ├── schema_with_embeddings.json (opcional)
│   ├── overview_counts.json
│   ├── faiss_column_index.idx (opcional)
│   ├── chat_history.json (opcional)
│   └── chat_feedback.json (opcional)
├── docs/                   # Documentação
│   └── README.md           # Documentação principal (MOVIDO)
├── scripts/                # Scripts auxiliares (extração, merge, contagem, etc.)
│   └── ... (arquivos .py)
├── src/                    # Código fonte auxiliar (ex: integração Ollama)
│   └── ollama_integration/
│   │   └── client.py
│   └── utils/
│       └── json_helpers.py
├── ui/                     # Módulos da Interface Streamlit
│   ├── __init__.py
│   ├── sidebar.py          # Código da barra lateral
│   ├── overview_page.py    # Código da página "Visão Geral"
│   ├── edit_page.py        # Código da página "Editar Metadados"
│   ├── chat_page.py        # Código da página "Chat com Schema"
│   └── analysis_page.py    # Código da página "Análise"
├── .gitignore
├── requirements.txt
├── streamlit_app.py        # Ponto de entrada principal (orquestrador)
└── roteiro-organizacao.md  # Este arquivo
```

## Etapas da Refatoração

**Concluídas:**

1.  ✅ Criação da branch `refactor/organize-directories`.
2.  ✅ Criação da estrutura inicial de arquivos (`config.py`, `data_loader.py`, `db_utils.py`, `metadata_logic.py`, `ai_integration.py`, `analysis.py`).
3.  ✅ Criação do diretório `ui/` e arquivos de página (`sidebar.py`, `overview_page.py`, `edit_page.py`, `chat_page.py`, `analysis_page.py`).
4.  ✅ Criação do diretório `docs/`.
5.  ✅ Movimentação de `README.md` para `docs/`.
6.  ✅ Movimentação de `etapas-sem-gpu/schema_metadata.json` para `data/schema_metadata.json`.
7.  ✅ Remoção do diretório `etapas-sem-gpu/`.
8.  ✅ Atualização da constante `METADATA_FILE` em `streamlit_app.py`.
9.  ✅ Movimentação das constantes de `streamlit_app.py` para `config.py`.
10. ✅ Criação do diretório `core/`.
11. ✅ Movimentação dos arquivos de lógica (`config.py`, `data_loader.py`, etc.) para `core/`.
12. ✅ Adição dos novos arquivos/diretórios ao Git (`git add .`).
13. ✅ Atualização da importação de `config` em `streamlit_app.py` para `core.config`.
14. ✅ Movimentação das funções de carregamento (`load_technical_schema`, `load_metadata`, `load_overview_counts`, `load_and_process_data`) para `core/data_loader.py`.
15. ✅ Remoção da definição de `load_and_process_data` de `streamlit_app.py` e ajuste da importação/chamada.

**Pendentes:**

1.  ⏳ Remoção das definições de `load_technical_schema`, `load_metadata`, `load_overview_counts` de `streamlit_app.py`.
2.  ⏳ Remoção das definições de `fetch_latest_nfs_timestamp`, `fetch_sample_data` de `streamlit_app.py`.
3.  ⏳ Remoção das definições de `get_type_explanation`, `save_metadata`, `find_existing_info`, `get_column_concept`, `apply_heuristics_globally`, `populate_descriptions_from_keys`, `compare_metadata_changes` de `streamlit_app.py`.
4.  ⏳ Mover funções de `core/ai_integration.py` (`generate_ai_description`, `build_faiss_index`, `find_similar_columns`, `get_query_embedding`, `handle_embedding_toggle`) e remover de `streamlit_app.py`.
5.  ✅ Mover funções de `core/analysis.py` (`analyze_key_structure`, `generate_documentation_overview`) e remover de `streamlit_app.py`.
6.  ✅ Mover código da UI da barra lateral para `ui/sidebar.py`.
7.  ✅ Mover código da UI da página "Visão Geral" para `ui/overview_page.py`.
8.  ✅ Mover código da UI da página "Editar Metadados" para `ui/edit_page.py` (código original comentado em `streamlit_app.py`).
9.  ✅ Mover código da UI da página "Análise" para `ui/analysis_page.py`.
10. ✅ Mover código da UI da página "Chat com Schema" para `ui/chat_page.py` (código original comentado em `streamlit_app.py`).
11. ⬜ Refatorar `streamlit_app.py` para importar e chamar as funções/componentes dos módulos `core/` e `ui/`.
12. ⬜ Revisar e limpar imports em todos os arquivos.
13. ⬜ Testar a aplicação completa para garantir que tudo funciona como esperado.
14. ⬜ Atualizar o `docs/README.md` para refletir a estrutura final e como usar os módulos.
15. ⬜ Fazer commit das alterações na branch `refactor/organize-directories`.

## Arquivos Chave e Caminhos Alterados

*   **`etapas-sem-gpu/schema_metadata.json`** -> **`data/schema_metadata.json`**
*   **`README.md`** -> **`docs/README.md`**
*   **`config.py`** -> **`core/config.py`**
*   **`data_loader.py`** -> **`core/data_loader.py`**
*   **`db_utils.py`** -> **`core/db_utils.py`**
*   **`metadata_logic.py`** -> **`core/metadata_logic.py`**
*   **`ai_integration.py`** -> **`core/ai_integration.py`**
*   **`analysis.py`** -> **`core/analysis.py`**

*(Outros arquivos de UI serão movidos para `ui/` e funções serão movidas de `streamlit_app.py` para os módulos em `core/`)* 