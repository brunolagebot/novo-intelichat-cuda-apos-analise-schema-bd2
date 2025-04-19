# Roteiro de Expansão da Cobertura de Testes

Este documento descreve os passos sugeridos para aumentar a cobertura de testes automatizados do projeto, visando maior confiabilidade e facilidade de manutenção.

## Checkpoints

- [X] **1. Configuração Inicial do Ambiente de Testes**
    - **Objetivo:** Estabelecer a estrutura e configuração básica para `pytest`.
    - **Ações:**
        - [X] Criar diretório `tests/`.
        - [X] Configurar `pytest.ini` com captura de logs.
        - [X] Criar teste inicial (`tests/core/test_config.py`) como exemplo.

- [X] **2. Testes de Módulos Utilitários e de Carregamento Simples**
    - **Objetivo:** Cobrir funções fundamentais de baixo nível.
    - **Ações:**
        - [X] Testar `src/core/log_utils.py` (`setup_logging`).
        - [X] Testar `src/utils/json_helpers.py` (`load_json`, `save_json`) com casos de sucesso e erro.
        - [X] Testar funções de carregamento simples em `src/core/data_loader.py` (`load_metadata`, `load_technical_schema`, `load_overview_counts`) usando mocks/fixtures.
        - [X] Testar script de análise (`scripts/analyze_schema.py`) com dados de exemplo.

- [ ] **3. Expansão dos Testes Existentes**
    - **Objetivo:** Aumentar a robustez dos testes já criados.
    - **Ações:**
        - [ ] Adicionar mais casos de teste em `test_log_utils.py` (ex: diferentes níveis de log).
        - [ ] Adicionar mais casos de teste em `test_json_helpers.py` (ex: dados não serializáveis).
        - [ ] Adicionar mais casos de teste em `test_data_loader.py` (ex: arquivos JSON malformados).
        - [ ] Adicionar mais casos de teste em `test_analyze_schema.py` (ex: cenários sem FKs, sem PKs, etc.).

- [ ] **4. Testes de Carregamento de Dados Principal**
    - **Objetivo:** Testar a função central de carregamento e inicialização.
    - **Ações:**
        - [ ] Testar `src/core/data_loader.py::load_and_process_data`.
        - [ ] Implementar mocking para `streamlit.session_state` (usando `unittest.mock` ou fixtures `pytest`).
        - [ ] Verificar se o estado da sessão é populado corretamente.

- [ ] **5. Testes de Lógica de Negócio e Metadados**
    - **Objetivo:** Cobrir as funções que manipulam os metadados.
    - **Ações:**
        - [ ] Criar `tests/core/test_metadata_logic.py`.
        - [ ] Testar funções de heurística (`apply_heuristics_globally`, `populate_descriptions_from_keys`).
        - [ ] Testar `save_metadata`, verificando a escrita no arquivo e a criação de backups.
        - [ ] Testar `compare_metadata_changes`.

- [ ] **6. Testes de Integração com IA**
    - **Objetivo:** Testar as funções que interagem com modelos de linguagem e embeddings.
    - **Ações:**
        - [ ] Criar `tests/core/test_ai_integration.py`.
        - [ ] Testar `build_faiss_index` (pode precisar de dados de embedding de teste).
        - [ ] Testar `find_similar_columns` (usando índice FAISS de teste).
        - [ ] Implementar mocking para APIs externas (Ollama/OpenAI) para testar `generate_ai_description` e `get_query_embedding` sem fazer chamadas reais.

- [ ] **7. Testes de Interação com Banco de Dados**
    - **Objetivo:** Cobrir funções que acessam o banco Firebird.
    - **Ações:**
        - [ ] Criar `tests/core/test_db_utils.py`.
        - [ ] Configurar um banco de dados de teste (ou usar mocking da biblioteca `fdb`).
        - [ ] Testar `fetch_latest_nfs_timestamp`.
        - [ ] Testar `fetch_sample_data`.

- [ ] **8. Testes de Scripts Adicionais**
    - **Objetivo:** Aumentar a cobertura dos scripts em `scripts/`.
    - **Ações:**
        - [ ] Criar arquivos de teste para scripts importantes (ex: `test_extract_schema.py`, `test_generate_embeddings.py`).
        - [ ] Testar a lógica principal de cada script, usando argumentos de linha de comando mockados e/ou arquivos de entrada/saída temporários.

- [ ] **9. Testes de Interface (UI) - *Opcional/Avançado***
    - **Objetivo:** Testar a lógica dentro dos módulos da UI.
    - **Ações:**
        - [ ] Investigar estratégias para testar componentes Streamlit (mocking de chamadas `st.*`, ferramentas de teste de UI).
        - [ ] Focar em testar a lógica *por trás* dos componentes (ex: as funções chamadas por botões) em vez da renderização visual em si.

## Benefícios Esperados

*   Maior confiança ao refatorar ou adicionar novas funcionalidades.
*   Detecção precoce de regressões.
*   Documentação viva do comportamento esperado do código.
*   Facilidade na depuração, com logs específicos de testes. 