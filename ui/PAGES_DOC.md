# Descrição das Páginas da Interface (UI)

Este documento descreve a finalidade e as funcionalidades principais de cada módulo de página encontrado na pasta `ui/`.

## `overview_page.py`

**Finalidade:** Apresentar uma visão geral e resumida do schema e do estado da documentação dos metadados.

**Funcionalidades Principais:**
*   Exibe estatísticas gerais (número de tabelas, colunas, etc.).
*   Mostra a cobertura da documentação (quantas colunas/tabelas possuem descrições, notas, etc.).
*   Pode apresentar informações sobre a última atualização dos dados no banco de dados (ex: timestamp da última nota fiscal, se aplicável).
*   Utiliza `technical_schema_data` e `metadata_dict` para obter os dados.
*   Pode usar as credenciais do banco de dados (`db_path`, `db_user`, etc.) para buscar informações adicionais diretamente do banco, se necessário.

## `edit_page.py`

**Finalidade:** Permitir a edição manual dos metadados (descrições, notas, mapeamentos) para tabelas e colunas.

**Funcionalidades Principais:**
*   Apresenta uma interface para navegar entre tabelas/views e suas colunas.
*   Exibe os metadados técnicos (`technical_schema_data`) e os metadados editáveis (`metadata_dict`).
*   Permite ao usuário inserir ou modificar:
    *   Descrições de negócio (`business_description`).
    *   Notas de mapeamento de valor (`value_mapping_notes`).
    *   Possivelmente outros campos de metadados definidos.
*   Pode oferecer funcionalidades de IA (se `OLLAMA_AVAILABLE` for `True`):
    *   Gerar sugestões de descrição usando `chat_completion`.
    *   Encontrar colunas similares usando busca FAISS (se o índice estiver disponível) para auxiliar na escrita de descrições.
*   Pode usar as credenciais do banco de dados para buscar dados de exemplo (`fetch_sample_data`) para colunas específicas.
*   Interage com a lógica de salvamento (manual ou automático) dos metadados.

## `analysis_page.py`

**Finalidade:** Fornecer análises mais profundas sobre a estrutura e as relações dentro do schema técnico.

**Funcionalidades Principais:**
*   Utiliza `technical_schema_data` como fonte principal.
*   Pode exibir análises sobre:
    *   Estrutura de chaves primárias (simples vs. compostas).
    *   Identificação de tabelas de junção.
    *   Estrutura de chaves estrangeiras.
    *   Visualização de relacionamentos (potencialmente).
    *   Outras métricas ou análises estruturais definidas em `core/analysis.py`.

## `chat_page.py`

**Finalidade:** Oferecer uma interface de chat para que os usuários possam fazer perguntas em linguagem natural sobre o schema documentado.

**Funcionalidades Principais:**
*   Utiliza um modelo de linguagem grande (LLM) via Ollama (`chat_completion`) para gerar respostas.
*   Coleta contexto relevante para a pergunta do usuário a partir de:
    *   Metadados globais.
    *   Busca por palavras-chave no `technical_schema_data` e `metadata_dict`.
    *   Busca por similaridade semântica (se `OLLAMA_EMBEDDING_AVAILABLE` e o índice FAISS estiverem configurados corretamente) usando `get_embedding` e `find_similar_columns`.
*   Constrói um prompt para o LLM contendo o contexto e a pergunta.
*   Exibe a conversa usando a interface de chat do Streamlit.
*   Gerencia o histórico de chat (carregando e salvando em `data/chat_history.json`).
*   Permite que os usuários forneçam feedback sobre as respostas do assistente (salvo em `data/chat_feedback.json`).
*   Depende da disponibilidade da integração Ollama (`OLLAMA_AVAILABLE`) e da função de embedding (`OLLAMA_EMBEDDING_AVAILABLE`). 