# Editor e Explorador de Metadados de Schema Firebird

## Visão Geral

Esta aplicação Streamlit serve como uma ferramenta para visualizar, editar e enriquecer metadados de um schema de banco de dados Firebird. Ela permite aos usuários:

*   Navegar por tabelas e views existentes no schema.
*   Visualizar a estrutura técnica (colunas, tipos, chaves primárias/estrangeiras) extraída do banco.
*   Adicionar e editar metadados de negócios, como descrições para tabelas, views e colunas, além de notas sobre mapeamento de valores.
*   Utilizar heurísticas para preencher automaticamente descrições com base em relacionamentos (FKs) e nomes de colunas.
*   (Opcional) Integrar com um modelo de linguagem (via Ollama) para sugerir descrições.
*   (Opcional) Utilizar embeddings vetoriais (gerados via Ollama e indexados com FAISS) para realizar buscas por similaridade semântica entre colunas e para alimentar um chat que responde perguntas sobre o schema.
*   Visualizar uma visão geral do estado da documentação, incluindo contagens de linhas (cacheadas) das tabelas/views.
*   Analisar a estrutura de chaves do banco (PKs, FKs, tabelas de junção).
*   Interagir com um chatbot para fazer perguntas em linguagem natural sobre o schema.

## Geração do Schema e Embeddings

A aplicação depende de arquivos JSON pré-gerados que contêm a estrutura do banco de dados e, opcionalmente, embeddings vetoriais. O processo de geração geralmente envolve os seguintes scripts (localizados no diretório `scripts/`):

1.  **`extract_schema.py`**: Conecta-se ao banco de dados Firebird e extrai a **estrutura técnica detalhada** em etapas:
    *   Busca informações básicas de Tabelas e Views.
    *   Para cada Tabela/View:
        *   Busca metadados de colunas (tipo, nulidade, default, desc. técnica).
        *   Tenta buscar **10 valores de exemplo (amostra)** para cada coluna (pode falhar para GTTs ou por permissão, gerando um aviso).
        *   Busca Constraints (PK, FK, Unique).
        *   Busca todos os Índices (usuário, sistema, PK, FK, Unique) com seus propósitos.
    *   Calcula contagens de referência FK.
    *   Salva o resultado completo em **`data/enhanced_technical_schema.json`**.
    *   Exibe uma **barra de progresso por Tabela/View** processada durante a execução.
2.  **`merge_schema_data.py`**: Combina o schema técnico extraído (geralmente lê `data/technical_schema_details.json` por padrão, mas pode ser adaptado para usar o `enhanced_...` se necessário) com os metadados manuais de `data/schema_metadata.json`. Adiciona validações e contagens, salvando em `data/combined_schema_details.json`. **Este é o arquivo usado pela aplicação Streamlit principal.**
3.  **`generate_embeddings.py`** (Opcional): Carrega o `combined_schema_details.json`, gera embeddings vetoriais, e salva o resultado em `schema_with_embeddings.json` e `faiss_column_index.idx`.

## Gerenciamento de Segredos (Secrets)

A aplicação Streamlit (`streamlit_app.py`) e os scripts auxiliares (como `extract_schema.py`, `calculate_row_counts.py`) precisam de credenciais para acessar o banco de dados Firebird.

A senha do banco de dados **não** deve ser armazenada diretamente no código. A ordem de prioridade para obter as credenciais é:

1.  **Arquivo de Segredos do Streamlit (`.streamlit/secrets.toml`):** Este é o método preferido, especialmente para deploy. Crie um arquivo chamado `secrets.toml` dentro de uma pasta `.streamlit` na raiz do projeto. As credenciais devem estar na seção `[database]`:
    ```toml
    [database]
    host = "localhost"
    port = 3050
    db_path = "C:/Caminho/Para/Seu/Banco.fdb" # Use barras normais ou escape as invertidas
    user = "SYSDBA"
    password = "sua_senha_aqui"
    charset = "WIN1252"
    ```
    *Tanto a aplicação quanto os scripts tentarão ler deste arquivo primeiro.* Certifique-se de que este arquivo esteja no `.gitignore` para não commitar senhas.

2.  **Variáveis de Ambiente (ou arquivo `.env`):** Se o `secrets.toml` não for encontrado ou não contiver as chaves necessárias, a aplicação e os scripts tentarão ler as seguintes variáveis de ambiente (que podem ser definidas diretamente no sistema ou carregadas de um arquivo `.env` na raiz do projeto pela biblioteca `dotenv`):
    *   `FIREBIRD_HOST`
    *   `FIREBIRD_PORT`
    *   `FIREBIRD_DB_PATH`
    *   `FIREBIRD_USER`
    *   `FIREBIRD_PASSWORD`
    *   `FIREBIRD_CHARSET`

3.  **Prompt Interativo (Apenas Scripts):** Se a senha (`FIREBIRD_PASSWORD`) não for encontrada nem no `secrets.toml` nem nas variáveis de ambiente, os scripts como `extract_schema.py` solicitarão que você a digite diretamente no terminal.

## Estrutura do Código

O código foi recentemente refatorado para uma estrutura mais modular, visando melhor organização e manutenibilidade:

*   **`streamlit_app.py`**: Ponto de entrada principal da aplicação Streamlit. Responsável por inicializar o estado, carregar dados (usando `data_loader`), configurar a página e rotear para os diferentes módulos da UI com base na seleção do usuário na sidebar.
*   **`config.py`**: Armazena constantes globais, como caminhos de arquivos padrão, configurações padrão de conexão (exceto senha), explicações de tipos SQL, e constantes relacionadas à IA/FAISS.
*   **`data_loader.py`**: Contém funções para carregar os diversos arquivos de dados (`.json`) necessários para a aplicação (schema técnico, metadados, contagens cacheadas). Inclui a função `load_and_process_data` que orquestra o carregamento inicial e a inicialização do estado da sessão (`st.session_state`).
*   **`db_utils.py`**: Isola as interações diretas com o banco de dados Firebird. Contém funções como `fetch_latest_nfs_timestamp` e `fetch_sample_data` que usam a biblioteca `fdb`. Também pode conter a lógica para invocar scripts externos que acessam o banco, como `calculate_row_counts.py`.
*   **`metadata_logic.py`**: Agrupa funções relacionadas à manipulação e enriquecimento dos metadados. Inclui heurísticas como `find_existing_info`, `populate_descriptions_from_keys`, e `apply_heuristics_globally`.
*   **`ai_integration.py`**: Contém toda a lógica relacionada à inteligência artificial, incluindo a comunicação com a API do Ollama (`chat_completion`, `get_embedding`), a construção e consulta do índice FAISS (`build_faiss_index`, `find_similar_columns`), e funções auxiliares relacionadas.
*   **`analysis.py`**: Contém funções para análise estrutural do schema, como `analyze_key_structure`.
*   **`ui/` (Diretório)**: Contém módulos separados para cada seção principal da interface do usuário:
    *   `sidebar.py`: Define os elementos da barra lateral (navegação, toggles, botões de ação global).
    *   `overview_page.py`: Renderiza a tela "Visão Geral", incluindo a tabela de resumo e o botão para atualizar contagens.
    *   `edit_page.py`: Renderiza a tela "Editar Metadados", com a seleção de objetos e a lógica complexa de edição por abas de colunas.
    *   `chat_page.py`: Implementa a interface e a lógica do modo "Chat com Schema".
    *   `analysis_page.py`: Renderiza a tela "Análise", exibindo os resultados da análise estrutural.
*   **`src/` (Diretório)**: Pode conter código auxiliar reutilizável, como:
    *   `utils/json_helpers.py`: Funções para carregar e salvar JSON de forma padronizada.
    *   `ollama_integration/client.py`: Código cliente para interagir com a API do Ollama.

## Acesso ao Banco de Dados

O acesso ao banco Firebird é feito principalmente através da biblioteca `fdb`. As funções que necessitam de conexão estão geralmente localizadas em `db_utils.py` ou em scripts externos (`scripts/calculate_row_counts.py`).

Parâmetros de conexão necessários:

*   **DSN (Data Source Name) ou Path:** Caminho para o arquivo `.fdb` (e.g., `C:\Bancos\MEU_BANCO.FDB`) ou um alias configurado no servidor Firebird.
*   **Usuário:** Nome de usuário para conexão (e.g., `SYSDBA`).
*   **Senha:** Senha do usuário (gerenciada via `st.secrets` ou `FIREBIRD_PASSWORD`).
*   **Charset:** Charset da conexão (e.g., `WIN1252`, `UTF8`).

## Logging

A aplicação utiliza o módulo `logging` padrão do Python para registrar informações e erros.

*   **Configuração:** A configuração do logging é centralizada no arquivo `core/logging_config.py` e ativada no início de `streamlit_app.py`.
*   **Níveis:** Por padrão, logs de nível `INFO` e superiores são exibidos no console, enquanto logs de nível `DEBUG` e superiores são salvos no arquivo.
*   **Arquivo de Log:** Um arquivo de log detalhado chamado `app.log` é gerado na pasta `data/`. Este arquivo é útil para depurar problemas que podem não ser óbvios no console.
*   **Rotação/Tamanho:** Atualmente, o arquivo de log cresce indefinidamente (modo 'append'). Para produção, considere adicionar handlers de rotação (como `RotatingFileHandler` ou `TimedRotatingFileHandler`) em `core/logging_config.py` para gerenciar o tamanho do arquivo.

## Arquivos de Dados Chave (Pasta `data/`)

A aplicação utiliza e gera diversos arquivos na pasta `data/`. É crucial entender o propósito e a origem de cada um:

1.  **`technical_schema_details.json`** (Legado/Básico)
    *   **Origem:** Gerado por versões anteriores de `scripts/extract_schema.py` ou se o script atual for modificado para gerar este formato.
    *   **Conteúdo:** Estrutura técnica básica (tabelas, colunas, tipos, constraints PK/FK). **Não inclui defaults ou índices.**
    *   **Uso:** Pode ser usado como input para `merge_schema_data.py` se o schema aprimorado não for necessário ou desejado.

2.  **`enhanced_technical_schema.json`** (Recomendado)
    *   **Origem:** Gerado pela versão atual de `scripts/extract_schema.py`.
    *   **Conteúdo:** Contém a **estrutura técnica mais completa** extraída do banco, incluindo tabelas, views, colunas (com tipo, nulidade, default, descrição técnica, **amostra de valores - até 10**), constraints (PK, FK, Unique) e índices (com propósito). Inclui também contagem de referências FK (`fk_reference_counts`).
    *   **Uso:** Fonte de dados técnicos mais rica. Pode ser usado (com adaptações) pelo script de merge ou para outras análises.

3.  **`schema_metadata.json`**
    *   **Origem:** **Criado e atualizado pela interface da aplicação Streamlit.** Pode ser iniciado manually ou como cópia de um arquivo anterior.
    *   **Conteúdo:** Armazena os **metadados de negócio inseridos manualmente**: descrições de tabelas/views/colunas, notas de mapeamento de valores, contexto global, etc.
    *   **⚠️ IMPORTANTE:** Este arquivo é a **fonte da verdade para os dados inseridos manualmente**. Nenhum script deve **sobrescrever** este arquivo automaticamente. Scripts como `merge_schema_data.py` apenas **leem** este arquivo.
    *   ** बैकअप/संस्करण:** Para prevenir perda acidental de dados, **um backup automático é criado** na subpasta `data/metadata_backups/` cada vez que alterações são salvas pela interface. O nome do backup inclui a data e hora (timestamp), por exemplo: `schema_metadata_20231027_153000.json`. Isso permite recuperar versões anteriores se necessário.

4.  **`combined_schema_details.json`**
    *   **Origem:** Gerado pelo script `scripts/merge_schema_data.py` (lendo um schema técnico e `schema_metadata.json`).
    *   **Conteúdo:** Resulta da **combinação** da estrutura técnica (`technical_schema_details.json`) com os metadados manuais (`schema_metadata.json`). Pode incluir informações adicionais calculadas durante o merge (ex: contagens de referência FK).
    *   **Metadados Internos (`_metadata_info`):** Este arquivo também contém uma chave especial `_metadata_info` no nível raiz, adicionada pelo script de merge, com as seguintes informações:
        *   `total_column_count`: Número total de colunas presentes neste arquivo combinado.
        *   `manual_metadata_column_count`: Número de colunas que possuem `business_description` ou `value_mapping_notes` preenchidos (indicando metadados manuais).
        *   `missing_manual_metadata_column_count`: Número de colunas que **não** possuem `business_description` nem `value_mapping_notes` preenchidos (total - manual).
        *   `validation_status`: Indica se o schema combinado está completo em relação ao `technical_schema_details.json` no momento do merge ('OK' ou 'Incomplete').
        *   `validation_timestamp`: Data e hora (ISO 8601) em que o merge e a validação foram executados.
        *   `missing_objects`: Lista de tabelas/views do schema técnico que não foram encontradas no combinado (se `validation_status` for 'Incomplete').
        *   `missing_columns`: Dicionário mapeando tabelas/views para listas de colunas técnicas que não foram encontradas no combinado (se `validation_status` for 'Incomplete').
    *   **Uso:** Principal arquivo de dados **lido pela aplicação Streamlit** para exibir a estrutura combinada e alimentar as funcionalidades de edição e análise. É definido pela constante `config.TECHNICAL_SCHEMA_FILE` no código.

5.  **`schema_with_embeddings.json`** (Opcional)
    *   **Origem:** Gerado pelo script `scripts/generate_embeddings.py`.
    *   **Conteúdo:** Uma cópia do `combined_schema_details.json` **enriquecida com vetores de embedding** para cada coluna (ou suas descrições).
    *   **Uso:** Carregado pela aplicação quando a opção "Usar Embeddings" está ativa, habilitando a busca por similaridade semântica e o contexto do chat. É definido pela constante `config.EMBEDDED_SCHEMA_FILE`. Devido ao tamanho (potencialmente >1GB), sua geração e uso são opcionais.

6.  **`overview_counts.json`**
    *   **Origem:** Gerado e atualizado pelo script `scripts/calculate_row_counts.py` (executado manualmente pela interface).
    *   **Conteúdo:** Cache da contagem de linhas para cada tabela/view e o timestamp da contagem.
    *   **Uso:** Exibido na página "Visão Geral" para fornecer uma rápida noção do volume de dados sem consultar o banco a cada vez.

7.  **`faiss_column_index.idx`** (Opcional)
    *   **Origem:** Gerado pelo script `scripts/generate_embeddings.py` junto com `schema_with_embeddings.json`.
    *   **Conteúdo:** Índice binário FAISS otimizado para busca rápida por similaridade nos embeddings.
    *   **Uso:** Carregado em memória pela aplicação (se embeddings estiverem ativos) para acelerar a funcionalidade "Buscar Similares".

8.  **`chat_history.json`** (Opcional)
    *   **Origem:** Criado e atualizado pela funcionalidade "Chat com Schema".
    *   **Conteúdo:** Histórico das perguntas e respostas da interação com o chat.
    *   **Uso:** Persistir o histórico do chat entre sessões.

9.  **`chat_feedback.json`** (Opcional)
    *   **Origem:** Criado e atualizado pela funcionalidade "Chat com Schema" quando o usuário dá feedback.
    *   **Conteúdo:** Registro do feedback (Bom/Médio/Ruim) para cada mensagem do assistente.
    *   **Uso:** Coleta de dados para avaliação e melhoria futura do assistente de chat.

**Ordem de Geração/Fluxo:**

Extração Técnica (`extract_schema.py`) -> `technical_schema_details.json` \
+ Edição Manual (UI) -> `schema_metadata.json` \
-> Merge (`merge_schema_data.py`) -> `combined_schema_details.json` \
-> Geração de Embeddings (`generate_embeddings.py`) -> `schema_with_embeddings.json` + `faiss_column_index.idx`

O `overview_counts.json` é gerado separadamente por `calculate_row_counts.py`. Os arquivos de chat são gerados pela interação do usuário.

## Configuração e Execução

1.  **Pré-requisitos:**
    *   Python (versão 3.9 ou superior recomendada).
    *   Acesso a um servidor Firebird e ao arquivo de banco de dados (`.fdb`).
    *   (Opcional) Instância do Ollama rodando localmente ou acessível pela rede, com um modelo de embedding (e.g., `nomic-embed-text`) e um modelo de chat (e.g., `llama3`) baixados.
2.  **Clonar o Repositório:**
    ```bash
    git clone <url_do_repositorio>
    cd <diretorio_do_repositorio>
    ```
3.  **Criar Ambiente Virtual (Recomendado):**
    ```bash
    python -m venv venv
    # Windows
    .\venv\Scripts\activate
    # Linux/macOS
    source venv/bin/activate
    ```
4.  **Instalar Dependências:**
    ```bash
    pip install -r requirements.txt
    ```
    Certifique-se de que `requirements.txt` inclua `streamlit`, `pandas`, `fdb`, `faiss-cpu` (ou `faiss-gpu` se aplicável), `ollama`, `numpy`, etc.
5.  **Configurar Segredos/Variáveis de Ambiente:**
    *   Se rodando localmente, defina a variável de ambiente `FIREBIRD_PASSWORD` com a senha do banco.
    *   Se fazendo deploy, configure o segredo `database.password` na plataforma de hospedagem.
6.  **Gerar Arquivos de Schema (se ainda não existirem):**
    *   Execute os scripts necessários (e.g., `python scripts/extract_schema.py`, `python scripts/generate_embeddings.py`) conforme descrito na seção "Geração do Schema". Certifique-se de que os caminhos e credenciais nos scripts estejam corretos ou sejam passados como argumentos.
7.  **Executar a Aplicação:**
    ```bash
    streamlit run streamlit_app.py
    ```
8.  Acesse a aplicação no navegador através do endereço fornecido pelo Streamlit (geralmente `http://localhost:8501`).

## Dependências Principais

*   **Streamlit:** Framework para criação da interface web interativa.
*   **Pandas:** Manipulação e exibição de dados tabulares (visão geral).
*   **fdb:** Driver Python para conexão com bancos de dados Firebird.
*   **Ollama:** Biblioteca cliente para interagir com a API do Ollama (modelos de linguagem).
*   **FAISS:** Biblioteca para busca eficiente por similaridade em vetores (embeddings).
*   **NumPy:** Manipulação de arrays numéricos (usado com FAISS e embeddings). 