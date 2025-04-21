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

1.  **`scripts/data_preparation/extract_technical_schema.py`**:
    *   **Propósito:** Conecta-se ao banco de dados Firebird e extrai a **estrutura técnica detalhada**, incluindo tabelas, views, colunas, tipos, constraints (PK/FK), valores default, descrições do DB e **amostras de dados** (limitadas por `SAMPLE_SIZE`).
    *   **Execução:** Deve ser executado como um módulo a partir da raiz do projeto:
        ```bash
        python -m scripts.data_preparation.extract_technical_schema
        ```
    *   **Dependências:** Requer variáveis de ambiente ou segredos configurados (ver seção "Gerenciamento de Segredos").
    *   **Importante:** A busca por amostras é feita em paralelo (usando `MAX_WORKERS_FOR_SAMPLING` definido no script, atualmente 15) e **pode ser muito demorada (horas)**, dependendo do tamanho do banco e da máquina. Erros de amostragem podem ocorrer (ver logs), mas o script tenta continuar.
    *   **Saída:** Salva o resultado completo em **`data/metadata/technical_schema_from_db.json`**. Este arquivo serve como a base para enriquecimentos posteriores.

2.  **`scripts/data_preparation/merge_metadata_for_embeddings.py`** (ou similar):
    *   **Propósito:** Combina a estrutura técnica base (`technical_schema_from_db.json`) com metadados manuais (`metadata_schema_manual.json`) e/ou descrições geradas por IA (`ai_generated_descriptions...json`).
    *   **Saída:** Gera o arquivo `merged_schema_for_embeddings.json` (ou similar), pronto para a geração de embeddings.

3.  **`scripts/ai_tasks/generate_embeddings.py`** (Opcional):
    *   **Propósito:** Carrega o schema mesclado (e.g., `merged_schema_for_embeddings.json`), gera embeddings vetoriais usando um modelo (via Ollama ou outra API) e salva o resultado em `schema_with_embeddings.json` e `faiss_column_index.idx`.

*(Outros scripts mencionados anteriormente, como `extract_schema.py` ou `merge_schema_data.py`, podem ser legados ou ter propósitos específicos. A sequência acima representa o fluxo principal para obter o schema técnico e prepará-lo para embeddings).*

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

1.  **`data/metadata/technical_schema_from_db.json`** (Base Técnica Atual)
    *   **Origem:** Gerado pelo script `scripts/data_preparation/extract_technical_schema.py`.
    *   **Conteúdo:** Estrutura técnica detalhada extraída diretamente do banco de dados (tabelas, views, colunas, tipos, constraints (PK/FK), defaults, descrições do DB e amostras de dados limitadas). Inclui campos pré-definidos (inicializados como `null`) para metadados manuais e de IA **tanto para colunas quanto para objetos (tabelas/views)**.
    *   **Estrutura Esperada:**
        ```json
        {
          "NOME_DA_TABELA_OU_VIEW": {
            "object_type": "TABLE" | "VIEW",
            "description": "Descrição do objeto vinda do DB (ou null)",
            "object_business_description": null,
            "object_ai_generated_description": null,
            "object_ai_model_used": null,
            "object_ai_generation_timestamp": null,
            "columns": [
              {
                "name": "NOME_DA_COLUNA",
                "type": "TIPO_MAPEADO (VARCHAR, INTEGER, etc.)",
                "nullable": true | false,
                "default_value": "Valor default (string ou null)",
                "description": "Descrição da coluna vinda do DB (ou null)",
                "is_pk": true | false,
                "is_fk": true | false,
                "fk_references": { // Ou null se não for FK, pode ser lista se FK composta
                  "references_table": "NOME_TABELA_PK",
                  "references_column": "NOME_COLUNA_PK"
                  // ... outros campos se FK composta
                } | list | null,
                "sample_values": [ /* amostras */ ] | null, // Amostras (ou null se erro/BLOB)
                // Campos para enriquecimento futuro
                "business_description": null,
                "value_mapping_notes": null,
                "ai_generated_description": null,
                "ai_model_used": null,
                "ai_generation_timestamp": null,
                "text_for_embedding": null
              },
              // ... outras colunas
            ],
            // Chave _analysis adicionada pelo script
            "_analysis": {
                 "composite_pk_tables": [],
                 "junction_tables": [],
                 "fk_definitions": {}
             }
          },
          // ... outras tabelas/views
        }
        ```
    *   **Uso:** Serve como a **base estrutural** para o processo de merge no script `scripts/merge_metadata_for_embeddings.py` ou para uso direto se outros enriquecimentos não forem necessários.

2.  **`metadata_schema_manual.json`** (Metadados Manuais)
    *   **Origem:** **Criado e atualizado pela interface da aplicação Streamlit (ou manualmente).** 
    *   **Conteúdo:** Armazena os **metadados de negócio inseridos manualmente**: `business_description` e `value_mapping_notes` para colunas. Pode conter descrições a nível de tabela/view também.
    *   **⚠️ IMPORTANTE:** Este arquivo é a **fonte da verdade para os dados inseridos manualmente**. 
    *   **Uso:** Usado como input pelo `scripts/merge_metadata_for_embeddings.py` para enriquecer o schema base com informações manuais.
    *   **Backup/Versioning:** A aplicação Streamlit pode criar backups automáticos deste arquivo na pasta `data/metadata_backups/`.

3.  **`ai_generated_descriptions_openai_35turbo.json`** (Descrições IA)
    *   **Origem:** Gerado por scripts como `scripts/generate_ai_descriptions.py` (ou nome similar dependendo do modelo/método usado).
    *   **Conteúdo:** Uma lista de dicionários, cada um contendo a `generated_description`, `model_used`, e `generation_timestamp` para uma coluna específica.
    *   **Uso:** Usado como input pelo `scripts/merge_metadata_for_embeddings.py` para adicionar as descrições geradas por IA ao schema base.

4.  **`merged_schema_for_embeddings.json`** (Schema Mesclado para Embeddings) - **NOVO**
    *   **Origem:** Gerado pelo novo script `scripts/merge_metadata_for_embeddings.py`.
    *   **Conteúdo:** Resulta da **combinação** da estrutura técnica base (`technical_schema_from_db.json`) com os metadados manuais (`metadata_schema_manual.json`) e as descrições geradas por IA (`ai_generated_descriptions...json`). Preserva a estrutura do arquivo base, mas preenche os campos `business_description`, `value_mapping_notes`, `ai_generated_description`, `ai_model_used`, `ai_generation_timestamp` com os dados das outras fontes.
    *   **Uso:** Este é o arquivo **final e enriquecido** que deve ser usado como **input** para o script que gera os embeddings vetoriais (ex: `scripts/generate_embeddings.py`).

5.  **`combined_schema_details.json`** (Schema para UI)
    *   **Origem:** Gerado pelo script `scripts/merge_schema_data.py` (script legado ou alternativo, documentação pendente). 
    *   **Conteúdo:** Resulta da combinação de um schema técnico (possivelmente `technical_schema_details.json`) com metadados manuais (`schema_metadata.json` ou similar). *Pode ter uma estrutura diferente do `merged_schema_for_embeddings.json`.*
    *   **Uso:** Principal arquivo de dados **lido pela aplicação Streamlit** para exibir a estrutura e alimentar as funcionalidades de edição e análise. **Não é mais o input direto para a geração de embeddings no fluxo principal.** É definido pela constante `config.OUTPUT_COMBINED_FILE`.

6.  **`schema_with_embeddings.json`** (Schema com Embeddings - Opcional)
    *   **Origem:** Gerado pelo script `scripts/generate_embeddings.py` (ou similar).
    *   **Conteúdo:** Uma cópia do **`merged_schema_for_embeddings.json`** enriquecida com vetores de embedding para cada coluna (ou suas descrições).
    *   **Uso:** Carregado pela aplicação quando a opção "Usar Embeddings" está ativa, habilitando a busca por similaridade semântica e o contexto do chat. É definido pela constante `config.EMBEDDED_SCHEMA_FILE`.

7.  **`overview_counts.json`**
    *   **Origem:** Gerado e atualizado pelo script `scripts/calculate_row_counts.py` (executado manualmente pela interface ou via script).
    *   **Conteúdo:** Cache da contagem de linhas para cada tabela/view e o timestamp da contagem.
    *   **Uso:** Exibido na página "Visão Geral".

8.  **`faiss_column_index.idx`** (Opcional)
    *   **Origem:** Gerado pelo script `scripts/generate_embeddings.py` (ou similar) junto com `schema_with_embeddings.json`.
    *   **Conteúdo:** Índice binário FAISS otimizado para busca rápida por similaridade nos embeddings.
    *   **Uso:** Carregado em memória pela aplicação (se embeddings estiverem ativos).

9.  **`chat_history.json`** (Opcional)
    *   **Origem:** Criado e atualizado pela funcionalidade "Chat com Schema".
    *   **Conteúdo:** Histórico das perguntas e respostas da interação com o chat.
    *   **Uso:** Persistir o histórico do chat entre sessões.

10. **`chat_feedback.json`** (Opcional)
    *   **Origem:** Criado e atualizado pela funcionalidade "Chat com Schema" quando o usuário dá feedback.
    *   **Conteúdo:** Registro do feedback (Bom/Médio/Ruim) para cada mensagem do assistente.
    *   **Uso:** Coleta de dados para avaliação e melhoria futura do assistente de chat.

**(Arquivos Legados/Alternativos - Verificar necessidade)**

*   **`technical_schema_details.json`**: Versão básica da extração técnica (provavelmente substituído por `technical_schema_from_db.json`).
*   **`enhanced_technical_schema.json`**: Versão mais completa da extração técnica (de `extract_schema.py` - legado?).
*   **`generated_schema_structure.json`**: Saída de `extract_new_schema_firebird.py` (pode ser redundante agora).
*   **`merge_schema_data.py`**: Script legado para combinar schema técnico e manual para a UI (verificar se ainda necessário ou substituído pelo fluxo de embeddings).
*   **`scripts/analyze_schema_completeness.py`**: Script removido.
*   **`scripts/data_preparation/merge_enrich_schema.py`**: Script removido.

**Ordem de Geração/Fluxo (Foco em Embeddings):**

1.  Extração Técnica (`scripts/data_preparation/extract_technical_schema.py`) -> `data/metadata/technical_schema_from_db.json`
2.  Edição Manual (UI ou Direta) -> `data/metadata/metadata_schema_manual.json`
3.  Geração IA (`scripts/generate_ai_descriptions.py`) -> `data/metadata/ai_generated_descriptions...json`
4.  **Merge Final** (`scripts/merge_metadata_for_embeddings.py`) -> `data/processed/merged_schema_for_embeddings.json`
5.  Geração Embeddings (`scripts/generate_embeddings.py`) -> `data/embeddings/schema_with_embeddings.json` + `data/embeddings/faiss_column_index.idx`

(O `overview_counts.json` e os arquivos de chat são gerados independentemente. O `combined_schema_details.json` pode ter um fluxo paralelo ou legado para a UI.)

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

## Estrutura do Projeto

```
Novo/\
├── .streamlit/                 # Configurações e segredos do Streamlit\
│   └── secrets.toml\
├── data/                       # Arquivos de dados (schemas, metadados, embeddings, logs, etc.)\
│   ├── ai_outputs/             # Saídas geradas por modelos de IA\
│   │   └── raw/                # Saídas brutas\
│   │       └── ... (ex: ai_generated_descriptions_*.json)\
│   ├── chat/                   # Dados relacionados ao chatbot\
│   │   ├── chat_feedback.json\
│   │   └── chat_history.json\
│   ├── embeddings/             # Embeddings e índices FAISS\
│   │   ├── ... (ex: faiss_index_*.idx)\
│   │   └── ... (ex: schema_embeddings_*.json)\
│   ├── input/                  # Dados de entrada brutos (ex: schema técnico inicial)\
│   │   └── technical_schema_details.json\
│   ├── logs/                   # Arquivos de log\
│   │   └── app.log\
│   ├── metadata/               # Metadados manuais e contagens\
│   │   ├── overview_counts.json\
│   │   ├── schema_metadata.json  # <<< Editado pela UI
│   │   └── schema_types_documentation.md\
│   ├── metadata_backups/       # Backups automáticos de schema_metadata.json\
│   │   └── ...\
│   ├── processed/              # Dados processados ou combinados\
│   │   ├── combined_schema_details.json # <<< Usado pela UI
│   │   ├── enhanced_technical_schema.json\
│   │   ├── schema_dataframe.csv\
│   │   └── schema_dataset.jsonl\
│   └── ai_generated_descriptions_openai_35turbo.json # ARQUIVO TEMPORÁRIO (em uso)
├── docs/                       # Documentação do projeto\
│   └── ROADMAP.md\
├── scripts/                    # Scripts Python para tarefas batch/automação\
│   ├── __init__.py\
│   ├── old/                    # Scripts antigos ou substituídos\
│   │   ├── standardize_metadata_keys.py\
│   │   └── standardize_schema_metadata.py\
│   ├── finetuning/             # Scripts para finetune de modelos\
│   │   ├── prepare_finetune_data.py\
│   │   ├── run_finetune.py\
│   │   ├── run_finetune_schema.py\
│   │   └── run_finetune_schema_phi3.py\
│   ├── SCRIPT_DOCUMENTATION.md # Documentação dos scripts (se existir)\
│   └── ... (extract_schema, generate_embeddings, merge_schema, etc.)\
├── src/                        # Código fonte principal da aplicação\
│   ├── __init__.py\
│   ├── core/                   # Lógica central da aplicação\
│   │   ├── __init__.py\
│   │   ├── ai_integration.py   # Integração IA (Ollama, OpenAI, FAISS)\
│   │   ├── data_loader.py      # Carregamento de dados\
│   │   ├── metadata_logic.py   # Lógica de metadados editáveis\
│   │   └── ... (config.py, analysis.py, db_utils.py, etc. - a verificar)\
│   ├── ollama_integration/     # Integração específica com Ollama\
│   │   └── ...\
│   └── utils/                  # Utilitários gerais\
│       └── json_helpers.py\
├── tests/                      # Testes automatizados\
│   └── ...\
├── ui/                         # Módulos da interface Streamlit\
│   ├── __init__.py\
│   ├── analysis_page.py\
│   ├── chat_page.py\
│   ├── edit_page.py\
│   ├── main_page.py\
│   ├── overview_page.py\
│   ├── PAGES_DOC.md\
│   └── sidebar.py\
├── .gitignore                  # Arquivos ignorados pelo Git\
├── README.md                   # Este arquivo\
├── requirements.txt            # Dependências Python\
└── streamlit_app.py            # Ponto de entrada da aplicação Streamlit\
```

## Configuração

*   Clonar o repositório.\
*   Criar e ativar um ambiente virtual Python (ex: `python -m venv .venv`, `source .venv/bin/activate` ou `.\venv\Scripts\activate`).\
*   Instalar dependências: `pip install -r requirements.txt`.\
*   Configurar `secrets.toml` em `.streamlit/` com as chaves de API necessárias (ex: OpenAI).\
*   Configurar variáveis de ambiente (se aplicável, ex: `FIREBIRD_PASSWORD`).\
*   Instalar e configurar o servidor Ollama (se for usar IA local):\
    *   Baixar e instalar Ollama: [https://ollama.com/](https://ollama.com/)\
    *   Baixar os modelos necessários (ex: `ollama pull llama3`, `ollama pull nomic-embed-text`).\
    *   Garantir que o servidor Ollama esteja em execução.\

## Como Usar

### Aplicação Streamlit

Para iniciar a interface web principal:\

```bash\
streamlit run streamlit_app.py\
```\

A aplicação abrirá no seu navegador padrão.\

### Scripts

Os scripts na pasta `scripts/` podem ser executados individualmente para tarefas específicas. Consulte `scripts/SCRIPT_DOCUMENTATION.md` (se existir) ou o código de cada script para detalhes sobre seus argumentos e funcionalidades.\

Exemplo:\

```bash\
python scripts/generate-ai-description-openia.py --max_items 10\
```\

## Diretrizes de Desenvolvimento

*   **Comentários:** Todos os scripts Python criados devem incluir:
    *   Um *docstring* no nível do arquivo explicando o propósito geral do script.
    *   Um *docstring* para cada função definida, explicando o que ela faz, seus parâmetros e o que retorna.
*   **Controle de Versão:** Utilize o Git para controle de versão. Faça commits pequenos e focados com mensagens claras.
*   **Estrutura do Projeto:** Mantenha a estrutura de diretórios organizada conforme definido neste README e no roadmap.
*   **Configuração:** Evite caminhos e configurações "hardcoded". Utilize o arquivo `src/core/config.py` para constantes e `st.secrets` ou variáveis de ambiente para dados sensíveis.
*   **Refatoração:** Se um arquivo ou função se tornar muito longo ou complexo, divida-o em unidades menores e mais gerenciáveis.
*   **Resumo da Execução:** Ao final da execução, cada script deve imprimir um resumo claro das suas ações no console (ex: número de itens processados, arquivos lidos/escritos, erros encontrados). Isso complementa os logs detalhados.

## Formato para Descrições Geradas por IA

Arquivos JSON contendo descrições de tabelas e colunas geradas por ferramentas de IA (como `scripts/ai_tasks/generate-ai-description-openia.py`) devem seguir a estrutura abaixo. Estes arquivos são usados por scripts de enriquecimento (como `scripts/enrichment/apply_ai_descriptions.py`) para mesclar as descrições geradas com o schema base consolidado.

O arquivo JSON deve ser uma **lista** (`list`), onde cada elemento é um **objeto** (`dict`) representando uma única descrição gerada.

Cada objeto na lista deve conter as seguintes chaves:

*   `object_type` (String): O tipo do objeto descrito (ex: "TABLE", "VIEW").
*   `object_name` (String): O nome da tabela ou view à qual a descrição pertence.
*   `column_name` (String | Null): O nome da coluna específica descrita. Se a descrição for para a tabela/view como um todo, esta chave pode estar ausente ou ser `null` (embora o script atual gere apenas para colunas).
*   `generated_description` (String): A descrição textual gerada pela IA.
*   `model_used` (String): O identificador do modelo de IA utilizado (ex: "gpt-3.5-turbo").
*   `generation_timestamp` (String): Um timestamp no formato ISO 8601 (ex: "2024-08-01T10:00:00Z") indicando quando a descrição foi gerada.

**Exemplo:**

```json
[
  {
    "object_type": "TABLE",
    "object_name": "NOME_DA_TABELA_1",
    "column_name": "NOME_DA_COLUNA_A",
    "generated_description": "Descrição da Coluna A gerada por IA.",
    "model_used": "gpt-3.5-turbo",
    "generation_timestamp": "2024-08-01T10:00:00Z"
  },
  {
    "object_type": "TABLE",
    "object_name": "NOME_DA_TABELA_1",
    "column_name": "NOME_DA_COLUNA_B",
    "generated_description": "Descrição da Coluna B gerada por IA.",
    "model_used": "gpt-3.5-turbo",
    "generation_timestamp": "2024-08-01T10:00:05Z"
  },
  {
    "object_type": "VIEW",
    "object_name": "NOME_DA_VIEW_1",
    "column_name": "COLUNA_X_DA_VIEW",
    "generated_description": "Descrição da Coluna X da View 1 gerada por IA.",
    "model_used": "gpt-3.5-turbo",
    "generation_timestamp": "2024-08-01T10:00:10Z"
  }
  // ... outros objetos de descrição
]
```

Scripts que consomem este formato (como `scripts/enrichment/apply_ai_descriptions.py`) irão iterar sobre esta lista, extrair as informações de cada objeto e mesclá-las no schema consolidado (`data/metadata/consolidated_schema.json`), procurando pela tabela/coluna correspondente no schema base.

<!-- Adicionar mais seções conforme necessário: Contribuição, Licença, etc. --> 