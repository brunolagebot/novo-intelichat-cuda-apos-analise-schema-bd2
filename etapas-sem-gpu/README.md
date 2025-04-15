# Sistema de Análise e Geração de Metadados de Esquema de Banco de Dados

Este projeto fornece um conjunto de ferramentas para extrair, visualizar, anotar e analisar esquemas de banco de dados Firebird, com foco na geração de metadados semânticos e dados de treinamento para modelos de linguagem.

## Componentes Principais

*   **Extração de Esquema (`extract_firebird_schema.py`):** Conecta-se a um banco de dados Firebird (`.fdb`) e extrai a estrutura técnica (tabelas, views, colunas, tipos, constraints) para um arquivo JSON (`firebird_schema.json`).
*   **Visualização e Anotação (`view_schema_app.py`):** Uma aplicação web Streamlit que permite:
    *   Visualizar a estrutura do esquema carregada de `firebird_schema.json`.
    *   Carregar, editar e salvar metadados descritivos (descrição de tabelas/views, descrição de colunas, notas de mapeamento de valores) em `schema_metadata.json`.
    *   Visualizar amostras de dados diretamente do banco.
    *   Utilizar heurísticas e sugestões de IA (via Ollama) para auxiliar no preenchimento das descrições.
    *   Calcular e exibir contagens de linhas por tabela/view, armazenadas em `overview_counts.json`.
    *   Classificar objetos (tabelas/views).
*   **Análise de Dados de Treinamento (`analyze_training_data.py`):** Script para analisar arquivos `.jsonl` contendo dados de treinamento formatados para fine-tuning, gerando métricas e visualizações.
*   **Geração Automática de Rascunho de Metadados (`auto_generate_metadata_draft.py`):** Script experimental para gerar um rascunho inicial de `schema_metadata.json` usando Ollama, baseado apenas na estrutura de `firebird_schema.json`.
*   **Interface de Chat (`app.py`):** Uma interface Gradio simples para interagir com um modelo de linguagem local via API do Ollama.
*   **Scripts Auxiliares:**
    *   `check_db.py`: Verifica a conexão com o banco de dados Firebird.
    *   `setup_env.py`: Auxilia na configuração inicial do ambiente (criação de `.env`).
    *   `main.py`: Ponto de entrada (potencialmente para orquestração futura).

## Bibliotecas e Dependências

As principais bibliotecas utilizadas estão listadas no arquivo `requirements.txt`. Incluem:

*   **`fdb`:** Driver Python para conexão com Firebird.
*   **`streamlit`:** Framework para criação da aplicação web de visualização/anotação.
*   **`requests`:** Para comunicação com a API do Ollama.
*   **`python-dotenv`:** Para carregar variáveis de ambiente do arquivo `.env`.
*   **`pandas`, `matplotlib`, `seaborn`:** Para análise e visualização de dados (usado em `analyze_training_data.py`).
*   **`transformers`, `datasets`, `peft`, `accelerate`, `bitsandbytes`:** Bibliotecas do ecossistema Hugging Face para fine-tuning de modelos (utilizadas no contexto de treinamento descrito em `COMO_TREINAR_O_MODELO.md`).
*   **`gradio`:** Para a interface de chat (`app.py`).
*   **`pytest`, `pytest-mock`:** Para execução de testes.

## Parâmetros de Configuração (`.env`)

O arquivo `.env` armazena configurações sensíveis ou específicas do ambiente:

*   **`OLLAMA_API_URL`:** URL da API do Ollama (padrão: `http://localhost:11434/api/chat`).
*   **`OLLAMA_DEFAULT_MODEL`:** Nome do modelo Ollama a ser usado por padrão (ex: `orca-mini`, `llama3`).
*   **Credenciais do Banco de Dados (Exemplo - adicione conforme necessário):**
    ```dotenv
    DB_HOST=localhost
    DB_PORT=3050
    DB_NAME=/path/to/your/DADOS.FDB
    DB_USER=SYSDBA
    DB_PASSWORD=masterkey
    DB_CHARSET=WIN1252
    ```
    *(**Nota:** As credenciais exatas podem variar e devem ser adicionadas ao `.env` conforme a configuração do seu banco Firebird.)*

## Requisitos

*   Python 3.8+ (recomendado 3.10 ou superior).
*   Acesso a um banco de dados Firebird (`.fdb`).
*   Ollama instalado e em execução (se for utilizar as funcionalidades de IA ou a interface de chat).
*   Dependências listadas em `requirements.txt`.

## Instalação

1.  Clone este repositório.
2.  Crie e ative um ambiente virtual:
    ```bash
    python -m venv venv
    # Linux/Mac:
    source venv/bin/activate
    # Windows:
    .\venv\Scripts\Activate.ps1
    ```
3.  Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```
4.  Crie um arquivo `.env` na raiz do projeto (pode usar `python setup_env.py` como guia inicial) e preencha com a URL do Ollama e as credenciais do seu banco de dados Firebird.

## Uso

*   **Extrair Esquema:**
    ```bash
    python extract_firebird_schema.py
    ```
    (Certifique-se de que as credenciais no `.env` estão corretas).
*   **Visualizar e Anotar Metadados:**
    ```bash
    streamlit run view_schema_app.py
    ```
*   **Analisar Dados de Treinamento (Exemplo):**
    ```bash
    python analyze_training_data.py --input_file finetune_data.jsonl
    ```
*   **Executar Interface de Chat:**
    ```bash
    python app.py
    ```

## Fluxo de Geração de Dados de Treinamento (Resumo)

1.  **Extrair:** Execute `extract_firebird_schema.py` para obter `firebird_schema.json`.
2.  **Anotar:** Use `streamlit run view_schema_app.py` para enriquecer/criar `schema_metadata.json` com descrições e mapeamentos.
3.  **(Próxima Etapa)** **Gerar:** Um script futuro (ex: `generate_schema_training_data.py`, ainda não implementado) lerá `firebird_schema.json` e `schema_metadata.json` para produzir um arquivo `.jsonl` (ex: `firebird_schema_training_data.jsonl`) com exemplos de perguntas e respostas sobre o esquema e seu significado.
4.  **(Opcional) Concatenar:** O arquivo `.jsonl` gerado pode ser concatenado com outros datasets `.jsonl` para treinamentos combinados.
5.  **Treinar:** Use o arquivo `.jsonl` final para treinar um adaptador LoRA para um modelo de linguagem (conforme descrito em `COMO_TREINAR_O_MODELO.md`).

## Estado Atual (Documentado em `ESTADO_TREINAMENTO_MODELO.md`)

O foco atual está na etapa de **Anotação Interativa de Metadados** usando a aplicação Streamlit (`view_schema_app.py`) para refinar o arquivo `schema_metadata.json`. A próxima etapa principal será a implementação do script para gerar os dados de treinamento (`.jsonl`) a partir dos arquivos de esquema e metadados. 