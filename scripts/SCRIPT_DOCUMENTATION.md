# Documentação dos Scripts (`scripts/`)

Este documento descreve os scripts auxiliares encontrados no diretório `scripts/`, que são usados para gerar, processar e analisar os dados de schema utilizados pela aplicação Streamlit principal.

---

## 1. `extract_schema.py`

**Propósito:**

Este script conecta-se a um banco de dados Firebird e extrai uma representação detalhada da sua estrutura técnica. Ele coleta informações sobre tabelas, views, colunas (tipos, nulidade, defaults, descrições técnicas), amostras de dados, constraints (PK, FK, Unique) e índices (incluindo seus propósitos). Adicionalmente, calcula quantas vezes cada coluna é referenciada por chaves estrangeiras.

**Uso:**

O script é projetado para ser executado da raiz do projeto:

```bash
python scripts/extract_schema.py
```

**Configuração:**

As credenciais e parâmetros de conexão com o banco de dados Firebird são obtidos na seguinte ordem de prioridade:

1.  **Arquivo `.streamlit/secrets.toml`:** Procure pela seção `[database]` com as chaves:
    *   `host` (padrão: "localhost")
    *   `port` (padrão: 3050)
    *   `db_path` (**Obrigatório:** Caminho para o arquivo .fdb)
    *   `user` (padrão: "SYSDBA")
    *   `password` (**Obrigatório se não estiver em env var**)
    *   `charset` (padrão: "WIN1252")
2.  **Variáveis de Ambiente:** Se não encontradas no `secrets.toml`, busca por:
    *   `FIREBIRD_HOST`
    *   `FIREBIRD_PORT`
    *   `FIREBIRD_DB_PATH`
    *   `FIREBIRD_USER`
    *   `FIREBIRD_PASSWORD`
    *   `FIREBIRD_CHARSET`
3.  **Prompt Interativo:** Se a senha (`password` / `FIREBIRD_PASSWORD`) não for encontrada, o script solicitará que seja digitada no terminal.

**Input:**

*   Acesso a um banco de dados Firebird válido, com as credenciais corretas configuradas.

**Output:**

*   **`data/enhanced_technical_schema.json`:** Um arquivo JSON contendo a estrutura detalhada do schema extraído. Inclui:
    *   Dicionário principal com nomes de tabelas/views como chaves.
    *   Para cada tabela/view: tipo, descrição, lista de colunas, dicionário de constraints, lista de índices.
    *   Para cada coluna: nome, tipo formatado, nulidade, valor default, descrição técnica, e `sample_values` (lista com até 50 valores string distintos das primeiras 50 linhas da tabela/view, exceto para `SMALLINT` que são marcados como `BOOLEAN_SKIPPED`).
    *   Para constraints/índices: nomes, colunas, tipos, referências (para FKs), status.
    *   Uma chave `fk_reference_counts` no nível raiz, mapeando `NomeTabela.NomeColuna` para o número de FKs que a referenciam.

**Lógica Principal:**

1.  Carrega configuração.
2.  Conecta ao banco Firebird.
3.  Itera sobre todas as tabelas/views não-sistema.
4.  Para cada uma, busca metadados de colunas, amostra de dados (primeiras 50 linhas), constraints e índices.
5.  Processa a amostra para obter até 50 valores distintos por coluna.
6.  Calcula as contagens de referência de FKs para todo o schema.
7.  Salva o resultado completo no arquivo JSON de saída.
8.  Exibe uma barra de progresso (`tqdm`) durante a extração.

**Dependências Principais:**

*   `fdb`: Driver Python para Firebird.
*   `toml`: Para ler o arquivo `secrets.toml`.
*   `python-dotenv`: Para carregar variáveis de ambiente do arquivo `.env`.
*   `tqdm`: Para a barra de progresso.

---

## 2. `merge_schema_data.py`

**(Documentação Pendente)**

*   **Propósito:**
*   **Uso:**
*   **Configuração:**
*   **Input:**
*   **Output:**
*   **Lógica Principal:**
*   **Dependências Principais:**

---

## 3. `generate_embeddings.py`

**(Documentação Pendente)**

*   **Propósito:**
*   **Uso:**
*   **Configuração:**
*   **Input:**
*   **Output:**
*   **Lógica Principal:**
*   **Dependências Principais:**

---

## 4. `calculate_row_counts.py`

**(Documentação Pendente)**

*   **Propósito:**
*   **Uso:**
*   **Configuração:**
*   **Input:**
*   **Output:**
*   **Lógica Principal:**
*   **Dependências Principais:**

---

## 5. `generate_schema_dataframe.py`

**(Documentação Pendente)**

*   **Propósito:**
*   **Uso:**
*   **Configuração:**
*   **Input:**
*   **Output:**
*   **Lógica Principal:**
*   **Dependências Principais:**

---

## 6. `generate_ai_descriptions.py`

**Propósito:**

Este script utiliza um modelo de linguagem grande (LLM), como o Llama 3, via `transformers` e `peft`, para gerar descrições de negócio para colunas de um schema de banco de dados. Ele é projetado para:

*   Carregar um modelo base e, opcionalmente, um adaptador PEFT treinado.
*   Ler um arquivo de schema técnico (geralmente `data/enhanced_technical_schema.json`).
*   Ler um arquivo de schema combinado (geralmente `data/combined_schema_details.json`) para identificar descrições manuais já existentes.
*   Opcionalmente, carregar um índice FAISS e um arquivo com embeddings de colunas (`data/faiss_column_index.idx`, `data/schema_with_embeddings.json`) para enriquecer os prompts com descrições de colunas semanticamente similares.
*   Iterar sobre as colunas, pulando aquelas que já têm descrição manual, que já tiveram descrição gerada por IA anteriormente (a menos que `--force_regenerate` seja usado), ou que têm amostra de dados vazia no schema técnico.
*   Construir prompts detalhados para o LLM, incluindo contexto técnico da coluna e, opcionalmente, descrições de colunas similares.
*   Chamar o LLM para gerar a descrição de negócio.
*   Salvar as descrições geradas em um arquivo JSON separado (`data/ai_generated_descriptions.json`).
*   Registrar logs detalhados, incluindo métricas de performance para carregamento, busca por similaridade (se ativa), construção de prompt, inferência do modelo e tempo total.

**Uso:**

Execute o script da raiz do projeto. Exemplos:

```bash
# Execução simples (usará padrões, bfloat16, com adaptador se existir)
python scripts/generate_ai_descriptions.py --max_items 20

# Execução SEM adaptador PEFT
python scripts/generate_ai_descriptions.py --max_items 20 --adapter ""

# Execução COM enriquecimento por similaridade (requer arquivos FAISS/embeddings)
python scripts/generate_ai_descriptions.py --max_items 50 --enable_similarity_enrichment

# Forçar regeneração de tudo, mesmo se já existirem descrições no output
python scripts/generate_ai_descriptions.py --force_regenerate
```

**Configuração:**

*   **Modelo/Adaptador:** Controlado pelos argumentos `--base_model` e `--adapter`.
*   **Arquivos:** Argumentos `--input`, `--output`, `--embeddings_file`, `--faiss_index` controlam os arquivos de entrada/saída.
*   **Precisão/Device:** Por padrão, tenta carregar em `bfloat16` e usar `device_map="auto"` para distribuir entre GPU e CPU RAM conforme necessário, visando evitar erros de memória.

**Argumentos Principais:**

*   `-i`, `--input`: Schema técnico base (padrão: `data/enhanced_technical_schema.json`).
*   `-o`, `--output`: Arquivo de saída para descrições geradas por IA (padrão: `data/ai_generated_descriptions.json`).
*   `-a`, `--adapter`: Caminho para o adaptador PEFT (padrão: `./results-llama3-8b-chat-schema-adapter`). Passe `""` para desabilitar.
*   `-b`, `--base_model`: Modelo base (padrão: `meta-llama/Meta-Llama-3-8B-Instruct`).
*   `--embeddings_file`: Schema JSON com embeddings (padrão: `data/schema_with_embeddings.json`).
*   `--faiss_index`: Índice FAISS (padrão: `data/faiss_column_index.idx`).
*   `--enable_similarity_enrichment`: Flag para ativar o enriquecimento do prompt via FAISS.
*   `--similarity_top_k`: Número de vizinhos a buscar no FAISS (padrão: 5).
*   `--force_regenerate`: Ignora descrições geradas anteriormente no arquivo de saída.
*   `--max_items`: Limita o número de colunas a processar (para testes).

**Input:**

*   `args.input`: Arquivo JSON do schema técnico.
*   `data/combined_schema_details.json`: Usado para verificar descrições manuais.
*   `args.output`: Lido no início para verificar descrições de IA pré-existentes (se `force_regenerate` não for usado).
*   (Opcional, se `--enable_similarity_enrichment`): `args.embeddings_file` e `args.faiss_index`.

**Output:**

*   `args.output`: Arquivo JSON (`data/ai_generated_descriptions.json` por padrão) contendo uma lista de dicionários, cada um representando uma descrição gerada. Inclui:
    *   `object_type`, `object_name`, `column_name`.
    *   `technical_context`: Detalhes técnicos da coluna usados no prompt.
    *   `generated_description`: A descrição gerada pelo LLM.
    *   `model_used`: Nome do modelo/adaptador.
    *   `generation_timestamp`: Data/hora da geração.
*   **Logs:** Informações detalhadas e métricas de performance no console e no arquivo `data/app.log`.

**Lógica Principal:**

1.  Parsear argumentos.
2.  Carregar modelo e tokenizer (com `bfloat16`, `device_map='auto'`, opcionalmente PEFT).
3.  Carregar schema técnico e combinado.
4.  Carregar FAISS/embeddings se o enriquecimento estiver ativo.
5.  Carregar descrições de IA pré-existentes do arquivo de saída (se não forçar regeneração).
6.  Iterar pelas colunas do schema técnico:
    a.  Pular se já existe descrição manual (no combinado).
    b.  Pular se já existe descrição de IA (no output, se não forçar).
    c.  Pular se a amostra de dados está vazia.
    d.  (Se enriquecimento ativo): Buscar embedding da coluna, pesquisar vizinhos no FAISS, buscar descrições manuais dos vizinhos.
    e.  Construir o prompt (com ou sem contexto similar).
    f.  Chamar `model.generate` para obter a descrição.
    g.  Salvar o resultado na lista.
7.  Registrar contagens (gerados, pulados por tipo, erros).
8.  Salvar a lista completa de resultados no arquivo de saída JSON.
9.  Registrar logs de performance detalhados.

**Dependências Principais:**

*   `torch`
*   `transformers`
*   `peft`
*   `faiss-cpu` ou `faiss-gpu`
*   `numpy`
*   `tqdm`

--- 