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

**Propósito:**

Este script conecta-se a um banco de dados Firebird e calcula a contagem de linhas para todas as tabelas e views não-sistema. Ele obtém o schema diretamente do banco de dados, eliminando a necessidade de um arquivo de schema externo. O script gera um relatório JSON com as contagens de linhas para cada objeto do banco, incluindo um timestamp para rastreabilidade histórica.

**Uso:**

O script é projetado para ser executado da raiz do projeto:

```bash
# Uso básico (usa configurações padrão)
python scripts/calculate_row_counts.py

# Especificando senha diretamente
python scripts/calculate_row_counts.py --db_password sua_senha

# Usando variável de ambiente para senha
# (Defina FIREBIRD_PASSWORD antes de executar)
python scripts/calculate_row_counts.py

# Salvando também o schema gerado para referência
python scripts/calculate_row_counts.py --save_schema data/metadata/schema_atual.json
```

**Configuração:**

As credenciais e parâmetros de conexão com o banco de dados Firebird são obtidos na seguinte ordem de prioridade:

1. **Argumentos de linha de comando:** `--db_path`, `--db_user`, `--db_password`, `--db_charset`
2. **Variáveis de Ambiente:** `FIREBIRD_PASSWORD` para a senha do banco
3. **Valores padrão de `config.py`:** `DEFAULT_DB_PATH`, `DEFAULT_DB_USER`, `DEFAULT_DB_CHARSET`

**Input:**

* Acesso a um banco de dados Firebird válido, com as credenciais corretas configuradas.

**Output:**

* **`data/metadata/overview_counts.json` (padrão):** Um arquivo JSON contendo:
  * `counts`: Dicionário mapeando `"tipo:nome"` (ex: `"table:CLIENTES"`) para a contagem de linhas.
  * `timestamp`: Data e hora da execução do script.
* **Arquivo de schema opcional:** Se `--save_schema` for especificado, salva o schema gerado no caminho fornecido.

**Lógica Principal:**

1. Estabelece conexão com o banco de dados Firebird.
2. Consulta as tabelas de sistema do Firebird para obter todas as tabelas e views não-sistema.
3. Para cada objeto (tabela/view), executa uma consulta `SELECT COUNT(*)` para obter o número de linhas.
4. Compila os resultados em um dicionário, usando o formato `"tipo:nome"` como chave.
5. Adiciona um timestamp ao resultado final.
6. Salva o resultado em um arquivo JSON.
7. Opcionalmente, salva o schema gerado em um arquivo separado.

**Dependências Principais:**

* `fdb`: Driver Python para Firebird.
* `json`: Para manipulação de arquivos JSON.
* `logging`: Para registro de logs.
* `argparse`: Para processamento de argumentos de linha de comando.
* `src.core.logging_config`: Para configuração de logs.
* `src.core.config`: Para constantes e configurações padrão.

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

## 6. `generate-ai-description-openia.py` (Refatorado)

**Propósito:**

Este script utiliza a API da OpenAI (modelo `gpt-3.5-turbo`) para gerar descrições de negócio para colunas de um schema de banco de dados. Ele é projetado para ler um arquivo de **schema já mesclado** (que combina estrutura técnica, metadados manuais e possivelmente descrições de IA de execuções anteriores) e gerar descrições apenas para as colunas que ainda não as possuem.

**Uso:**

Execute o script da raiz do projeto. O script automaticamente lê o schema mesclado e salva as descrições geradas no arquivo de saída configurado.

```bash
# Execução padrão (usa arquivos definidos em config.py)
python scripts/generate-ai-description-openia.py

# Forçar regeneração de todas as descrições
python scripts/generate-ai-description-openia.py --force_regenerate

# Limitar o número de colunas processadas (para teste)
python scripts/generate-ai-description-openia.py --max_items 50
```

**Configuração:**

*   **Chave API OpenAI:** Lida de `.streamlit/secrets.toml` (chave `openai.api_key`) ou da variável de ambiente `OPENAI_API_KEY`.
*   **Arquivos:** Os caminhos para o arquivo de schema mesclado de entrada e o arquivo de descrições geradas de saída são definidos pelas constantes `MERGED_SCHEMA_FOR_EMBEDDINGS_FILE` e `AI_DESCRIPTIONS_FILE` em `src/core/config.py`.

**Argumentos Principais:**

*   `-i`, `--input`: Schema mesclado de entrada (padrão: `config.MERGED_SCHEMA_FOR_EMBEDDINGS_FILE`).
*   `-o`, `--output`: Arquivo de saída para descrições geradas (padrão: `config.AI_DESCRIPTIONS_FILE`).
*   `--force_regenerate`: Ignora todas as verificações e tenta gerar descrição para todas as colunas encontradas na entrada.
*   `--max_items`: Limita o número de colunas a processar.

**Input:**

*   `args.input`: Arquivo JSON do schema mesclado (ex: `data/processed/merged_schema_for_embeddings.json`). Contém a estrutura técnica e pode conter `business_description` ou `ai_generated_description` já preenchidos.
*   `args.output` (se `--force_regenerate` não for usado): Lido no início para identificar colunas já processadas nesta execução ou em execuções anteriores não forçadas.
*   `ROW_COUNTS_FILE` (Opcional): Usado para pular tabelas/views com contagem de linhas 0.

**Output:**

*   `args.output`: Arquivo JSON (ex: `data/metadata/ai_generated_descriptions_openai_35turbo.json`) contendo uma **lista** de dicionários, cada um representando uma descrição gerada. Inclui:
    *   `object_type`, `object_name`, `column_name`.
    *   `generated_description`: A descrição gerada pela OpenAI.
    *   `model_used`: ("gpt-3.5-turbo").
    *   `generation_timestamp`: Data/hora da geração (UTC).
*   **Logs:** Informações detalhadas sobre o processo, itens pulados (e motivo), erros e resumo final.

**Lógica Principal:**

1.  Parsear argumentos.
2.  Verificar existência da chave API OpenAI.
3.  Carregar schema mesclado (`args.input`).
4.  Carregar contagens de linhas (opcional).
5.  Carregar descrições AI já existentes do arquivo de saída (`args.output`), se não estiver forçando a regeneração.
6.  Preparar lista de colunas a processar, iterando sobre o schema mesclado.
7.  Iterar sobre as colunas a processar:
    a.  Pular objeto (tabela/view) se a contagem de linhas for 0 (se disponível).
    b.  Se não forçar regeneração, pular coluna se:
        i.  Já possui `business_description` (manual) no schema de entrada.
        ii. Já possui `ai_generated_description` no schema de entrada.
        iii.Não possui `sample_values` (amostra de dados vazia/nula).
        iv. Já existe uma entrada para ela no arquivo de saída carregado.
    c.  Se não pulou, construir o prompt usando `build_prompt`. O prompt inclui:
        *   Nome da tabela/view e coluna.
        *   Tipo técnico.
        *   Informação se é PK ou FK (incluindo tabela/coluna referenciada).
        *   Descrição técnica original.
        *   Até 10 amostras de valores (`sample_values`).
    d.  Chamar a API `client.chat.completions.create` da OpenAI com o prompt e modelo `gpt-3.5-turbo`.
    e.  Se a geração for bem-sucedida, adicionar o resultado à lista (`results_list`).
    f.  Registrar erros e aplicar pausas se necessário (ex: RateLimit).
8.  Salvar a `results_list` completa (ou atualizada) no arquivo de saída (`args.output`).
9.  Logar resumo da execução.

**Dependências Principais:**

*   `openai` (>= 1.0)
*   `python-dotenv`
*   `toml`
*   `tqdm`
*   Módulos internos: `src.core.utils`, `src.core.logging_config`, `src.core.config`.

---

## `extract_technical_schema.py`

**Propósito:**

Este script unificado conecta-se ao banco de dados Firebird e extrai um schema técnico detalhado e abrangente. Ele consolida a extração de estrutura de tabelas e views, tipos de dados, nulidade, valores padrão, constraints (PKs, FKs), análise estrutural (PKs compostas, tabelas de junção) e busca amostras de dados para cada coluna.

**Fluxo de Execução:**

1.  **Conexão:** Estabelece conexão com o banco de dados Firebird utilizando as credenciais definidas em `src/core/config.py` e a senha de `.streamlit/secrets.toml`.
2.  **Extração de Metadados:** Executa consultas SQL nas tabelas de sistema do Firebird (`RDB$RELATIONS`, `RDB$RELATION_FIELDS`, `RDB$FIELDS`, `RDB$RELATION_CONSTRAINTS`, etc.) para obter informações sobre tabelas, views, colunas, tipos, defaults, constraints e descrições (comentários) do banco.
3.  **Processamento:** Organiza os dados brutos, mapeia tipos de dados, extrai valores padrão, identifica PKs, FKs, tabelas com PKs compostas e tabelas de junção.
4.  **Busca de Amostras:** Para cada coluna (exceto BLOBs) em cada tabela e view, executa uma query `SELECT FIRST 50 DISTINCT ... WHERE ... IS NOT NULL` para obter até 50 valores distintos não nulos como amostra.
5.  **Montagem do JSON:** Constrói uma estrutura de dicionário aninhada contendo todos os detalhes extraídos para cada tabela/view e suas colunas.
6.  **Salvamento:** Salva o dicionário completo no arquivo JSON especificado.
7.  **Fechamento:** Fecha a conexão com o banco de dados.

**Saída:**

*   **Arquivo:** `data/processed/technical_schema_from_db.json`
*   **Estrutura:** Um dicionário JSON onde as chaves são os nomes das tabelas e views. Cada valor é um dicionário contendo:
    *   `object_type`: ("TABLE" ou "VIEW")
    *   `description`: Descrição/comentário da tabela/view (se existir no DB).
    *   `columns`: Uma lista de dicionários, um para cada coluna, contendo:
        *   `name`: Nome da coluna.
        *   `type`: Tipo de dado mapeado (ex: "VARCHAR", "INTEGER", "TIMESTAMP", "BLOB").
        *   `nullable`: (boolean) Indica se a coluna permite nulos.
        *   `default_value`: Valor padrão da coluna (extraído da definição do DB).
        *   `description`: Descrição/comentário da coluna (se existir no DB).
        *   `is_pk`: (boolean) Se a coluna faz parte da chave primária (apenas para tabelas).
        *   `is_fk`: (boolean) Se a coluna faz parte de alguma chave estrangeira (apenas para tabelas).
        *   `fk_references`: Detalhes da primeira FK encontrada para a coluna (se `is_fk` for true).
        *   `sample_values`: Lista de até 50 valores distintos não nulos, ou `[]` se vazia/só nulos, ou `None` se erro/BLOB.
        *   Outros campos inicializados como `None` para enriquecimento futuro (`business_description`, `value_mapping_notes`, etc.).
    *   `_analysis` (Chave no nível raiz do JSON): Contém resultados da análise estrutural:
        *   `composite_pk_tables`: Lista de nomes de tabelas com PK composta.
        *   `junction_tables`: Lista de nomes de tabelas identificadas como de junção.
        *   `fk_definitions`: Dicionário detalhando as definições lógicas de FKs (incluindo colunas e nomes das constraints).

**Dependências:**

*   Bibliotecas Python: `fdb`, `streamlit`.
*   Configuração: `src/core/config.py` (para caminho do DB, usuário, charset).
*   Segredos: `.streamlit/secrets.toml` (para a senha do banco em `[database] password`).
*   Utilitários: `src/core/logging_config.py`, `src/utils/json_helpers.py`.

**Execução:**

Execute a partir do diretório raiz do projeto:
```bash
python -m scripts.extract_technical_schema
```

**Notas Importantes:**

*   **Performance:** A etapa de busca por amostras de dados (`fetch_column_samples`) pode ser **significativamente demorada**, especialmente para bancos com muitas tabelas/colunas ou tabelas muito grandes.
*   **Tipos de Dados:** A conversão de tipos de dados do Firebird (Timestamp, Decimal, Bytes) para JSON é realizada. Amostras de BLOBs não são buscadas.
*   **Erros:** Erros durante a busca de amostras para colunas específicas são registrados como warnings, e o campo `sample_values` ficará como `None` para essa coluna. Erros fatais (ex: conexão, queries de metadados) abortam o script.
*   **Precisão:** A acurácia da extração depende da correção dos metadados no próprio banco Firebird.
*   **Apenas Leitura:** O script realiza apenas operações de leitura no banco de dados.

**Funções Principais:**

*   `parse_default_value(default_source)`: Analisa a string `RDB$DEFAULT_SOURCE` para extrair o valor padrão.
*   `map_fb_type(type_code, sub_type, scale)`: Mapeia códigos de tipo numéricos do Firebird para nomes de tipo legíveis (ex: VARCHAR, INTEGER).
*   `convert_to_json_serializable(value)`: Converte tipos Python não serializáveis em JSON (datas, decimais, bytes) para formatos compatíveis.
*   `fetch_column_samples(conn, table_name, column_name, column_type)`: Busca amostras de dados distintos e não nulos para uma coluna específica.
*   `extract_full_schema_from_db(conn)`: Orquestra toda a extração, processamento de metadados, análise de chaves e busca de amostras.
*   `main()`: Gerencia a conexão com o banco, chama a função de extração principal, e salva o resultado no arquivo JSON.

---

## `extract_new_schema_firebird.py`

**Propósito:**

Este script conecta-se ao banco de dados Firebird para extrair a estrutura técnica atual (tabelas, views, colunas, tipos, PKs, FKs). Em seguida, ele tenta carregar descrições de negócio e notas de mapeamento de valores de um arquivo de metadados existente (`data/metadata/schema_metadata.json`). Finalmente, combina a estrutura extraída do banco com as descrições carregadas, gerando um novo arquivo JSON (`data/metadata/generated_schema_structure.json`) que representa o schema atualizado, pronto para revisão e edição manual (seja no próprio arquivo ou através da interface Streamlit). O objetivo principal é atualizar a estrutura técnica no arquivo JSON, preservando o máximo possível dos metadados manuais já inseridos.

**Uso:**

Execute o script da raiz do projeto:

```bash
python scripts/extract_new_schema_firebird.py
```

**Configuração:**

As credenciais e parâmetros de conexão com o banco de dados Firebird são obtidos na seguinte ordem de prioridade:

1.  **Variáveis de Ambiente (ou arquivo `.env`):** Busca por:
    *   `FIREBIRD_HOST` (padrão: "localhost")
    *   `FIREBIRD_PORT` (padrão: 3050)
    *   `FIREBIRD_DB_PATH` (**Obrigatório:** Caminho para o arquivo .fdb)
    *   `FIREBIRD_USER` (padrão: "SYSDBA")
    *   `FIREBIRD_PASSWORD` (**Obrigatório**)
    *   `FIREBIRD_CHARSET` (padrão: "WIN1252")
    *   **Nota:** Ao contrário de outros scripts, este **não** parece usar `.streamlit/secrets.toml` diretamente, priorizando variáveis de ambiente.

**Input:**

*   Acesso a um banco de dados Firebird válido, com as credenciais corretas configuradas nas variáveis de ambiente ou `.env`.
*   **`data/metadata/schema_metadata.json`** (Opcional): Se este arquivo existir, suas descrições (`business_description`, `source_description`, `value_mapping_notes`) serão lidas e mescladas com a estrutura extraída do banco. O `_GLOBAL_CONTEXT` também será preservado. Se não existir, o arquivo de saída conterá apenas a estrutura técnica com campos de descrição vazios.

**Output:**

*   **`data/metadata/generated_schema_structure.json`:** Um arquivo JSON contendo a estrutura do schema combinada. A estrutura principal é:
    *   `_GLOBAL_CONTEXT`: Preservado do arquivo de metadados original, se existir.
    *   `schema_objects`: Uma lista de dicionários ordenados (OrderedDict), um para cada tabela/view.
        *   Cada objeto contém: `name`, `type`, `business_description`, `value_mapping_notes`, `source_description`, `text_for_embedding` (inicializado como vazio), e uma lista `columns`.
        *   Cada coluna na lista `columns` é um dicionário ordenado com: `name`, `type`, `is_pk`, `is_fk`, `fk_references` (detalhes da referência), `business_description`, `value_mapping_notes`, `source_description`, `text_for_embedding` (inicializado como vazio).
    *   Os nomes de tabelas/views e colunas são padronizados para MAIÚSCULAS.

**Lógica Principal:**

1.  Carrega configuração de ambiente (`.env`).
2.  Configura logging.
3.  Define caminhos para os arquivos de entrada e saída.
4.  Define mapeamento de tipos Firebird.
5.  Define função `load_original_descriptions` para carregar metadados do `schema_metadata.json` (se existir) em um dicionário para lookup rápido (chaves são tuplas `(NOME_TABELA_UPPER, NOME_COLUNA_UPPER)`).
6.  Define função `