# Projeto de Documentação e Análise de Schema Firebird com LLM

Este projeto visa extrair, documentar interativamente e analisar o schema de um banco de dados Firebird (`DADOS.FDB`), além de facilitar o fine-tuning de um modelo de linguagem grande (LLM) com esse conhecimento específico.

## Status Atual (Julho 2024)

*   **Extração de Schema Técnico:** O script `scripts/extract_schema.py` conecta ao banco Firebird, extrai detalhes de tabelas, views, colunas (tipos, nulidade, descrições do DB), constraints (PK, FK, Unique) e calcula contagens de referência de chaves estrangeiras (`fk_reference_counts`). Salva em `data/technical_schema_details.json`.
*   **Editor/Analisador de Metadados (Streamlit):** A aplicação `streamlit_app.py` permite:
    *   **Editar Metadados:** Adicionar/modificar descrições de negócio e notas de mapeamento para tabelas, views e colunas. As edições são salvas em `etapas-sem-gpu/schema_metadata.json`. Utiliza heurísticas e sugestões de IA (via Ollama, se disponível) para auxiliar na documentação.
    *   **Visão Geral:** Exibe um resumo do status da documentação (percentual de descrição/notas), contagem de linhas (via cache ou execução sob demanda do `scripts/calculate_row_counts.py`), e data da última contagem.
    *   **Análise:** Mostra as colunas mais referenciadas por chaves estrangeiras, com base nos dados pré-calculados.
*   **Mesclagem de Dados:** O script `scripts/merge_schema_data.py` combina os dados técnicos de `technical_schema_details.json` com os metadados manuais de `schema_metadata.json`, gerando `data/combined_schema_details.json`, que é usado pelos modos "Visão Geral" e "Análise" do Streamlit.
*   **Fine-tuning do LLM:** Um fine-tuning do modelo **Llama 3 8B Instruct** foi realizado utilizando os dados do schema. O adaptador LoRA resultante e as métricas de treinamento estão salvos no diretório `results-llama3-8b-chat-schema-adapter`. O treinamento alcançou uma perda (loss) final de aproximadamente 0.70 após ~84 horas.
*   **Próxima Etapa:** Implementar a inferência para utilizar o modelo Llama 3 base com o adaptador LoRA treinado (`results-llama3-8b-chat-schema-adapter`) para responder perguntas sobre o schema.

## Componentes Principais

*   **`scripts/`**:
    *   `extract_schema.py`: Extrai schema técnico do Firebird.
    *   `calculate_row_counts.py`: Calcula contagem de linhas das tabelas/views (executado via Streamlit ou manualmente).
    *   `merge_schema_data.py`: Combina schema técnico e metadados manuais.
    *   `analyze_schema.py`: Gera análises básicas do schema (ex: tabelas/colunas mais referenciadas).
    *   `generate_schema_doc.py`: Gera documentação Markdown do schema.
    *   `run_finetune_schema_phi3.py`: (OBSOLETO/REFERÊNCIA) Script configurado para treinar Phi-3 (o treinamento efetivo usou Llama 3).
*   **`streamlit_app.py`**: Aplicação principal para visualização, edição e análise.
*   **`data/`**:
    *   `technical_schema_details.json`: Saída do `extract_schema.py`.
    *   `combined_schema_details.json`: Saída do `merge_schema_data.py`, usado pelo Streamlit.
    *   `overview_counts.json`: Cache das contagens de linhas.
*   **`etapas-sem-gpu/`**:
    *   `schema_metadata.json`: Armazena as descrições/notas manuais editadas via Streamlit.
    *   `ESTADO_TREINAMENTO_MODELO.md`: Documenta o processo de treinamento (a ser atualizado).
    *   *(Outros arquivos de referência/etapas anteriores)*
*   **`results-llama3-8b-chat-schema-adapter/`**: Contém o adaptador LoRA treinado e métricas do fine-tuning do Llama 3 8B com os dados do schema.
*   **`results-llama3-8b-chat-adapter/`**: Contém resultados de outro treinamento (provavelmente um teste ou tarefa diferente).
*   **`docs/`**:
    *   `schema_documentation.md`: Documentação gerada pelo `generate_schema_doc.py`.

## Fluxo de Trabalho Típico

1.  **(Opcional, se estrutura do DB mudou)** Executar `python scripts/extract_schema.py` para obter a estrutura técnica mais recente e recalcular contagens de FK.
2.  Executar `streamlit run streamlit_app.py`.
3.  No modo "Editar Metadados", navegar pelos objetos e adicionar/refinar descrições e notas. Salvar as alterações.
4.  **(Opcional)** No modo "Visão Geral", executar o cálculo de contagem de linhas, se necessário.
5.  Executar `python scripts/merge_schema_data.py` para atualizar o `combined_schema_details.json` com as últimas informações técnicas e manuais. Isso atualizará os dados exibidos na "Visão Geral" e "Análise".
6.  **(Opcional)** Executar `python scripts/generate_schema_doc.py` para gerar a documentação Markdown.
7.  **(Futuro)** Usar o adaptador LoRA treinado em `results-llama3-8b-chat-schema-adapter` com o modelo base Llama 3 8B para realizar inferência e responder perguntas sobre o schema.

## Configuração

1.  **Clone o repositório.**
2.  **Crie um ambiente virtual:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate # Linux/macOS
    # ou
    .venv\\Scripts\\activate # Windows
    ```
3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```
4.  **Configure o `.env`:** Crie um arquivo `.env` na raiz do projeto com as credenciais e caminho do banco Firebird:
    ```dotenv
    FIREBIRD_HOST=seu_host_ou_ip
    FIREBIRD_PORT=3050
    FIREBIRD_DB_PATH=C:\\Caminho\\Para\\Seu\\DADOS.FDB # Atenção às barras no Windows
    FIREBIRD_USER=SYSDBA
    FIREBIRD_PASSWORD=sua_senha
    FIREBIRD_CHARSET=WIN1252

    # Opcional: Para integração Ollama no Streamlit
    # OLLAMA_BASE_URL=http://localhost:11434
    # OLLAMA_MODEL=llama3:8b-instruct-q5_K_M # Ou outro modelo Ollama
    
    # Opcional: Token Hugging Face (se necessário para baixar modelos)
    # HF_TOKEN=seu_token_hf
    ```
5.  **(Opcional: Ollama)** Se for usar as sugestões de IA no Streamlit, certifique-se de que o Ollama esteja instalado, rodando e com o modelo especificado (`OLLAMA_MODEL`) disponível.

## Como Executar

*   **Editor/Analisador Streamlit:**
    ```bash
    streamlit run streamlit_app.py
    ```
*   **Scripts Individuais:** Execute os scripts individuais conforme necessário (veja Fluxo de Trabalho).

## Contribuição

Contribuições são bem-vindas. Siga as práticas padrão de fork e pull request. 