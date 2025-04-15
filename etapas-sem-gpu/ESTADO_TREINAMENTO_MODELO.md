# Estado do Treinamento do Modelo e Análise de Schema Firebird

Este documento descreve o processo atual para extrair, documentar, analisar e treinar um modelo de linguagem (LLM) com base no esquema e significado do banco de dados Firebird (`DADOS.FDB`).

## Objetivo

O objetivo principal desta fase é ensinar ao modelo de linguagem (Llama 3 8B) sobre:

1.  A **estrutura técnica** do banco de dados Firebird: tabelas, views, colunas, tipos de dados, chaves primárias e estrangeiras, contagens de referência FK.
2.  O **significado semântico e de negócio** dessas estruturas: o que cada tabela representa, o propósito de cada coluna, e o significado de valores específicos (mapeamentos).

## Processo Implementado

1.  **Extração do Esquema Técnico:**
    *   **Ferramenta:** Script `scripts/extract_schema.py`.
    *   **Entrada:** Conexão direta ao banco `DADOS.FDB` (configurado via `.env`).
    *   **Saída:** Arquivo `data/technical_schema_details.json`, contendo a estrutura técnica e a chave `fk_reference_counts`.

2.  **Anotação Interativa e Análise (Streamlit):**
    *   **Ferramenta:** Aplicação web `streamlit_app.py`.
    *   **Funcionalidades Atuais:**
        *   **Três Modos de Operação:**
            *   **Editar Metadados:** Permite navegar por tabelas/views (estrutura de `combined_schema_details.json`), visualizar exemplos de dados e editar descrições/notas de mapeamento que são salvas no `etapas-sem-gpu/schema_metadata.json`. Inclui heurísticas e sugestões de IA (Ollama).
            *   **Visão Geral:** Exibe uma tabela sumarizada com o status da documentação (baseada em `combined_schema_details.json`), contagem de linhas (de `data/overview_counts.json`) e data da contagem.
            *   **Análise:** Exibe uma tabela com as colunas mais referenciadas por chaves estrangeiras (baseada na chave `fk_reference_counts` dentro de `combined_schema_details.json`).
        *   **Carregamento de Dados:**
            *   Carrega `etapas-sem-gpu/schema_metadata.json` para edição.
            *   Carrega `data/combined_schema_details.json` para exibição técnica, visão geral e análise.
            *   Carrega `data/overview_counts.json` para exibir contagens na visão geral.
        *   **Anotação:** Permite editar descrições para objetos e colunas, e notas de mapeamento para colunas.
        *   **Contagem:** Permite executar `scripts/calculate_row_counts.py` para atualizar o cache de contagens.
        *   **Persistência:** Salva edições em `etapas-sem-gpu/schema_metadata.json`.

3.  **Mesclagem dos Dados:**
    *   **Ferramenta:** Script `scripts/merge_schema_data.py`.
    *   **Entrada:** `data/technical_schema_details.json` e `etapas-sem-gpu/schema_metadata.json`.
    *   **Saída:** Arquivo `data/combined_schema_details.json`, combinando estrutura técnica, contagens de FK e descrições/notas manuais. Este arquivo é a base para a exibição de dados combinados no Streamlit.

4.  **Fine-Tuning do LLM (Llama 3 8B):**
    *   **Modelo Base:** `meta-llama/Meta-Llama-3-8B-Instruct`
    *   **Dados:** Um dataset (presumivelmente no formato `.jsonl`) foi gerado (processo não detalhado neste arquivo, mas provavelmente usando os dados combinados) e usado para o fine-tuning.
    *   **Método:** Fine-tuning com LoRA (Low-Rank Adaptation).
    *   **Duração:** Aproximadamente 84 horas (3.5 dias).
    *   **Resultados:**
        *   O adaptador LoRA treinado (pesos específicos do schema) está salvo em `results-llama3-8b-chat-schema-adapter/adapter_model.safetensors`.
        *   As métricas finais de treinamento (`train_results.json`) indicam uma perda (loss) de aproximadamente **0.70**, sugerindo um bom aprendizado.
        *   Outros arquivos no diretório `results-llama3-8b-chat-schema-adapter/` incluem a configuração do adaptador, argumentos de treinamento e um checkpoint.

## Resumo do Progresso (Julho 2024)

*   **Extração de Esquema:** Completa e funcional.
*   **Anotação e Análise (Streamlit):** Aplicação funcional com múltiplos modos (Edição, Visão Geral, Análise).
*   **Mesclagem de Dados:** Funcional, combina dados técnicos e manuais.
*   **Fine-tuning LLM (Schema):** Concluído com sucesso para Llama 3 8B. O adaptador LoRA está disponível em `results-llama3-8b-chat-schema-adapter/`.

## Próxima Etapa Clara

1.  **Implementar a Inferência:** Criar ou adaptar um script (ex: similar a `scripts/run_inference.py` de projetos anteriores) que:
    *   Carregue o modelo base Llama 3 8B Instruct.
    *   Carregue o adaptador LoRA específico do schema de `results-llama3-8b-chat-schema-adapter/`.
    *   Permita ao usuário fazer perguntas sobre o schema Firebird e receber respostas geradas pelo modelo ajustado.
2.  **(Opcional)** Avaliar a qualidade das respostas geradas pelo modelo ajustado.
3.  **(Opcional)** Considerar carregar múltiplos adaptadores (se o adaptador em `results-llama3-8b-chat-adapter/` for relevante para a tarefa final). 