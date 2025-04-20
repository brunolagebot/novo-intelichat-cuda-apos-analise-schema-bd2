# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato é baseado em [Keep a Changelog](https://keepachangelog.com/pt-BR/1.0.0/),
e este projeto adere ao [Semantic Versioning](https://semver.org/lang/pt-BR/).

## [Não Lançada] - 2024-12-21

### Adicionado
- Docstring e resumo de execução ao script `scripts/ai_tasks/generate_embeddings_and_index.py`.
- Confirmação da lógica de salvamento da UI, garantindo que `metadata_schema_manual.json` é o único arquivo atualizado manualmente pela interface.

### Modificado
- Atualizado `src/core/config.py`:
    - Padronizados nomes dos arquivos de embeddings e FAISS (`schema_with_embeddings.json`, `faiss_index.idx`).
    - Comentada a constante `AI_DESCRIPTIONS_FILE` (gerenciada via argumentos de script).
    - Removida a constante `OUTPUT_COMBINED_FILE` por redundância/potencial confusão.
- Script `scripts/ai_tasks/generate_embeddings_and_index.py` confirmado para usar as constantes atualizadas de `config.py`.
- Refatorado `scripts/data_preparation/merge_metadata_for_embeddings.py` para usar `argparse`, ler estrutura manual corretamente e gerar `text_for_embedding`.
- Adicionada regra de resumo de execução para scripts no `README.md`.

### Removido
- Script redundante `scripts/data_preparation/merge_enrich_schema.py`.

### Corrigido
- (Registre aqui correções de bugs)

### Segurança
- (Registre aqui vulnerabilidades corrigidas)

## [0.1.0] - 2024-12-19
### Adicionado
- Versão inicial do projeto com funcionalidades básicas de visualização e edição de metadados.
- Estrutura inicial do projeto com Streamlit.
- Adicionada seção "Diretrizes de Desenvolvimento" ao `README.md`, incluindo regras para comentários e resumo de execução em scripts.
- Adicionado arquivo `CHANGELOG.md` para rastrear mudanças.
- Adicionado resumo de execução ao script `scripts/data_preparation/calculate_row_counts.py`.
- Adicionado resumo de execução e docstring ao script `scripts/data_preparation/generate_new_schema.py`. 