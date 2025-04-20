# Roadmap de Organização e Refatoração (19/04)

Este documento descreve as etapas planejadas para melhorar a organização, manutenibilidade e escalabilidade do projeto.

## Fase 1: Organização Estrutural e Consistência

**Objetivo:** Melhorar a clareza da estrutura do projeto e garantir o uso consistente de configurações.

1.  **Reorganizar `scripts/`:**
    *   [ ] Criar subdiretórios por finalidade (ex: `data_preparation`, `ai_tasks`, `analysis`, `database`).
    *   [ ] Mover scripts existentes para os subdiretórios apropriados.
    *   [ ] **Identificar** lógica reutilizável (conexão DB, I/O JSON, etc.) para refatoração futura (Fase 2).
    *   [ ] Mover `analyze_schema_completeness.py` para `scripts/analysis/`.
2.  **Refatorar `src/core/`:**
    *   [ ] Mover `db_utils.py` para `src/database/`.
    *   [ ] Mover `ai_integration.py` para `src/ollama_integration/` (ou `src/ai/`).
    *   [ ] Mover `analysis.py` para `src/analysis/` (criar pasta).
    *   [ ] Criar `src/analysis/schema_analysis.py` para lógica de análise de completude e qualidade do schema.
    *   [ ] Atualizar todas as importações nos arquivos afetados (`streamlit_app.py`, outros scripts/módulos).
3.  **Consistência de Caminhos:**
    *   [ ] Revisar **todos** os scripts em `scripts/` (após reorganização).
    *   [ ] Garantir que todos os caminhos de arquivos (entrada/saída) usem as constantes definidas em `src/core/config.py`.
    *   [ ] Atualizar `analyze_schema_completeness.py` para usar constantes de `config.py` como caminho padrão.
    *   [ ] Remover caminhos hardcoded ou inconsistentes.

## Fase 2: Refatoração e Robustez do Workflow

**Objetivo:** Reduzir duplicação de código, tornar os scripts mais robustos e documentar o fluxo de dados.

1.  **Refatorar Lógica dos Scripts para `src/`:**
    *   [ ] Implementar funções reutilizáveis em `src/database/`, `src/utils/`, `src/analysis/`, `src/ai/` para lógica identificada na Fase 1.
    *   [ ] Simplificar os scripts em `scripts/` para importar e orquestrar essas funções de `src/`.
    *   [ ] Refatorar `analyze_schema_completeness.py` para usar `src.utils.json_helpers` para operações de I/O.
    *   [ ] Mover a lógica de análise de `analyze_schema_completeness.py` para `src/analysis/schema_analysis.py`.
2.  **Robustecer Workflow de Dados:**
    *   [ ] Modificar scripts em `scripts/` para verificar a existência de arquivos de entrada necessários no início da execução.
    *   [ ] Fazer os scripts falharem com mensagens de erro claras se as dependências não forem encontradas.
    *   [ ] Adicionar verificação em `src/core/data_loader.py` para garantir que o `schema_file_to_load` existe antes de tentar carregá-lo.
    *   [ ] Adicionar validação de schema em `analyze_schema_completeness.py` para garantir formato correto dos dados.
3.  **Documentar Workflow:**
    *   [ ] Criar ou atualizar `README.md` ou `docs/WORKFLOW.md` descrevendo a sequência correta de execução dos scripts de preparação de dados.
    *   [ ] Documentar como `analyze_schema_completeness.py` se integra ao fluxo de análise de qualidade dos metadados.

## Fase 3: Limpeza e Testes (Menor Prioridade Inicial)

**Objetivo:** Remover código obsoleto e adicionar cobertura de testes.

1.  **Revisar/Remover Código Antigo:**
    *   [ ] Analisar o conteúdo de `archive/` e `scripts/old/`.
    *   [ ] Remover arquivos e códigos que não são mais relevantes ou úteis.
2.  **Implementar Testes:**
    *   [ ] Adicionar testes unitários para funções críticas e reutilizáveis em `src/`.
    *   [ ] Implementar testes para as funções de análise de schema em `src/analysis/schema_analysis.py`.
    *   [ ] (Opcional) Adicionar testes de integração para fluxos chave dos scripts.

## Fase 4: Expansão de Funcionalidades de Análise

**Objetivo:** Expandir as capacidades de análise para melhorar a qualidade dos metadados e embeddings.

1.  **Melhorar Análise de Completude:**
    *   [ ] Adicionar opção para exportar resultados em JSON/CSV no script `analyze_schema_completeness.py`.
    *   [ ] Implementar visualização gráfica dos resultados de completude (gráficos de barras/pizza).
    *   [ ] Adicionar análise de tendências ao longo do tempo (comparando resultados históricos).
2.  **Análise de Embeddings:**
    *   [ ] Criar script `analyze_embeddings_quality.py` para avaliar a qualidade dos embeddings gerados.
    *   [ ] Implementar métricas de similaridade entre colunas relacionadas para validar embeddings.
    *   [ ] Adicionar análise de cobertura de embeddings em relação aos metadados existentes.
3.  **Dashboards de Qualidade:**
    *   [ ] Integrar resultados de análise de completude na UI principal do Streamlit.
    *   [ ] Criar página dedicada para visualizar métricas de qualidade e completude do schema.

---

**Observação:** As fases podem ter alguma sobreposição. A prioridade inicial é a Fase 1 para estabelecer uma estrutura mais clara. 