# Roteiro de Refatoração do Projeto

Este documento descreve os passos sugeridos para refatorar a estrutura do projeto, visando melhorar a organização, manutenibilidade e escalabilidade.

## Checkpoints

- [X] **1. Unificar Diretórios de Código Fonte (`core/` e `src/`)**
    - **Objetivo:** Centralizar todo o código fonte principal da aplicação em um único diretório (`src/` é recomendado).
    - **Ações:**
        - [X] Mover o conteúdo de `core/` para `src/` (ex: `src/core/` ou reorganizar em subpastas dentro de `src/`).
        - [X] Atualizar todos os imports no projeto (`streamlit_app.py`, `scripts/`, outros módulos em `src/`) para refletir a nova localização.
        - [X] Remover o diretório `core/` vazio.

- [X] **2. Reestruturar Diretório `data/`**
    - **Objetivo:** Organizar os arquivos de dados por tipo/propósito.
    - **Ações:**
        - [X] Criar subdiretórios dentro de `data/` (ex: `input/`, `processed/`, `metadata/`, `ai_outputs/raw/`, `embeddings/`, `chat/`, `logs/`).
        - [X] Mover os arquivos existentes para os subdiretórios apropriados (exceto um arquivo AI em uso).
        - [X] Atualizar a configuração central (`src/core/config.py`) com os novos caminhos para todos os arquivos de dados movidos.
        - [X] Verificar e ajustar todos os pontos do código que leem/escrevem nesses arquivos para usar os caminhos da configuração (scripts em `scripts/` ajustados).

- [ ] **3. Centralizar Configurações**
    - **Objetivo:** Garantir que todas as configurações (caminhos, nomes de modelos, URLs, credenciais padrão) estejam definidas em `src/core/config.py`.
    - **Ações:**
        - [X] Revisar `streamlit_app.py`, scripts em `scripts/` e módulos em `src/` para identificar constantes ou valores hardcoded que deveriam estar na configuração (Revisão inicial feita, caminhos identificados).
        - [X] Mover essas configurações para `src/core/config.py` (Caminhos movidos).
        - [X] Atualizar o código para importar e usar essas configurações a partir de `src/core/config.py` (Scripts em `scripts/` atualizados para usar caminhos do config).

- [ ] **4. Implementar Testes Automatizados**
    - **Objetivo:** Aumentar a confiabilidade e facilitar a manutenção futura.
    - **Ações:**
        - [ ] Criar um diretório `tests/` na raiz do projeto.
        - [ ] Configurar um framework de teste (ex: `pytest`).
        - [ ] Adicionar testes unitários para funções críticas nos módulos de `src/` (ex: `data/metadata.py`, `ai/generation.py`, `utils/`).

- [ ] **5. Melhorar Documentação Geral**
    - **Objetivo:** Facilitar o entendimento e a colaboração no projeto.
    - **Ações:**
        - [ ] Mover o `README.md` para a raiz do projeto (este passo será feito separadamente).
        - [ ] Atualizar o `README.md` na raiz com:
            - Descrição clara do projeto.
            - Instruções de configuração (ambiente, dependências, Ollama, chaves API).
            - Como executar a aplicação Streamlit.
            - Como executar os scripts principais.
        - [ ] Manter o `ui/PAGES_DOC.md` atualizado.

## Benefícios Esperados

*   Código mais organizado e fácil de navegar.
*   Configuração centralizada e mais fácil de gerenciar.
*   Maior facilidade para adicionar novas funcionalidades.
*   Maior confiabilidade através de testes.
*   Melhor documentação para novos membros da equipe ou para referência futura. 