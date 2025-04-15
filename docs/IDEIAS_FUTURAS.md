# Roadmap e Ideias Futuras para o Chatbot Ollama

Este documento lista potenciais melhorias e funcionalidades a serem implementadas no projeto.

## Funcionalidades Implementadas (Principais)

*   Integração com API `/api/chat` do Ollama.
*   Interface web básica com Gradio (`gr.Blocks`).
*   Streaming de respostas na UI.
*   Manutenção do histórico de conversa por sessão.
*   Seleção de modelo Ollama na UI.
*   Salvamento persistente do histórico de chat em BD SQLite (`chat_history.db`).
*   Pré-processamento básico da entrada do usuário (limpeza de espaços).
*   Testes unitários com mock para a integração com Ollama.
*   Teste de integração (marcado como `slow`).
*   Teste unitário para pré-processamento.
*   Configuração externa via `.env` (URL API, Modelo Padrão, Nível de Log).
*   Verificação/Instalação automática de dependências ao iniciar `app.py`.
*   Exibição do tempo total de resposta na UI.

## Próximos Passos e Ideias Futuras

### UI/UX (Interface e Experiência do Usuário)

*   **[Em Progresso]** Botões de Feedback (👍/👎) para as respostas do assistente.
*   Botão "Limpar Chat" para reiniciar a conversa na interface.
*   Exibição de um indicador de "pensando..." ou timer *durante* a geração da resposta (mais complexo com `gr.Blocks`).
*   Permitir configurar parâmetros do modelo na UI (ex: temperatura, top_p).
*   Adicionar exemplos de prompts iniciais.
*   Melhorar layout e estilização geral da interface.
*   Exibir mensagens de erro de forma mais amigável na interface.

### Core / Backend

*   **Pós-processamento:** Implementar limpeza/formatação da resposta do LLM (`src/core/processing.py`).
*   **Expandir Pré-processamento:** Adicionar mais regras (ex: normalização de texto).
*   **RAG (Retrieval-Augmented Generation):**
    *   Implementar busca em base de conhecimento local (ex: arquivos de texto, PDFs) para fornecer contexto adicional ao LLM antes de responder.
    *   Integrar com APIs externas para buscar informações em tempo real (ex: clima, notícias).
*   **Gerenciamento de Contexto:** Implementar estratégias mais avançadas para gerenciar o histórico enviado ao LLM (ex: sumarização de conversas longas, janelas deslizantes) para evitar exceder limites de contexto.
*   **Lógica de Comandos:** Implementar detecção de comandos específicos na entrada do usuário (ex: `/resumir`, `/buscar`).

### Testes

*   Expandir cobertura de testes unitários, especialmente para novas funcionalidades no `src/core`.
*   Implementar testes de ponta a ponta (e2e) usando ferramentas como Playwright ou Selenium (simulam interação do usuário na interface web).

### Fine-tuning

*   **Preparação de Dados:** Desenvolver scripts para extrair, limpar, anonimizar e formatar os dados do `chat_history.db`.
*   **Seleção de Estratégia/Ferramenta:** Escolher a melhor abordagem (local com `trl`, serviço de nuvem, etc.) com base nos recursos e objetivos.
*   **Execução e Avaliação:** Realizar o fine-tuning e avaliar o desempenho do modelo customizado.
*   **Integração (Opcional):** Integrar o modelo ajustado de volta à aplicação (ex: importando no Ollama, se possível).

### Deploy e Operações

*   **Dockerização:** Criar um `Dockerfile` para empacotar a aplicação e suas dependências, facilitando o deploy.
*   **Configuração de Ambientes:** Estruturar melhor a configuração para diferentes ambientes (dev, prod).
*   **Monitoramento:** Adicionar monitoramento básico da aplicação (ex: saúde, uso de recursos). 