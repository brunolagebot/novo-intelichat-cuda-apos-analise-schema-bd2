# Roadmap - Executar Geração de Descrições com OpenAI

Passos necessários para executar o script `scripts/generate-ai-description-openia.py`:

- [ ] **Verificar Arquivos de Entrada:**
    - [ ] Confirmar a existência e o formato correto do arquivo de schema técnico: `data/enhanced_technical_schema.json`.
    - [ ] (Opcional, mas recomendado) Confirmar a existência do arquivo com descrições manuais: `data/combined_schema_details.json`. O script pode rodar sem ele, mas não pulará itens com descrição manual.
    - [ ] (Opcional, mas recomendado) Confirmar a existência do arquivo com contagem de linhas: `data/overview_counts.json`. O script pode rodar sem ele, mas não pulará tabelas/views vazias.

- [ ] **Configurar Chave da API OpenAI:**
    - [ ] Garantir que a chave da API OpenAI esteja configurada corretamente. Verificar uma das seguintes opções:
        - Arquivo `.streamlit/secrets.toml` com a entrada `[openai]` e `api_key = "SUA_CHAVE_AQUI"`.
        - Variável de ambiente `OPENAI_API_KEY` definida com sua chave.

- [ ] **Instalar Dependências:**
    - [ ] Certificar-se de que todas as dependências Python estão instaladas. Se houver um arquivo `requirements.txt`, execute: `pip install -r requirements.txt`. Caso contrário, instale individualmente: `pip install openai toml tqdm python-dotenv`.

- [ ] **Executar o Script:**
    - [ ] Navegar até o diretório raiz do projeto (`c:\Projetos\Novo`) no terminal.
    - [ ] Executar o script: `python scripts/generate-ai-description-openia.py`
    - [ ] (Opcional - Teste) Para testar com um número limitado de itens (ex: 5): `python scripts/generate-ai-description-openia.py --max_items 5`
    - [ ] (Opcional - Regenerar) Para forçar a regeneração de todas as descrições: `python scripts/generate-ai-description-openia.py --force_regenerate`

- [ ] **Verificar Saída:**
    - [ ] Após a execução, verificar o arquivo de saída `data/ai_generated_descriptions_openai_35turbo.json` para as descrições geradas.
