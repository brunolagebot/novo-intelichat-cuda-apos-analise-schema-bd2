# Como Treinar o Modelo (Especialização Modular com LoRA)

Este documento descreve o processo recomendado para adicionar novos conhecimentos específicos ou habilidades ao modelo de linguagem Llama 3 8B Instruct, **após** o treinamento inicial (por exemplo, o treinamento do schema do banco de dados), usando adaptadores LoRA separados e modulares.

**Objetivo:** Ensinar novas tarefas ou informações ao modelo sem precisar refazer treinamentos longos anteriores e permitindo combinar diferentes "especializações" durante o uso.

**Princípio Central:** O modelo base Llama 3 8B **nunca** é modificado. Cada novo conhecimento é encapsulado em um **adaptador LoRA separado e pequeno**, treinado a partir do modelo base original.

## Pré-requisitos

1.  **Modelo Base:** O modelo Llama 3 8B Instruct original acessível (via Hugging Face Hub).
2.  **Ambiente Configurado:** Ambiente Python com todas as dependências (`requirements.txt`) instalado e funcionando.
3.  **Adaptador(es) Anterior(es) (Opcional, mas comum):** Você provavelmente já terá treinado um ou mais adaptadores (ex: `schema-adapter`).
4.  **Compreensão da Nova Tarefa:** Clareza sobre qual conhecimento específico você quer adicionar (ex: entender 3 novos indicadores, responder sobre um novo produto, etc.).

## Roteiro Passo a Passo

**Passo 1: Definir a Nova Tarefa/Conhecimento**

*   Seja específico sobre o que o modelo deve aprender. Quanto mais focado, melhor e mais rápido o treinamento.

**Passo 2: Preparar o Novo Dataset Específico**

*   Crie um **novo arquivo de dataset** (formato `.jsonl`) contendo exemplos *apenas* para esta nova tarefa.
*   Use o formato de mensagens:
    ```json
    {"messages": [{"role": "user", "content": "Pergunta sobre a nova tarefa..."}, {"role": "assistant", "content": "Resposta ideal para a nova tarefa..."}]}
    {"messages": [{"role": "user", "content": "Outra pergunta..."}, {"role": "assistant", "content": "Outra resposta..."}]}
    ```
*   **Nomeie o arquivo claramente**, indicando seu propósito (ex: `data/indicadores_dataset.jsonl`, `data/produto_x_dataset.jsonl`).
*   Este dataset deve ser relativamente **pequeno e focado**. Não inclua exemplos do schema ou de outras tarefas já treinadas.

**Passo 3: Preparar o Script de Fine-Tuning Específico**

*   **NÃO MODIFIQUE** os scripts existentes (`run_finetune.py`, `run_finetune_schema.py`).
*   **FAÇA UMA CÓPIA** de um script de fine-tuning funcional (ex: copie `scripts/run_finetune.py` ou `scripts/run_finetune_schema.py`).
*   **Nomeie a cópia claramente**, indicando a tarefa (ex: `scripts/run_finetune_indicadores.py`).
*   **Modifique APENAS as seguintes linhas na CÓPIA:**
    *   `dataset_file = "data/seu_novo_dataset_especifico.jsonl"` (aponte para o dataset criado no Passo 2).
    *   `adapter_output_dir = "./results-llama3-8b-chat-novo-adapter"` (defina um NOVO diretório de saída claro para este adaptador específico, ex: `./results-llama3-8b-chat-indicadores-adapter`).
*   **NÃO MUDE** o `base_model_name`. O treinamento sempre parte do modelo base original.
*   *Opcional:* Você pode ajustar hiperparâmetros como `num_train_epochs` se o dataset for muito pequeno (talvez 2 ou 3 épocas), mas geralmente 1 época é suficiente para LoRA em tarefas específicas.

**Passo 4: Executar o Fine-Tuning Específico**

*   Execute o **novo script** que você criou:
    ```bash
    python scripts/seu_novo_script_finetune.py 
    # Ex: python scripts/run_finetune_indicadores.py
    ```
*   Este treinamento deve ser **muito mais rápido** que o treinamento do schema, pois o dataset é menor.
*   Ao final, um novo adaptador LoRA será salvo no diretório de saída especificado (ex: `./results-llama3-8b-chat-indicadores-adapter`).

**Passo 5: Preparar Script de Inferência para Múltiplos Adaptadores**

*   Modifique seu script de inferência (`scripts/run_inference.py`) para carregar **múltiplos adaptadores LoRA** sobre o modelo base.
*   A lógica geral (pode variar ligeiramente com atualizações da biblioteca `peft`) é:
    1.  Carregar o modelo base (`AutoModelForCausalLM.from_pretrained(...)`).
    2.  Carregar o **primeiro** adaptador (ex: o de schema) usando `PeftModel.from_pretrained(model, path_schema_adapter, adapter_name="schema")`.
    3.  Carregar o **segundo** adaptador (ex: o de indicadores) no *mesmo* objeto `PeftModel` usando `model.load_adapter(path_indicadores_adapter, adapter_name="indicadores")`.
    4.  Carregar quantos adaptadores adicionais forem necessários da mesma forma.
    5.  (Opcional, mas recomendado) Você pode definir qual adaptador está ativo ou como eles são combinados se necessário, mas por padrão, carregar múltiplos adaptadores geralmente mescla suas influências.
*   **Importante:** Use nomes lógicos (`adapter_name`) ao carregar cada adaptador.

**Passo 6: Executar Inferência com Conhecimento Combinado**

*   Execute o script de inferência modificado:
    ```bash
    python scripts/run_inference.py
    ```
*   O modelo agora deve ser capaz de responder perguntas sobre o schema (usando o `schema-adapter`) E sobre a nova tarefa específica (usando o `indicadores-adapter`, por exemplo).

## Princípios Chave (Resumo)

*   **Modelo Base Intocável:** Sempre comece o fine-tuning de um novo adaptador a partir do modelo base original (`meta-llama/Meta-Llama-3-8B-Instruct`).
*   **Um Adaptador por Tarefa:** Crie um adaptador LoRA separado para cada novo conjunto de conhecimento ou habilidade.
*   **Datasets Pequenos e Focados:** Use datasets específicos e concisos para treinar os novos adaptadores.
*   **Scripts Separados (Opcional, mas Seguro):** Use scripts de fine-tuning distintos para cada tipo de adaptador.
*   **Combine na Inferência:** Carregue o modelo base e todos os adaptadores LoRA relevantes no momento da inferência para usar o conhecimento combinado.
*   **Nomeie Claramente:** Dê nomes descritivos aos seus datasets, scripts e diretórios de saída dos adaptadores.

## Exemplo: Adicionando 3 Indicadores

1.  **Tarefa:** Ensinar o modelo a calcular/explicar 3 indicadores específicos de negócio.
2.  **Dataset:** Criar `data/indicadores_kpi_dataset.jsonl` com exemplos Q&A sobre esses 3 indicadores.
3.  **Script:** Copiar `scripts/run_finetune.py` para `scripts/run_finetune_kpi.py`. Editar a cópia para usar `data/indicadores_kpi_dataset.jsonl` e salvar em `./results-llama3-8b-chat-kpi-adapter`.
4.  **Executar:** `python scripts/run_finetune_kpi.py` (será rápido).
5.  **Inferência:** Modificar `scripts/run_inference.py` para carregar o modelo base, depois `model = PeftModel.from_pretrained(model, "./results-llama3-8b-chat-schema-adapter", adapter_name="schema")`, e então `model.load_adapter("./results-llama3-8b-chat-kpi-adapter", adapter_name="kpi")`.
6.  **Usar:** Rodar `scripts/run_inference.py` e fazer perguntas sobre o schema ou sobre os KPIs. 