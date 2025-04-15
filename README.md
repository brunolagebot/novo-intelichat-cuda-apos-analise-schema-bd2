# Projeto de Fine-Tuning Llama 3 8B Instruct

Este projeto demonstra como realizar o fine-tuning do modelo Llama 3 8B Instruct usando a biblioteca `transformers` e a técnica PEFT (LoRA) em um dataset personalizado e, em seguida, usar o modelo ajustado para inferência.

## Estrutura do Projeto

```
/
├── scripts/
│   ├── create_dataset.py    # Script para criar/formatar o dataset (exemplo)
│   ├── run_finetune.py      # Script para executar o fine-tuning LoRA
│   └── run_inference.py     # Script para carregar o modelo ajustado e interagir
├── data/
│   └── dataset.jsonl        # Dataset de exemplo formatado
├── results-llama3-8b-chat-adapter/ # Diretório de saída do fine-tuning (adaptador LoRA)
│   └── ...
├── app.py                   # (Script inicial, pode ser removido ou adaptado)
└── requirements.txt         # Dependências Python
└── README.md                # Este arquivo
```

## Configuração do Ambiente

1.  **Clone o repositório (se aplicável):**
    ```bash
    git clone <url_do_seu_repositorio>
    cd <nome_do_diretorio>
    ```

2.  **Crie e ative um ambiente virtual:**
    ```bash
    python -m venv .venv
    # Windows (PowerShell/CMD)
    .\.venv\Scripts\activate
    # Linux/macOS
    # source .venv/bin/activate 
    ```

3.  **Instale as dependências:**
    Certifique-se de ter o `requirements.txt` criado anteriormente.
    ```bash
    pip install -r requirements.txt
    ```
    *Observação: A instalação pode levar algum tempo, especialmente para `torch`.* 

4.  **Login no Hugging Face (Necessário para Llama 3):**
    Você precisará de um token de acesso do Hugging Face com permissão para usar o Llama 3.
    ```bash
    huggingface-cli login
    # Cole seu token quando solicitado
    ```

## Preparação dos Dados

-   O fine-tuning espera um dataset no formato JSON Lines (`.jsonl`), onde cada linha é um objeto JSON.
-   Para o `SFTTrainer` com o formato de chat do Llama 3, cada linha deve idealmente conter uma chave (por exemplo, `"messages"`) com uma lista de dicionários, cada um com `"role"` (`"system"`, `"user"`, `"assistant"`) e `"content"`.
-   Exemplo (`data/dataset.jsonl`):
    ```json
    {"messages": [{"role": "user", "content": "Qual a capital da França?"}, {"role": "assistant", "content": "A capital da França é Paris."}]}
    {"messages": [{"role": "user", "content": "Você pode me ajudar com matemática?"}, {"role": "assistant", "content": "Sim, posso ajudar com matemática! Que tipo de problema você tem?"}]}
    ```
-   Você pode adaptar ou usar o script `scripts/create_dataset.py` como ponto de partida para formatar seus próprios dados.

## Executando o Fine-Tuning

-   Após configurar o ambiente e preparar o dataset (`data/dataset.jsonl` por padrão), execute o script de fine-tuning:

    ```bash
    python scripts/run_finetune.py
    ```
-   Este script irá:
    -   Carregar o modelo base Llama 3 8B Instruct.
    -   Carregar o dataset.
    -   Configurar e aplicar o adaptador LoRA.
    -   Executar o treinamento por 1 época (padrão).
    -   Salvar o adaptador treinado em `./results-llama3-8b-chat-adapter`.
    -   Executar um teste rápido de geração.

## Usando o Modelo Ajustado (Inferência)

-   Após o fine-tuning ter sido concluído com sucesso e o adaptador salvo, você pode usar o script de inferência para interagir com o modelo ajustado:

    ```bash
    python scripts/run_inference.py
    ```
-   Este script irá:
    -   Carregar o modelo base Llama 3 8B Instruct.
    -   Carregar e aplicar o adaptador LoRA de `./results-llama3-8b-chat-adapter`.
    -   Entrar em um loop onde você pode digitar prompts e ver as respostas geradas pelo modelo ajustado.
    -   Digite `sair` para encerrar.

## Próximos Passos Possíveis

-   Experimentar com mais dados de treinamento.
-   Ajustar hiperparâmetros no `run_finetune.py` (e.g., `num_train_epochs`, `learning_rate`, configurações LoRA).
-   Avaliar o modelo ajustado de forma mais rigorosa (usando métricas e um dataset de validação/teste separado).
-   Integrar o modelo ajustado em uma aplicação maior.
-   Explorar outras técnicas de PEFT ou fine-tuning completo (requer mais recursos computacionais). 