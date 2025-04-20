# O que são Embeddings e Como Usamos Neste Projeto?

## O que são Embeddings?

Em termos simples, **embeddings** são representações numéricas (vetores) do significado semântico de um pedaço de texto (ou outros tipos de dados, como imagens). Em vez de tratar palavras apenas como sequências de caracteres, um modelo de embedding captura o "sentido" ou o contexto do texto e o traduz em uma lista de números.

Textos com significados semelhantes terão embeddings (vetores) numericamente próximos uns dos outros em um espaço vetorial.

## Como Usamos Embeddings Neste Projeto?

Neste projeto, usamos embeddings para permitir uma **busca semântica poderosa sobre o schema do banco de dados** através da interface de chat (Streamlit). O objetivo é que você possa fazer perguntas em linguagem natural sobre os dados e o sistema encontre as tabelas e colunas mais relevantes, mesmo que você não use os nomes exatos.

O fluxo é o seguinte:

1.  **Geração dos Embeddings das Colunas:**
    *   **Fonte de Texto:** O script `scripts/merge_enrich_schema.py` prepara o arquivo `data/processed/schema_enriched_for_embedding.json`. Este arquivo contém, para cada coluna, um campo `text_for_embedding` que combina informações importantes (nome da tabela, nome da coluna, tipo, descrições técnica, manual e de IA, notas, exemplos de valores).
    *   **Processo de Embedding:** O script `scripts/generate_embeddings_and_index.py` lê o arquivo acima. Para cada coluna, ele pega o `text_for_embedding` e o envia para um modelo de embedding rodando localmente via **Ollama** (especificamente, o modelo `nomic-embed-text`).
    *   **Resultado:** O Ollama retorna um vetor numérico (o embedding) para cada coluna. Esses vetores são salvos:
        *   Dentro de um novo arquivo JSON (`data/embeddings/schema_with_embeddings_*.json`), junto com os outros dados da coluna.
        *   Organizados em um **Índice FAISS** (`data/embeddings/faiss_index_*.idx`). FAISS é uma biblioteca otimizada para buscas de similaridade extremamente rápidas em grandes conjuntos de vetores.

2.  **Busca Semântica no Chat:**
    *   **Pergunta do Usuário:** Você faz uma pergunta no chat (ex: "Onde estão os dados de endereço dos fornecedores?").
    *   **Embedding da Pergunta:** A aplicação pega o texto da sua pergunta e usa o **mesmo modelo Ollama (`nomic-embed-text`)** para gerar o vetor de embedding da sua pergunta.
    *   **Busca no FAISS:** A aplicação usa o embedding da sua pergunta para consultar o índice FAISS. O FAISS rapidamente encontra os embeddings das colunas do banco de dados que são mais *semanticamente similares* ao embedding da sua pergunta.
    *   **Recuperação e Resposta:** A aplicação identifica quais colunas correspondem aos embeddings encontrados no FAISS, busca os detalhes completos dessas colunas no arquivo `schema_with_embeddings_*.json` e apresenta as informações mais relevantes (nomes, descrições, etc.) para você como resposta.

**Benefício Principal:**

Essa abordagem permite encontrar informações relevantes com base no **significado** da sua pergunta, e não apenas por correspondência exata de palavras-chave. Isso torna a exploração do schema do banco de dados muito mais intuitiva e eficiente. 