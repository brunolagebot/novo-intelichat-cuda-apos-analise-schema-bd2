# Layout do Arquivo `generated_schema_structure.json`

Este documento descreve a estrutura do arquivo JSON gerado pelo script `scripts/extract_new_schema_firebird.py`. Este arquivo combina informações estruturais extraídas diretamente do banco de dados Firebird com descrições textuais (quando disponíveis) do arquivo `data/metadata/schema_metadata.json` original.

## Estrutura Geral

O JSON possui duas chaves principais no nível raiz:

```json
{
  "_GLOBAL_CONTEXT": { ... },
  "schema_objects": [ ... ]
}
```

1.  **`_GLOBAL_CONTEXT`** (Objeto):
    *   **Origem:** Copiado do arquivo `data/metadata/schema_metadata.json` original.
    *   **Conteúdo:** Mantém o conteúdo original desta chave. Se a chave não existir no arquivo original ou não for um dicionário válido, será um objeto vazio (`{}`). As chaves dentro deste objeto são ordenadas alfabeticamente.
    *   **Propósito:** Armazenar informações globais ou de contexto relevantes para o schema.

2.  **`schema_objects`** (Array de Objetos):
    *   **Origem:** Gerado a partir da consulta às tabelas de sistema do Firebird (`RDB$RELATIONS`).
    *   **Conteúdo:** Uma lista contendo um objeto para cada tabela ou view encontrada no banco de dados (excluindo objetos de sistema). A ordem dos objetos na lista corresponde geralmente à ordem alfabética dos nomes das tabelas/views.
    *   **Propósito:** Descrever individualmente cada tabela e view do schema.

## Estrutura de um Objeto Tabela/View (dentro de `schema_objects`)

Cada objeto dentro do array `schema_objects` representa uma tabela ou view e possui as seguintes chaves (mantidas em ordem pelo uso de `OrderedDict`):

```json
{
  "name": "NOME_DA_TABELA_OU_VIEW", // (String) Nome extraído do banco (convertido para MAIÚSCULAS).
  "type": "TABLE" | "VIEW",         // (String) Tipo do objeto ('TABLE' ou 'VIEW') extraído do banco.
  "business_description": "...",   // (String) Descrição de negócio. Origem: `schema_metadata.json` original (nível da tabela/view). Padrão: ""
  "value_mapping_notes": "...",    // (String) Notas sobre mapeamento de valores. Origem: `schema_metadata.json` original (adicionado aqui, pode estar vazio). Padrão: ""
  "source_description": "...",     // (String) Descrição da origem dos dados. Origem: `schema_metadata.json` original (nível da tabela/view). Padrão: ""
  "text_for_embedding": "",        // (String) Campo reservado para texto combinado a ser usado em embeddings. Padrão: "" (gerado vazio).
  "columns": [ ... ]               // (Array de Objetos) Lista das colunas pertencentes a esta tabela/view.
}
```

## Estrutura de um Objeto Coluna (dentro de `columns`)

Cada objeto dentro do array `columns` representa uma coluna da tabela/view pai e possui as seguintes chaves (mantidas em ordem):

```json
{
  "name": "NOME_DA_COLUNA",          // (String) Nome da coluna extraído do banco (convertido para MAIÚSCULAS).
  "type": "VARCHAR(100)" | "INTEGER" | "DATE" | "BLOB TEXT" | "DECIMAL", // (String) Tipo de dado da coluna mapeado do Firebird. Inclui tamanho para tipos aplicáveis (VARCHAR, CHAR) e subtipo para BLOB. Tipos NUMERIC/DECIMAL podem não ter precisão/escala detalhada dependendo da extração.
  "is_pk": true | false,             // (Boolean) Indica se a coluna faz parte da chave primária. Origem: Banco de Dados.
  "is_fk": true | false,             // (Boolean) Indica se a coluna faz parte de uma chave estrangeira. Origem: Banco de Dados.
  "fk_references": null | {          // (Objeto | null) Se `is_fk` for true, contém detalhes da referência. Origem: Banco de Dados.
    "references_table": "TABELA_REFERENCIADA", // (String) Nome da tabela referenciada pela FK (MAIÚSCULAS).
    "references_column": "COLUNA_REFERENCIADA" // (String) Nome da coluna referenciada pela FK (MAIÚSCULAS).
  },
  "business_description": "...",   // (String) Descrição de negócio da coluna. Origem: `schema_metadata.json` original (nível da coluna). Padrão: ""
  "value_mapping_notes": "...",    // (String) Notas sobre mapeamento de valores da coluna. Origem: `schema_metadata.json` original (nível da coluna, chave `value_mapping_notes`). Padrão: ""
  "source_description": "...",     // (String) Descrição da origem dos dados da coluna. Origem: `schema_metadata.json` original (nível da coluna). Padrão: ""
  "text_for_embedding": ""         // (String) Campo reservado para texto combinado da coluna a ser usado em embeddings. Padrão: "" (gerado vazio).
  // "is_not_null": true | false, // (Boolean, Opcional) Poderia ser adicionado para indicar se a coluna permite nulos (1 = NOT NULL no Firebird). Origem: Banco de Dados.
}
```

## Exemplo

```json
{
  "_GLOBAL_CONTEXT": {
    "context_key_1": "value1",
    "context_key_2": "value2"
  },
  "schema_objects": [
    {
      "name": "CLIENTES",
      "type": "TABLE",
      "business_description": "Cadastro principal de clientes da empresa.",
      "value_mapping_notes": "",
      "source_description": "Sistema de Vendas legado",
      "text_for_embedding": "",
      "columns": [
        {
          "name": "ID_CLIENTE",
          "type": "INTEGER",
          "is_pk": true,
          "is_fk": false,
          "fk_references": null,
          "business_description": "Identificador único do cliente.",
          "value_mapping_notes": "Gerado sequencialmente.",
          "source_description": "Chave primária interna.",
          "text_for_embedding": ""
        },
        {
          "name": "NOME",
          "type": "VARCHAR(100)",
          "is_pk": false,
          "is_fk": false,
          "fk_references": null,
          "business_description": "Nome completo ou razão social do cliente.",
          "value_mapping_notes": "",
          "source_description": "Campo de entrada do formulário de cadastro.",
          "text_for_embedding": ""
        },
        {
          "name": "ID_CIDADE",
          "type": "INTEGER",
          "is_pk": false,
          "is_fk": true,
          "fk_references": {
            "references_table": "CIDADES",
            "references_column": "ID_CIDADE"
          },
          "business_description": "Chave estrangeira para a tabela de cidades.",
          "value_mapping_notes": "",
          "source_description": "Relacionamento com CIDADES.",
          "text_for_embedding": ""
        }
      ]
    },
    {
      "name": "V_CLIENTES_ATIVOS",
      "type": "VIEW",
      "business_description": "Visão que mostra apenas clientes com status ativo.",
      "value_mapping_notes": "",
      "source_description": "Baseada na tabela CLIENTES com filtro de status.",
      "text_for_embedding": "",
      "columns": [
        {
          "name": "ID_CLIENTE",
          "type": "INTEGER",
          "is_pk": false, // Views geralmente não têm PK definida assim
          "is_fk": false,
          "fk_references": null,
          "business_description": "ID do cliente ativo.",
          "value_mapping_notes": "",
          "source_description": "Originado de CLIENTES.ID_CLIENTE",
          "text_for_embedding": ""
        },
        {
          "name": "NOME_CLIENTE", // Nome pode ser diferente na view
          "type": "VARCHAR(100)",
          "is_pk": false,
          "is_fk": false,
          "fk_references": null,
          "business_description": "Nome do cliente ativo.",
          "value_mapping_notes": "",
          "source_description": "Originado de CLIENTES.NOME",
          "text_for_embedding": ""
        }
      ]
    }
    // ... mais tabelas e views
  ]
}
``` 