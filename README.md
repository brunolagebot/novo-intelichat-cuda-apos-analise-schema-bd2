## Geração de DataFrame do Schema

O script `scripts/generate_schema_dataframe.py` pode ser usado para gerar um arquivo CSV "achatado" contendo todas as informações de metadados técnicos e de negócios combinados do arquivo `data/combined_schema_details.json`. Isso pode ser útil para análises ou visualizações externas.

**Uso:**

```bash
python scripts/generate_schema_dataframe.py [-i CAMINHO_JSON_ENTRADA] [-o CAMINHO_CSV_SAIDA]
```

-   `-i` ou `--input`: Especifica o caminho para o arquivo JSON do schema combinado (padrão: `data/combined_schema_details.json`).
-   `-o` ou `--output`: Especifica o caminho para o arquivo CSV de saída (padrão: `data/schema_dataframe.csv`).

O script exibirá uma barra de progresso enquanto processa os objetos do schema.

A lógica principal de conversão reside no módulo `core/dataframe_generator.py`. 