# schema_metadata.json Details

Este arquivo serve como um repositório para informações adicionais sobre o schema que não podem ser extraídas automaticamente do banco de dados. Ele complementa o `schema_tecnico.json` com contexto de negócio e anotações específicas. Os principais campos são:

*   `business_description`: **Este campo é mantido manualmente.** Ele fornece descrições funcionais para tabelas, views e colunas, explicando seu propósito e uso no contexto do negócio. É crucial manter essas descrições atualizadas para garantir a precisão da documentação e auxiliar na interpretação correta dos dados. A forma como este campo é preenchido influencia o campo `source_description`.
*   `value_notes`: **Este campo também é mantido manualmente.** Ele contém anotações sobre valores específicos encontrados em certas colunas, como o significado de códigos, unidades de medida ou quaisquer outras observações relevantes que não estão diretamente presentes nos metadados técnicos. A atualização contínua dessas notas é fundamental para o entendimento completo dos dados.
*   `source_description`: **Este campo é preenchido automaticamente.** Ele indica a origem ou o método de derivação da `business_description`. Por exemplo, pode indicar se a descrição foi inserida manualmente, importada de outra fonte, ou gerada por algum processo específico. Sua atualização depende das ações realizadas no campo `business_description`.

Manter o `schema_metadata.json` atualizado, especialmente os campos manuais, é essencial para uma documentação de schema completa e útil. 