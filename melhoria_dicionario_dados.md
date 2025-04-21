# Roteiro para Melhoria do Dicionário de Dados

Este documento descreve os passos sugeridos para enriquecer o dicionário de dados (schema consolidado e metadados associados), visando facilitar a interação e a "conversa" com os dados, tanto por sistemas de IA quanto por usuários humanos.

## 1. Completude e Qualidade das Descrições

O objetivo é garantir que as descrições textuais de tabelas, views e colunas sejam claras, precisas e focadas no significado de negócio.

-   [ ] **Revisar e Refinar Descrições:**
    -   Analisar as descrições manuais existentes no `data/metadata/manual/manual_metadata_master.json`.
    -   Analisar as descrições geradas por IA (verificar arquivos em `data/ai_outputs/` ou o schema consolidado).
    -   Usar a interface "Editar Metadados" para corrigir, refinar e complementar as descrições, focando no *propósito de negócio*.
-   [ ] **Priorizar Objetos Chave:**
    -   Utilizar a página "Análise" para identificar tabelas e colunas mais importantes (PK/FK, alto nº de referências).
    -   Focar os esforços iniciais de revisão e enriquecimento nesses objetos prioritários.
-   [ ] **Descrever Views:**
    -   Assegurar que todas as Views possuam `object_business_description` claras, explicando o que representam ou qual consulta complexa elas simplificam.

## 2. Mapeamento de Valores e Notas Contextuais

Detalhar o significado de códigos e adicionar contexto sobre regras de negócio é crucial para a interpretação correta dos dados.

-   [ ] **Mapear Colunas Codificadas:**
    -   Identificar colunas que utilizam códigos (ex: 'S'/'N', status numéricos, tipos).
    -   Para cada uma, preencher o campo `value_mapping_notes` na interface "Editar Metadados" documentando o significado de cada código (ex: "1=Pendente; 2=Enviado; 9=Cancelado").
-   [ ] **Documentar Regras de Negócio:**
    -   Adicionar notas no campo `value_mapping_notes` ou `business_description` sobre regras de cálculo, condições de preenchimento ou outras lógicas de negócio associadas à coluna.

## 3. Clareza nas Relações (FKs)

Garantir que as conexões entre tabelas estejam bem documentadas e compreensíveis.

-   [ ] **Validar Relações:**
    -   Verificar se a análise de chaves (gerada por `scripts/analysis/analyze_and_save_keys.py`) capturou todas as relações relevantes, incluindo relações lógicas não impostas por constraints no banco de dados.
    -   Se necessário, adicionar informações sobre relações ausentes manualmente nos metadados.
-   [ ] **Contextualizar a Relação:**
    -   Opcionalmente, adicionar contexto na descrição da coluna FK sobre qual tabela/coluna ela referencia (ex: "ID do cliente que fez o pedido (ref: CLIENTES.ID_CLIENTE)").

## 4. Enriquecimento com Mais Informações

Adicionar detalhes que auxiliem na busca e compreensão dos dados.

-   [ ] **Adicionar Sinônimos/Termos Comuns:**
    -   Incluir termos alternativos nas descrições ou notas para ajudar na busca em linguagem natural (ex: para `VL_TOTAL_NOTA`, incluir "valor total", "total da nota fiscal").
-   [ ] **Especificar Unidades de Medida:**
    -   Para colunas numéricas, indicar a unidade no campo de descrição ou notas (ex: "Peso (Kg)", "Valor (R$)", "Duração (minutos)").
-   [ ] **Incluir Exemplos Relevantes:**
    -   Se as amostras de dados não forem suficientes, adicionar exemplos específicos em `value_mapping_notes` para ilustrar conceitos ou formatos complexos.
-   [ ] **(Opcional/Avançado) Adicionar Data Profiling:**
    -   Considerar a criação de scripts para calcular e adicionar estatísticas básicas aos metadados (min/max, média, valores únicos, frequência) para fornecer um perfil rápido dos dados.

## 5. Como Começar (Sugestão de Ordem)

1.  **Priorizar Tabelas/Colunas Chave:** Começar enriquecendo os objetos mais importantes identificados na Análise.
2.  **Mapeamento de Valores:** Preencher os mapeamentos de códigos é um passo de alto impacto inicial.
3.  **Revisão das Descrições (Manual e AI):** Corrigir e refinar as descrições existentes.
4.  **Executar Script de Amostras:** Se ainda não feito, rodar `scripts/data_preparation/extract_sample_data.py` para incluir dados reais no contexto.
5.  **Iterar:** Continuar o processo de enriquecimento incrementalmente.

## Conclusão

Um dicionário de dados bem cuidado e rico em contexto é a base para uma interação eficiente e inteligente com os dados, maximizando o valor extraído deles. 