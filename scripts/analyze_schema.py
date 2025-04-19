import json
import os
import sys
import logging
from collections import defaultdict, Counter

# Adiciona o diretório raiz ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.core.config import OUTPUT_COMBINED_FILE # Importar
from src.core.log_utils import setup_logging

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# Constantes (removido, usar importação)
# INPUT_JSON_FILE = 'data/processed/combined_schema_details.json'

INPUT_JSON_FILE = 'data/combined_schema_details.json'
TOP_N = 10 # Número de itens a serem exibidos nas listas "top"

def load_schema_data(filename=INPUT_JSON_FILE):
    """Carrega os dados do schema do arquivo JSON."""
    if not os.path.exists(filename):
        logger.error(f"Arquivo de schema não encontrado: {filename}")
        return None
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON do arquivo {filename}: {e}")
        return None
    except IOError as e:
        logger.error(f"Erro ao ler o arquivo {filename}: {e}")
        return None

def analyze_most_referenced(schema_data):
    """Analisa e exibe as tabelas e colunas mais referenciadas por FKs."""
    if not schema_data:
        return

    table_refs = []
    column_refs = []

    for table_name, data in schema_data.items():
        if data.get("object_type") == "TABLE":
            # Contagem de referência da tabela
            ref_count = data.get("referenced_by_fk_count", 0)
            if ref_count > 0:
                table_refs.append((table_name, ref_count))

            # Contagem de referência das colunas
            for col in data.get("columns", []):
                col_ref_count = col.get("referenced_by_fk_count", 0)
                if col_ref_count > 0:
                    column_refs.append((f"{table_name}.{col['name']}", col_ref_count))

    # Ordenar por contagem (decrescente)
    table_refs.sort(key=lambda item: item[1], reverse=True)
    column_refs.sort(key=lambda item: item[1], reverse=True)

    print(f"\n--- {TOP_N} Tabelas Mais Referenciadas por FKs ---")
    if not table_refs:
        print("Nenhuma tabela é referenciada por FKs.")
    else:
        for table, count in table_refs[:TOP_N]:
            print(f"- {table}: {count} referências")

    print(f"\n--- {TOP_N} Colunas Mais Referenciadas por FKs ---")
    if not column_refs:
        print("Nenhuma coluna é referenciada por FKs.")
    else:
        for col, count in column_refs[:TOP_N]:
            print(f"- {col}: {count} referências")

# --- Funções de Análise Adicionais (a serem implementadas) ---

def analyze_junction_tables(schema_data):
    """Identifica potenciais tabelas de junção (M-N)."""
    if not schema_data:
        return
    
    junction_tables = []
    for table_name, data in schema_data.items():
        if data.get("object_type") != "TABLE":
            continue

        constraints = data.get("constraints", {})
        pk = constraints.get("primary_key", [])
        fks = constraints.get("foreign_keys", [])

        # Critério 1: Deve ter uma PK definida
        if not pk:
            continue
            
        # Critério 2: Deve ter pelo menos duas FKs saindo dela
        if len(fks) < 2:
            continue

        # Critério 3: Todas as colunas da PK devem também ser parte de alguma FK
        #             E todas as colunas de FKs devem também ser parte da PK
        pk_columns = set(col for constraint in pk for col in constraint.get("columns", []))
        fk_columns = set(col for constraint in fks for col in constraint.get("columns", []))

        # Verifica se os conjuntos de colunas são idênticos e não vazios
        if pk_columns and pk_columns == fk_columns:
            # Critério 4 (Opcional, mas bom): A tabela não deve ter muitas outras colunas além das da PK/FK
            all_columns = set(c["name"] for c in data.get("columns", []))
            non_key_columns = all_columns - pk_columns
            # Consideramos tabela de junção se não houver colunas extras ou poucas
            # Ajuste o threshold (ex: 2) conforme necessário
            if len(non_key_columns) <= 2: 
                junction_tables.append(table_name)

    print("\n--- Potenciais Tabelas de Junção (Muitos-para-Muitos) ---")
    if not junction_tables:
        print("Nenhuma tabela de junção aparente encontrada pelos critérios.")
    else:
        for table in sorted(junction_tables):
            print(f"- {table}")

def analyze_tables_without_pk(schema_data):
    """Identifica tabelas sem chave primária definida."""
    if not schema_data:
        return

    tables_no_pk = []
    for table_name, data in schema_data.items():
        if data.get("object_type") == "TABLE":
            constraints = data.get("constraints", {})
            pk = constraints.get("primary_key", [])
            if not pk:
                tables_no_pk.append(table_name)

    print("\n--- Tabelas Sem Chave Primária (PK) Definida ---")
    if not tables_no_pk:
        print("Todas as tabelas parecem ter uma PK definida.")
    else:
        for table in sorted(tables_no_pk):
            print(f"- {table}")

def analyze_isolated_tables(schema_data):
    """Identifica tabelas que não referenciam nem são referenciadas por FKs."""
    if not schema_data:
        return

    isolated_tables = []
    for table_name, data in schema_data.items():
        if data.get("object_type") == "TABLE":
            is_referenced = data.get("referenced_by_fk_count", 0) > 0
            has_outgoing_fks = bool(data.get("constraints", {}).get("foreign_keys", []))

            if not is_referenced and not has_outgoing_fks:
                isolated_tables.append(table_name)

    print("\n--- Tabelas Isoladas (Sem FKs de entrada ou saída) ---")
    if not isolated_tables:
        print("Nenhuma tabela isolada encontrada.")
    else:
        for table in sorted(isolated_tables):
            print(f"- {table}")

def analyze_missing_descriptions(schema_data):
    """Identifica objetos (tabelas/views) e colunas sem descrição de negócio."""
    if not schema_data:
        return

    objects_no_desc = []
    columns_no_desc = []

    for object_name, data in schema_data.items():
        # Verifica descrição do objeto
        if not data.get("business_description"): # Verifica se é None ou string vazia
            objects_no_desc.append(f"{data.get('object_type', 'OBJETO')}: {object_name}")
        
        # Verifica descrição das colunas
        for col in data.get("columns", []):
            if not col.get("business_description"): # Verifica se é None ou string vazia
                columns_no_desc.append(f"{object_name}.{col['name']}")

    print("\n--- Objetos (Tabelas/Views) Sem Descrição de Negócio ---")
    if not objects_no_desc:
        print("Todos os objetos parecem ter uma descrição de negócio.")
    else:
        print(f"Total: {len(objects_no_desc)}")
        # Opcional: listar alguns ou todos
        # for item in sorted(objects_no_desc):
        #     print(f"- {item}") 
        print("(Lista completa omitida para brevidade se for muito longa)")

    print("\n--- Colunas Sem Descrição de Negócio ---")
    if not columns_no_desc:
        print("Todas as colunas parecem ter uma descrição de negócio.")
    else:
        print(f"Total: {len(columns_no_desc)}")
        # Opcional: listar alguns ou todos
        # for item in sorted(columns_no_desc):
        #     print(f"- {item}") 
        print("(Lista completa omitida para brevidade se for muito longa)")

def main():
    logger.info(f"Carregando dados do schema combinado de: {OUTPUT_COMBINED_FILE}")
    try:
        with open(OUTPUT_COMBINED_FILE, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
    except FileNotFoundError:
        logger.error(f"Erro: Arquivo {OUTPUT_COMBINED_FILE} não encontrado.")
        return
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao decodificar JSON em {OUTPUT_COMBINED_FILE}: {e}")
        return

    logger.info("Iniciando análise do schema...")
    analysis_results = analyze_schema(schema_data)

    logger.info("\n--- Resultados da Análise do Schema ---")
    print("\n--- Resultados da Análise do Schema ---")

    print(f"\nTipos de Objetos Encontrados: {list(analysis_results['object_types'])}")
    logger.info(f"Tipos de Objetos Encontrados: {list(analysis_results['object_types'])}")

    for obj_type, count in analysis_results['object_counts'].items():
        print(f"- Contagem de {obj_type}: {count}")
        logger.info(f"- Contagem de {obj_type}: {count}")

    print(f"\nContagem Total de Colunas: {analysis_results['total_columns']}")
    logger.info(f"Contagem Total de Colunas: {analysis_results['total_columns']}")

    print("\nDistribuição de Tipos de Dados das Colunas:")
    logger.info("Distribuição de Tipos de Dados das Colunas:")
    for data_type, count in analysis_results['column_type_distribution'].most_common():
        print(f"- {data_type}: {count}")
        logger.info(f"- {data_type}: {count}")

    print(f"\nColunas com Descrição Manual: {analysis_results['columns_with_manual_description']}")
    logger.info(f"Colunas com Descrição Manual: {analysis_results['columns_with_manual_description']}")
    print(f"Colunas SEM Descrição Manual: {analysis_results['columns_without_manual_description']}")
    logger.info(f"Colunas SEM Descrição Manual: {analysis_results['columns_without_manual_description']}")

    print(f"\nColunas com Descrição AI: {analysis_results['columns_with_ai_description']}")
    logger.info(f"Colunas com Descrição AI: {analysis_results['columns_with_ai_description']}")
    print(f"Colunas SEM Descrição AI: {analysis_results['columns_without_ai_description']}")
    logger.info(f"Colunas SEM Descrição AI: {analysis_results['columns_without_ai_description']}")

    print(f"\nColunas Primárias (PK): {analysis_results['primary_key_columns']}")
    logger.info(f"Colunas Primárias (PK): {analysis_results['primary_key_columns']}")
    print(f"Colunas Estrangeiras (FK): {analysis_results['foreign_key_columns']}")
    logger.info(f"Colunas Estrangeiras (FK): {analysis_results['foreign_key_columns']}")

    print("\nAnálise concluída.")
    logger.info("Análise concluída.")

if __name__ == "__main__":
    main() 