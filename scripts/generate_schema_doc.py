import json
import os
import logging

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

INPUT_COMBINED_FILE = 'data/combined_schema_details.json'
OUTPUT_DOC_FILE = 'docs/schema_documentation.md'

def load_json_safe(filename):
    """Carrega um arquivo JSON com tratamento de erros."""
    if not os.path.exists(filename):
        logger.error(f"Arquivo não encontrado: {filename}")
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
    except Exception as e:
        logger.error(f"Erro inesperado ao carregar {filename}: {e}", exc_info=True)
        return None

def generate_markdown_doc(schema_data, filename):
    """Gera um arquivo Markdown com a documentação do schema."""
    if not schema_data:
        logger.error("Nenhum dado de schema para gerar documentação.")
        return

    logger.info(f"Gerando documentação Markdown em {filename}...")
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, 'w', encoding='utf-8') as md_file:
            md_file.write("# Documentação do Schema do Banco de Dados\n\n")
            md_file.write("Este documento descreve as tabelas e views do banco de dados.\n\n")

            # Ordenar tabelas e views para consistência
            sorted_objects = sorted(schema_data.items(), key=lambda item: item[0])

            for object_name, data in sorted_objects:
                object_type = data.get("object_type", "Objeto")
                business_desc = data.get("business_description") or "*Sem descrição de negócio*"
                ref_count = data.get("referenced_by_fk_count", 0)

                md_file.write(f"## {object_name} ({object_type})\n\n")
                md_file.write(f"{business_desc}\n\n")
                if ref_count > 0:
                     md_file.write(f"*Referenciada por {ref_count} chave(s) estrangeira(s).*\n\n")

                # Colunas
                md_file.write("### Colunas\n\n")
                md_file.write("| Nome da Coluna | Tipo Técnico | Anulável | Descrição de Negócio | Notas de Mapeamento | Ref. FKs |\n")
                md_file.write("|---|---|---|---|---|---|\n")
                for col in sorted(data.get("columns", []), key=lambda c: c['name']): # Ordenar colunas por nome
                    col_name = col['name']
                    col_type = col.get('type', 'N/A')
                    col_nullable = "Sim" if col.get('nullable', False) else "Não"
                    col_desc = col.get("business_description") or "*N/A*"
                    col_notes = col.get("value_mapping_notes") or "*N/A*"
                    col_ref_count = col.get("referenced_by_fk_count", 0)
                    ref_count_str = str(col_ref_count) if col_ref_count > 0 else "-"
                    # Escapar pipe (|) nas descrições/notas para não quebrar a tabela markdown
                    col_desc_safe = col_desc.replace("|", "\\|")
                    col_notes_safe = col_notes.replace("|", "\\|")
                    md_file.write(f"| `{col_name}` | {col_type} | {col_nullable} | {col_desc_safe} | {col_notes_safe} | {ref_count_str} |\n")
                md_file.write("\n")

                # Constraints (se for tabela)
                if object_type == "TABLE":
                    constraints = data.get("constraints", {})
                    pk = constraints.get("primary_key")
                    fks = constraints.get("foreign_keys", [])
                    unique = constraints.get("unique", [])

                    if pk or fks or unique:
                         md_file.write("### Constraints\n\n")
                    
                    if pk:
                        pk_constraint = pk[0] # Geralmente só uma PK
                        pk_name = pk_constraint.get('name')
                        pk_cols = ", ".join([f"`{c}`" for c in pk_constraint.get('columns', [])])
                        md_file.write(f"- **Chave Primária (PK):** `{pk_name}` ({pk_cols})\n")
                        
                    if fks:
                         md_file.write(f"- **Chaves Estrangeiras (FKs):**\n")
                         for fk in sorted(fks, key=lambda f: f.get('name', '')): # Ordenar FKs por nome
                             fk_name = fk.get('name')
                             fk_cols = ", ".join([f"`{c}`" for c in fk.get('columns', [])])
                             ref_table = fk.get('references_table')
                             ref_cols = ", ".join([f"`{c}`" for c in fk.get('references_columns', [])])
                             md_file.write(f"  - `{fk_name}`: ({fk_cols}) -> `{ref_table}` ({ref_cols})\n")
                             
                    if unique:
                        md_file.write(f"- **Constraints Únicas:**\n")
                        for uq in sorted(unique, key=lambda u: u.get('name', '')): # Ordenar UQs por nome
                            uq_name = uq.get('name')
                            uq_cols = ", ".join([f"`{c}`" for c in uq.get('columns', [])])
                            md_file.write(f"  - `{uq_name}`: ({uq_cols})\n")
                    md_file.write("\n")
                md_file.write("---\n\n") # Separador entre objetos

        logger.info("Documentação Markdown gerada com sucesso.")
    except IOError as e:
        logger.error(f"Erro ao escrever o arquivo Markdown: {e}")
    except Exception as e:
         logger.exception("Erro inesperado ao gerar a documentação Markdown:")

# --- Execução Principal ---
if __name__ == "__main__":
    logger.info(f"Carregando schema combinado de: {INPUT_COMBINED_FILE}")
    combined_schema = load_json_safe(INPUT_COMBINED_FILE)

    if combined_schema:
        generate_markdown_doc(combined_schema, OUTPUT_DOC_FILE)
    else:
        logger.error("Falha ao carregar o schema combinado. Geração de documentação abortada.")

    print(f"\nProcesso de geração de documentação concluído. Resultado em {OUTPUT_DOC_FILE}") 