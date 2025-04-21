import json
import os
import sys
import logging
import argparse
from pathlib import Path

# --- Adiciona o diretório raiz ao sys.path --- #
# Assume que este script está em project_root/scripts/analysis/
script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parents[1] # Sobe dois níveis (analysis -> scripts -> project_root)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- FIM Adição ao sys.path --- #

# --- Documentação do Módulo ---
"""Gera documentação em formato Markdown a partir de um arquivo JSON
contendo o schema consolidado do banco de dados.

**Estado Atual:**
- O script lê de um arquivo JSON que combina informações técnicas, manuais e de IA.
- O formato exato deste arquivo JSON de entrada ainda é considerado
  **provisório** e pode mudar.
- Consequentemente, este script **precisará de ajustes futuros** para se alinhar
  com o padrão final do arquivo de metadados consolidado, uma vez definido.
"""
# --- Fim Documentação do Módulo ---

from src.core.config import (
    MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE, # Default Input (Provisório)
    SCHEMA_MARKDOWN_DOC_FILE           # Default Output
)
from src.core.logging_config import setup_logging # Corrigido para importar de logging_config
from src.utils.json_helpers import load_json # Usar helper centralizado

# Configura o logging
setup_logging()
logger = logging.getLogger(__name__)


def get_prioritized_description(col_data, obj_data=None):
    """Retorna a melhor descrição disponível, com prioridade.
       Prioridade: Manual (Coluna) > AI (Coluna) > Manual (Objeto) > Técnica (Coluna) > N/A
    """
    if col_data:
        if col_data.get('business_description'):
            return col_data['business_description']
        if col_data.get('ai_generated_description'):
            return f"{col_data['ai_generated_description']} (AI)"
    # Fallback para descrição manual do objeto se disponível
    if obj_data and obj_data.get('object_business_description'):
         return f"{obj_data['object_business_description']} (Objeto)"
    # Fallback para descrição técnica da coluna se disponível
    if col_data and col_data.get('description'):
        return f"{col_data['description']} (Técnica)"
    return "*N/A*"

def format_key_info(col_data):
    """Formata informação de chave primária/estrangeira para a documentação."""
    key_parts = []
    if col_data.get('is_pk'):
        key_parts.append("PK")
    if col_data.get('is_fk'):
        refs = col_data.get('fk_references')
        ref_str = "FK"
        if isinstance(refs, dict) and refs.get('references_table') and refs.get('references_column'):
            ref_str += f" -> `{refs['references_table']}`.`{refs['references_column']}`"
        elif isinstance(refs, list) and refs: # Lida com caso de múltiplas refs (pega a primeira)
            first_ref = refs[0]
            if isinstance(first_ref, dict) and first_ref.get('references_table') and first_ref.get('references_column'):
                ref_str += f" -> `{first_ref['references_table']}`.`{first_ref['references_column']}`"
        key_parts.append(ref_str)

    return ", ".join(key_parts) if key_parts else "-"

def format_sample_values(sample_values, limit=5):
     """Formata os valores de amostra para exibição concisa."""
     if not sample_values or not isinstance(sample_values, list):
         return "-"
     # Filtra valores None ou vazios e converte para string
     valid_samples = [str(s) for s in sample_values if s is not None and str(s).strip() != ""]
     if not valid_samples:
         return "-"
     # Limita o número e junta
     samples_str = ", ".join(valid_samples[:limit])
     if len(valid_samples) > limit:
         samples_str += ", ..."
     return f"`{samples_str}`"

def generate_markdown_doc(schema_data, output_filename):
    """Gera um arquivo Markdown com a documentação do schema consolidado.

    Lê a estrutura de dados do `schema_data` (assumindo o formato atual
    do arquivo consolidado provisório) e formata em Markdown.

    **Nota:** A lógica interna desta função (leitura de campos específicos,
    formatação de chaves, prioridade de descrição) provavelmente precisará
    ser ajustada quando o formato final do schema JSON for definido.

    Args:
        schema_data (dict): Dicionário carregado do JSON de schema consolidado.
        output_filename (str ou Path): Caminho para o arquivo Markdown de saída.
    """
    if not schema_data or not isinstance(schema_data, dict):
        logger.error("Dados de schema inválidos ou vazios fornecidos.")
        return

    output_path = Path(output_filename)
    logger.info(f"Gerando documentação Markdown em {output_path}...")

    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open('w', encoding='utf-8') as md_file:
            md_file.write("# Documentação do Schema do Banco de Dados\n\n")
            md_file.write("Este documento descreve as tabelas e views do banco de dados, "
                          "combinando informações técnicas, manuais e geradas por IA.\n\n")

            # Ordenar tabelas e views para consistência
            # Ignora a chave '_analysis' se existir
            sorted_objects = sorted(
                [(k, v) for k, v in schema_data.items() if k != '_analysis'],
                key=lambda item: item[0]
            )

            for object_name, data in sorted_objects:
                if not isinstance(data, dict):
                    logger.warning(f"Ignorando entrada inválida para objeto: {object_name}")
                    continue

                object_type = data.get("object_type", "Objeto Desconhecido")
                # Prioridade para descrição do objeto: Manual > Técnica > N/A
                obj_desc = data.get("object_business_description") or data.get("description") or "*Sem descrição disponível*"

                md_file.write(f"## {object_name} ({object_type})\n\n")
                md_file.write(f"{obj_desc}\n\n")
                if data.get("object_value_mapping_notes"):
                     md_file.write(f"**Notas (Objeto):** {data['object_value_mapping_notes']}\n\n")

                # Colunas
                if "columns" in data and isinstance(data["columns"], list):
                    md_file.write("### Colunas\n\n")
                    md_file.write("| Nome da Coluna | Tipo | Nulo | Chave | Descrição (Priorizada) | Notas Mapeamento | Amostra Valores |\n")
                    md_file.write("|---|---|---|---|---|---|---|\n")
                    for col in sorted(data["columns"], key=lambda c: c.get('name', '')):
                        if not isinstance(col, dict):
                             logger.warning(f"Ignorando entrada de coluna inválida em {object_name}")
                             continue

                        col_name = col.get('name', '*Erro: Sem Nome*')
                        col_type = col.get('type', 'N/A')
                        col_nullable = "Sim" if col.get('nullable', False) else "Não"
                        col_key_info = format_key_info(col)
                        col_desc = get_prioritized_description(col, data) # Passa obj_data para fallback
                        col_notes = col.get("value_mapping_notes") or "-"
                        col_samples = format_sample_values(col.get("sample_values"))

                        # Escapar pipe (|) nas descrições/notas
                        col_desc_safe = col_desc.replace("|", "\\|")
                        col_notes_safe = col_notes.replace("|", "\\|")

                        md_file.write(f"| `{col_name}` | {col_type} | {col_nullable} | {col_key_info} | {col_desc_safe} | {col_notes_safe} | {col_samples} |\n")
                    md_file.write("\n")
                else:
                    md_file.write("*Nenhuma coluna encontrada ou formato inválido.*\n\n")

                md_file.write("---\n\n") # Separador entre objetos

        logger.info("Documentação Markdown gerada com sucesso.")
    except IOError as e:
        logger.error(f"Erro de I/O ao escrever o arquivo Markdown '{output_path}': {e}")
    except Exception as e:
         logger.exception(f"Erro inesperado ao gerar a documentação Markdown em '{output_path}':")

# --- Função Main Refatorada ---
def main():
    parser = argparse.ArgumentParser(
        description="Gera documentação Markdown a partir de um arquivo JSON de schema consolidado (Formato Provisório)."
    )

    parser.add_argument(
        "-i", "--input",
        default=MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE,
        help=f"Caminho para o arquivo JSON de schema consolidado (com dados técnicos, AI, manual, samples). Padrão: {MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE}"
    )
    parser.add_argument(
        "-o", "--output",
        default=SCHEMA_MARKDOWN_DOC_FILE,
        help=f"Caminho para o arquivo Markdown de saída da documentação. Padrão: {SCHEMA_MARKDOWN_DOC_FILE}"
    )

    args = parser.parse_args()

    logger.info(f"Carregando dados do schema de: {args.input}")
    schema_data = load_json(args.input)

    if schema_data:
        generate_markdown_doc(schema_data, args.output)
    else:
        logger.error(f"Não foi possível carregar dados do schema de {args.input}. Documentação não gerada.")

if __name__ == "__main__":
    main() 