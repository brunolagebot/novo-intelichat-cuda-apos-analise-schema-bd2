import os
import sys
import json
import logging
import argparse
import time
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from tqdm import tqdm

# --- Adiciona o diretório raiz ao sys.path --- #
# Assume que este script está em project_root/scripts/ai_tasks/object_description/
script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parents[2] # Sobe três níveis (object_description -> ai_tasks -> scripts -> project_root)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- FIM Adição ao sys.path --- #

# Importar configurações e helpers necessários
from src.core.logging_config import setup_logging
from src.utils.json_helpers import load_json, save_json
from src.core.config import (
    MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE, # Input Principal (consolidado)
    OVERVIEW_COUNTS_FILE,                  # Input Contagens
    KEY_ANALYSIS_RESULTS_FILE,             # Input Análise de Chaves
    AI_OBJECT_DESCRIPTIONS_FILE            # Output Descrições AI de Objetos
)
# Reutilizar a chamada à API e tratamento de erro do script anterior
# (Idealmente, isso estaria em src/core/ai_integration.py, mas por ora importamos direto)
try:
    # Tenta importar do local original (se ainda existir para referência)
    from scripts.ai_tasks.generate_ai_description_openia import (
        generate_description_with_openai,
        _openai_api_key # Acessa a chave carregada
    )
except ImportError:
    # Fallback se o script original foi movido/removido (COPIAR A FUNÇÃO RELEVANTE)
    # É ALTAMENTE RECOMENDADO mover esta lógica para um módulo central depois
    from openai import OpenAI, RateLimitError, APIError, APITimeoutError, APIConnectionError
    import toml

    _openai_api_key = None
    try:
        # Usar Pathlib para construir o caminho de forma segura
        secrets_path = project_root / ".streamlit" / "secrets.toml"
        with secrets_path.open("r", encoding="utf-8") as f:
            secrets = toml.load(f)
            _openai_api_key = secrets.get("openai", {}).get("api_key", "")
    except Exception as e:
        _openai_api_key = os.getenv("OPENAI_API_KEY") or ""
        logging.warning(f"Falha ao carregar .streamlit/secrets.toml, tentando variável de ambiente. Erro: {e}")

    def generate_description_with_openai(prompt):
        """Gera descrição usando a API OpenAI V1.x (Cópia para fallback)."""
        logger = logging.getLogger(__name__) # Obter logger localmente
        if not _openai_api_key:
            logger.error("Chave da API da OpenAI não configurada. Não é possível gerar descrições.")
            return None
        try:
            client = OpenAI(api_key=_openai_api_key)
            response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Você é um assistente especialista em gerar descrições de negócio concisas e úteis para tabelas e views de banco de dados em português brasileiro."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                max_tokens=300 # << AUMENTADO: Permite descrições mais longas (ex: 2-4 frases)
            )
            return response.choices[0].message.content.strip()
        except RateLimitError as e:
            logger.error(f"Erro de Rate Limit da OpenAI: {e}. Aguardando e pulando item...")
            time.sleep(20)
            return None
        except APIError as e:
            logger.error(f"Erro da API OpenAI: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro inesperado ao chamar a API da OpenAI: {e}", exc_info=True)
            return None

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Funções Auxiliares Específicas ---

def format_sample_values(sample_values, limit=5):
     """Formata os valores de amostra para exibição concisa."""
     if not sample_values or not isinstance(sample_values, list):
         return None # Retorna None se não houver amostras válidas
     # Filtra valores None ou vazios e converte para string
     valid_samples = [str(s) for s in sample_values if s is not None and str(s).strip() != ""]
     if not valid_samples:
         return None
     # Limita o número e junta
     samples_str = ", ".join(valid_samples[:limit])
     if len(valid_samples) > limit:
         samples_str += ", ..."
     return f"`{samples_str}`"

def get_pk_summary(columns_data):
    """Retorna os nomes das colunas da chave primária."""
    if not columns_data or not isinstance(columns_data, list):
        return "N/A"
    pk_cols = [col['name'] for col in columns_data if isinstance(col, dict) and col.get('is_pk')]
    return ", ".join(pk_cols) if pk_cols else "Nenhuma"

def select_and_format_key_columns(columns_data, max_cols_with_samples=10):
    """Seleciona colunas chave com base em heurísticas e formata com amostras."""
    if not columns_data or not isinstance(columns_data, list):
        return "Nenhuma coluna encontrada."

    business_keywords = ["NOME", "DESC", "VALOR", "VLR", "PRECO", "QTD", "QUANTIDADE", "PESO", "DATA", "CODIGO", "STATUS", "TIPO"]
    id_suffixes = ["_ID", "ID_", "_COD", "COD_"]

    scored_cols = []
    for col in columns_data:
        if not isinstance(col, dict) or 'name' not in col:
            continue

        score = 0
        col_name_upper = col['name'].upper()

        # Atribui scores
        if col.get('business_description'):
            score += 100 # Prioridade máxima
        elif col.get('ai_generated_description'):
            score += 50

        if col.get('is_pk'):
            score += 20

        # Verifica palavras chave de negócio (exceto se parecer ID/FK)
        is_likely_id = any(col_name_upper.endswith(s) or col_name_upper.startswith(s) for s in id_suffixes)
        if not (is_likely_id and not col.get('is_pk')):
            if any(keyword in col_name_upper for keyword in business_keywords):
                score += 10

        # Inclui descrição técnica se disponível (score baixo)
        if col.get('description') and not col.get('business_description') and not col.get('ai_generated_description'):
             score += 5

        # Guarda coluna e score (apenas se score > 0 para relevância mínima)
        if score > 0:
             scored_cols.append((score, col))

    # Ordena por score (maior primeiro)
    scored_cols.sort(key=lambda x: x[0], reverse=True)

    # Formata as N melhores colunas
    summary_lines = []
    for i, (score, col) in enumerate(scored_cols):
        if i >= max_cols_with_samples:
            summary_lines.append("- ... (outras colunas omitidas)")
            break

        col_name = col['name']
        # Prioridade Descrição: Manual Coluna > AI Coluna > Técnica Coluna > N/A
        desc = col.get('business_description') or col.get('ai_generated_description') or col.get('description') or ""
        desc_str = f" ({desc})" if desc else ""
        samples_str = format_sample_values(col.get('sample_values'))
        samples_display = f" {samples_str}" if samples_str else ""

        summary_lines.append(f"- {col_name}{desc_str}{samples_display}")

    return "\n".join(summary_lines) if summary_lines else "Nenhuma coluna relevante encontrada para resumo."

def preprocess_key_analysis(key_analysis_data):
    """Pré-processa os dados de análise de chaves para fácil consulta de FKs."""
    fk_counts = {"incoming": defaultdict(int), "outgoing": defaultdict(int)}
    if not key_analysis_data or not isinstance(key_analysis_data, dict):
        return fk_counts, set(), set() # Retorna estruturas vazias

    fk_definitions = key_analysis_data.get("fk_definitions", {})
    for fk_sig, fk_details in fk_definitions.items():
        if isinstance(fk_details, dict):
            fk_table = fk_details.get("fk_table")
            pk_table = fk_details.get("pk_table")
            if fk_table:
                fk_counts["outgoing"][fk_table] += 1
            if pk_table:
                fk_counts["incoming"][pk_table] += 1

    composite_pk_tables = set(key_analysis_data.get("composite_pk_tables", []))
    junction_tables = set(key_analysis_data.get("junction_tables", []))

    return fk_counts, composite_pk_tables, junction_tables


def build_object_context(obj_name, obj_data, row_counts, fk_counts, is_composite_pk, is_junction_table):
    """Constrói a string de contexto para o prompt do objeto."""
    obj_type = obj_data.get('object_type', 'Desconhecido')
    tech_desc = obj_data.get('description', 'Nenhuma') # Descrição técnica do DB
    row_count = row_counts.get(f'{obj_type}:{obj_name}', 'N/A')
    if isinstance(row_count, int):
         row_count_str = f'{row_count:,}'.replace(",",".") # Formata com separador de milhar pt-BR
    elif row_count == "ERROR":
         row_count_str = "Erro ao calcular"
    else:
         row_count_str = "N/A"

    column_summary = select_and_format_key_columns(obj_data.get('columns', []))
    pk_summary = get_pk_summary(obj_data.get('columns', []))
    outgoing_fks = fk_counts["outgoing"].get(obj_name, 0)
    incoming_fks = fk_counts["incoming"].get(obj_name, 0)

    context = f"""
Nome do Objeto: {obj_name}
Tipo: {obj_type}
Descrição Técnica do DB: {tech_desc}
Contagem Aproximada de Linhas: {row_count_str}
Colunas Relevantes (com Amostras):
{column_summary}
Chave Primária (PK): {pk_summary}
Relações:
  - Referenciado por (FKs de outras tabelas): {incoming_fks}
  - Referencia (FKs para outras tabelas): {outgoing_fks}
  - PK Composta? {'Sim' if is_composite_pk else 'Não'}
  - Tabela de Junção? {'Sim' if is_junction_table else 'Não'}
"""
    return context.strip()

def build_object_prompt(context):
     """Cria o prompt final para a API OpenAI."""
     prompt = f"""
Tarefa: Gere uma descrição de negócio clara e concisa (1-2 frases) para o objeto de banco de dados descrito no contexto abaixo. Explique seu propósito principal para um usuário de negócio ou analista de dados. Use português brasileiro (pt-BR).

--- Contexto do Objeto ---
{context}
--- Fim do Contexto ---

Descrição de Negócio Solicitada:
"""
     return prompt

def load_existing_object_descriptions(filename):
    """Carrega descrições de OBJETOS já geradas para evitar reprocessamento."""
    existing_data = load_json(filename, default_value=[])
    processed_object_names = set()
    results_list = []
    if isinstance(existing_data, list):
        results_list = existing_data
        for item in existing_data:
            try:
                # Verifica se é uma descrição de objeto (column_name é None ou ausente)
                if item.get('object_name') and item.get('column_name') is None:
                    processed_object_names.add(item['object_name'].strip().upper())
            except Exception as e:
                logger.warning(f"Erro processando item existente {item} de {filename}: {e}")
    else:
        logger.warning(f"Arquivo de saída {filename} não contém uma lista. Iniciando do zero.")
        results_list = []
    logger.info(f"{len(processed_object_names)} descrições de objetos existentes carregadas de {filename}.")
    return processed_object_names, results_list


# --- Função Principal ---
def main(args):
    script_start_time = time.perf_counter()
    logger.info("--- Iniciando Geração de Descrições de OBJETOS com OpenAI ---")
    logger.info(f"Schema Consolidado de Entrada: {args.input_schema}")
    logger.info(f"Contagens de Linhas de Entrada: {args.row_counts}")
    logger.info(f"Análise de Chaves de Entrada: {args.key_analysis}")
    logger.info(f"Arquivo de Saída (Descrições de Objeto): {args.output_file}")
    if args.force_regenerate:
        logger.warning("*** MODO FORCE_REGENERATE ATIVO: Todas as descrições de objeto serão geradas novamente. ***")

    if not _openai_api_key:
        logger.critical("Chave da API da OpenAI não encontrada. Abortando.")
        return

    # 1. Carregar Dados de Input Necessários
    load_start = time.perf_counter()
    schema = load_json(args.input_schema)
    row_counts_data = load_json(args.row_counts)
    key_analysis_data = load_json(args.key_analysis)

    if not schema or not isinstance(schema, dict):
        logger.critical(f"Schema consolidado em {args.input_schema} não pôde ser carregado ou é inválido. Abortando.")
        return
    if not row_counts_data or 'counts' not in row_counts_data:
         logger.warning(f"Arquivo de contagem {args.row_counts} não encontrado ou inválido. A verificação de 0 linhas pode falhar.")
         row_counts_map = {} # Usar dict vazio como fallback
    else:
         row_counts_map = row_counts_data['counts'] # Usar o dicionário interno

    if not key_analysis_data:
         logger.warning(f"Arquivo de análise de chaves {args.key_analysis} não encontrado. Informações de relacionamento estarão incompletas.")
         key_analysis_data = {} # Usar dict vazio como fallback

    # Pré-processar análise de chaves
    fk_counts, composite_pk_tables, junction_tables = preprocess_key_analysis(key_analysis_data)

    # Carregar descrições de OBJETOS existentes do arquivo de SAÍDA
    processed_object_names, results_list = load_existing_object_descriptions(args.output_file)
    if args.force_regenerate:
         results_list = [] # Limpa resultados se forçar regeneração
         processed_object_names = set()
         logger.info(f"Forçando regeneração. Iniciando com {len(results_list)} descrições.")
    else:
         logger.info(f"Iniciando com {len(results_list)} descrições pré-existentes lidas de {args.output_file}.")

    load_end = time.perf_counter()
    logger.info(f"Carregamento de todos os inputs levou: {load_end - load_start:.2f}s")

    # 2. Preparar Lista de Objetos para Processar
    objects_to_process = []
    skipped_zero_rows = 0
    skipped_manual_desc = 0
    skipped_already_generated = 0
    prep_start = time.perf_counter()

    logger.info("Preparando lista de objetos (tabelas/views) a processar...")
    for obj_name, obj_data in schema.items():
         if obj_name.startswith('_') or not isinstance(obj_data, dict) or 'object_type' not in obj_data:
             continue # Ignora metadados internos ou entradas malformadas

         obj_type = obj_data['object_type']
         obj_name_upper = obj_name.strip().upper()

         # --- Filtro 1: Contagem de Linhas ---
         row_count = row_counts_map.get(f'{obj_type}:{obj_name}', -1) # Assume -1 se não encontrado
         if row_count == 0 or row_count == "ERROR":
             logger.debug(f"Pulando objeto '{obj_name}' (contagem={row_count}).")
             skipped_zero_rows += 1
             continue

         # --- Filtro 2: Descrição Manual Existente no Schema de Input ---
         # (Só checa se não for forçar regeneração)
         if not args.force_regenerate and obj_data.get('object_business_description'):
              logger.debug(f"Pulando objeto '{obj_name}' (descrição manual já existe no schema de entrada).")
              skipped_manual_desc += 1
              continue

         # --- Filtro 3: Descrição AI Existente no Arquivo de Output ---
         # (Só checa se não for forçar regeneração)
         if not args.force_regenerate and obj_name_upper in processed_object_names:
              logger.debug(f"Pulando objeto '{obj_name}' (descrição AI já existe no arquivo de saída).")
              skipped_already_generated += 1
              continue

         # Se passou por todos os filtros, adiciona à lista
         objects_to_process.append((obj_name, obj_data))

    prep_end = time.perf_counter()
    logger.info(f"Preparação de {len(objects_to_process)} objetos levou: {prep_end - prep_start:.2f}s")
    logger.info(f"Objetos pulados: {skipped_zero_rows} (0 linhas/erro) + {skipped_manual_desc} (manual no input) + {skipped_already_generated} (AI no output)")

    if args.max_items is not None:
        logger.warning(f"*** Limitando processamento aos primeiros {args.max_items} objetos. ***")
        objects_to_process = objects_to_process[:args.max_items]
        logger.info(f"Número de objetos após limitação: {len(objects_to_process)}")

    # 3. Gerar Descrições
    generated_count = 0
    error_count = 0
    loop_start_time = time.perf_counter()

    pbar = tqdm(objects_to_process, desc="Gerando descrições de OBJETOS com OpenAI")
    for obj_name, obj_data in pbar:
        pbar.set_postfix_str(f"Gerando: {obj_name}")
        obj_type = obj_data.get('object_type', 'Desconhecido')

        # Construir contexto
        is_composite = obj_name in composite_pk_tables
        is_junction = obj_name in junction_tables
        context_str = build_object_context(obj_name, obj_data, row_counts_map, fk_counts, is_composite, is_junction)

        # Construir prompt
        prompt = build_object_prompt(context_str)

        # Chamar API
        generation_start = time.perf_counter()
        generated_desc = generate_description_with_openai(prompt)
        generation_end = time.perf_counter()

        if generated_desc:
            generated_count += 1
            result_item = {
                "object_type": obj_type,
                "object_name": obj_name,
                "column_name": None, # Explicitamente None para objetos
                "generated_description": generated_desc,
                "model_used": "gpt-3.5-turbo",
                "generation_timestamp": datetime.utcnow().isoformat() + "Z"
            }
            results_list.append(result_item)
            # Adiciona ao set de processados para evitar duplicatas na mesma run (se houver restart/erro)
            processed_object_names.add(obj_name.strip().upper())
            pbar.set_postfix_str(f"OK: {obj_name} ({generation_end - generation_start:.2f}s)")
        else:
            error_count += 1
            pbar.set_postfix_str(f"ERRO: {obj_name}")
            # Não logar erro aqui novamente, já é logado dentro de generate_description_with_openai
            time.sleep(1) # Pequena pausa após erro

    pbar.close()
    loop_end_time = time.perf_counter()
    logger.info(f"--- Loop de Geração de Objeto Concluído em: {loop_end_time - loop_start_time:.2f}s ---")

    # 4. Salvar Resultados
    logger.info(f"--- Resumo da Execução (Descrições de Objeto) ---")
    logger.info(f"Novas descrições de OBJETO geradas: {generated_count}")
    logger.info(f"Erros durante a geração: {error_count}")
    logger.info(f"Total de descrições de OBJETO no arquivo de saída final: {len(results_list)}")

    save_start = time.perf_counter()
    logger.info(f"Salvando resultados em {args.output_file}...")
    save_json(results_list, args.output_file)
    save_end = time.perf_counter()
    logger.info(f"Salvamento concluído em: {save_end - save_start:.2f}s")

    script_end_time = time.perf_counter()
    logger.info(f"--- Tempo Total de Execução do Script (Descrições de Objeto): {script_end_time - script_start_time:.2f}s ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gera descrições de negócio para TABELAS e VIEWS usando OpenAI, com contexto enriquecido.")

    # Argumentos para arquivos
    parser.add_argument(
        "--input-schema",
        default=MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE,
        help=f"Caminho para o arquivo JSON de schema consolidado (fonte principal). Padrão: {MERGED_SCHEMA_FINAL_WITH_SAMPLES_FILE}"
    )
    parser.add_argument(
        "--row-counts",
        default=OVERVIEW_COUNTS_FILE,
        help=f"Caminho para o arquivo JSON de contagem de linhas. Padrão: {OVERVIEW_COUNTS_FILE}"
    )
    parser.add_argument(
        "--key-analysis",
        default=KEY_ANALYSIS_RESULTS_FILE,
        help=f"Caminho para o arquivo JSON de análise de chaves. Padrão: {KEY_ANALYSIS_RESULTS_FILE}"
    )
    parser.add_argument(
        "-o", "--output-file",
        default=AI_OBJECT_DESCRIPTIONS_FILE,
        help=f"Caminho para o arquivo JSON de saída com as descrições de objeto geradas. Padrão: {AI_OBJECT_DESCRIPTIONS_FILE}"
    )

    # Argumentos de controle
    parser.add_argument(
        "--force_regenerate",
        action="store_true",
        help="Força a regeneração de todas as descrições de objeto, ignorando existentes no arquivo de saída e descrições manuais no input."
    )
    parser.add_argument(
        "--max_items",
        type=int, default=None,
        help="Número máximo de OBJETOS para processar (para teste rápido)."
    )

    args = parser.parse_args()
    main(args) 