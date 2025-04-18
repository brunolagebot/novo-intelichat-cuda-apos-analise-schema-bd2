import os
import json
import logging
import argparse
from datetime import datetime
import openai
from tqdm import tqdm
import time
import toml
import sys
# NOVO: Importar cliente e exceções da V1
from openai import OpenAI, RateLimitError, APIError

# --- NOVO: Adiciona a raiz do projeto ao sys.path ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM NOVO ---

# Importar funções utilitárias e de geração
from src.core.utils import load_json_safe
# from src.core.ai_integration import generate_description_with_openai
from src.core.logging_config import setup_logging

# --- Carregar chave da OpenAI do .streamlit/secrets.toml ---
try:
    with open(".streamlit/secrets.toml", "r", encoding="utf-8") as f:
        secrets = toml.load(f)
        # NÃO definir openai.api_key globalmente na V1
        # A chave será passada ao instanciar o cliente
        _openai_api_key = secrets.get("openai", {}).get("api_key", "")
except Exception as e:
    _openai_api_key = os.getenv("OPENAI_API_KEY") or ""
    logging.warning("Falha ao carregar .streamlit/secrets.toml, tentando variável de ambiente. Erro: %s", e)

# --- Constantes e Configuração --- #
TECHNICAL_SCHEMA_FILE = "data/enhanced_technical_schema.json"
COMBINED_SCHEMA_FILE = "data/combined_schema_details.json" # Para descrições manuais
ROW_COUNTS_FILE = "data/overview_counts.json" # NOVO: Arquivo de contagens
OUTPUT_FILE_OPENAI_35TURBO = "data/ai_generated_descriptions_openai_35turbo.json" # Saída específica para 3.5 Turbo

# Configurar logging (usando a função centralizada)
setup_logging()
logger = logging.getLogger(__name__)

# --- Funções Auxiliares --- #

# NOVO: Identificador único para itens
def get_item_identifier(obj_type, obj_name, col_name=None):
    return f"{obj_type}:{obj_name}:{col_name if col_name else '__TABLE__'}"

# NOVO: Carregar descrições AI existentes
def load_existing_ai_descriptions(filename):
    """Carrega descrições AI já geradas para evitar reprocessamento."""
    existing_data = load_json_safe(filename)
    processed_ids = set()
    if existing_data and isinstance(existing_data, list):
        for item in existing_data:
            try:
                # Verifica se as chaves essenciais existem antes de gerar o identificador
                if all(k in item for k in ('object_type', 'object_name')):
                    identifier = get_item_identifier(item['object_type'], item['object_name'], item.get('column_name'))
                    processed_ids.add(identifier)
                else:
                    logger.warning(f"Item inválido (sem chaves obrigatórias) encontrado em {filename}: {item}")
            except KeyError:
                # Embora a verificação acima deva prevenir isso, mantemos por segurança
                logger.warning(f"Item inválido (KeyError) encontrado em {filename}: {item}")
        logger.info(f"{len(processed_ids)} descrições AI existentes carregadas de {filename}.")
    return processed_ids, (existing_data if isinstance(existing_data, list) else [])

# REFINADO: Prompt mais focado
def build_prompt(obj_type, obj_name, col_data=None):
    """Constrói o prompt para gerar a descrição de uma coluna ou tabela/view."""
    if col_data:
        # Foco nos detalhes essenciais para a descrição de negócio
        prompt = f"""
        Tabela/View: {obj_name}
        Coluna: {col_data.get('name', 'N/A')}
        Tipo Técnico: {col_data.get('type', 'N/A')}
        Descrição Técnica Original: {col_data.get('description', 'Nenhuma')}
        """
        # Incluir amostras pode ser útil, mas mantendo conciso
        samples = col_data.get('sample_values', [])
        if samples and samples != ["BOOLEAN_SKIPPED"]:
            sample_limit = 3 # Menos amostras para concisão
            prompt += f"\nAmostra de Valores ({min(len(samples), sample_limit)}): {samples[:sample_limit]}"

        # Instrução clara e focada
        prompt += f"\n\nTarefa: Gere uma descrição de negócio clara e concisa (1-2 frases) para a coluna \"{col_data.get('name')}\" da tabela \"{obj_name}\". Explique seu propósito principal para um usuário de negócio ou analista de dados. Use português brasileiro (pt-BR)."""
    else:
        # Prompt para Tabela/View (simplificado)
        prompt = f"""
        Objeto: {obj_name}
        Tipo: {obj_type}
        \nTarefa: Gere uma descrição de negócio clara e concisa (1-2 frases) para {obj_type.lower()} \"{obj_name}\". Explique seu propósito principal para um usuário de negócio ou analista de dados. Use português brasileiro (pt-BR)."""

    return prompt

# ATUALIZADO: Para usar OpenAI V1.x
def generate_description_with_openai(prompt):
    """Gera descrição usando a API OpenAI V1.x."""
    if not _openai_api_key: # Verificar a chave carregada
        logger.error("Chave da API da OpenAI não configurada. Não é possível gerar descrições.")
        return None

    try:
        client = OpenAI(api_key=_openai_api_key)
        response = client.chat.completions.create(
            model="gpt-3.5-turbo", # ATUALIZADO: Usar o modelo 3.5 Turbo
            messages=[
                {"role": "system", "content": "Você é um assistente especialista em gerar descrições de negócio concisas e úteis para elementos de banco de dados (tabelas, views, colunas) em português brasileiro."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=150
        )
        return response.choices[0].message.content.strip()
    # Atualizar para exceções da V1
    except RateLimitError as e:
        logger.error(f"Erro de Rate Limit da OpenAI: {e}. Aguardando e pulando item...")
        time.sleep(20)
        return None
    except APIError as e:
        # Captura outros erros da API (e.g., servidor, autenticação incorreta)
        logger.error(f"Erro da API OpenAI: {e}")
        return None
    except Exception as e:
        # Captura outros erros inesperados (e.g., rede)
        logger.error(f"Erro inesperado ao chamar a API da OpenAI: {e}", exc_info=True)
        return None

def save_json_safe(data, path):
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Arquivo salvo com sucesso em {path}")
    except Exception as e:
        logger.error(f"Erro ao salvar JSON em {path}: {e}")

# --- Função Principal --- #

def main():
    script_start_time = time.perf_counter()

    parser = argparse.ArgumentParser(description="Gera descrições de negócio para colunas de banco de dados usando a API da OpenAI (GPT-3.5-Turbo), pulando itens já descritos.")
    parser.add_argument("-i", "--input", default=TECHNICAL_SCHEMA_FILE, help=f"Caminho para o arquivo JSON com o esquema técnico. Padrão: {TECHNICAL_SCHEMA_FILE}")
    parser.add_argument("-o", "--output", default=OUTPUT_FILE_OPENAI_35TURBO, help=f"Arquivo de saída JSON com as descrições geradas. Padrão: {OUTPUT_FILE_OPENAI_35TURBO}")
    parser.add_argument("--force_regenerate", action="store_true", help="Força a regeneração de todas as descrições, ignorando as existentes no arquivo de saída e as manuais.")
    # NOVO: Argumento para limitar itens
    parser.add_argument("--max_items", type=int, default=None, help="Número máximo de itens (colunas) para processar (para teste rápido).")
    args = parser.parse_args()

    logger.info("--- Iniciando Geração de Descrições com OpenAI ---")
    logger.info(f"Schema de entrada: {args.input}")
    logger.info(f"Arquivo de saída: {args.output}")
    if args.force_regenerate:
        logger.warning("*** MODO FORCE_REGENERATE ATIVO: Todas as descrições serão geradas novamente. ***")

    # Checar se a chave foi carregada antes de iniciar o processo
    if not _openai_api_key:
        logger.critical("Chave da API da OpenAI não encontrada na configuração (.streamlit/secrets.toml ou env var OPENAI_API_KEY). Abortando.")
        return

    # Carregar schema técnico
    schema_load_start = time.perf_counter()
    schema = load_json_safe(args.input)
    schema_load_end = time.perf_counter()
    if not schema:
        logger.critical(f"Schema técnico em {args.input} não pôde ser carregado. Verifique o caminho e o formato do arquivo. Abortando.")
        return
    logger.info(f"Schema técnico carregado em: {schema_load_end - schema_load_start:.2f}s")

    # NOVO: Carregar contagens de linhas
    counts_load_start = time.perf_counter()
    logger.info(f"Carregando contagens de linhas de {ROW_COUNTS_FILE}...")
    row_counts = load_json_safe(ROW_COUNTS_FILE)
    counts_load_end = time.perf_counter()
    if not row_counts:
        logger.warning(f"Arquivo {ROW_COUNTS_FILE} não encontrado ou inválido. Não será possível pular objetos vazios.")
        row_counts = {} # Define como dict vazio para evitar erros posteriores
    logger.info(f"Contagens de linhas carregadas em: {counts_load_end - counts_load_start:.2f}s")

    # NOVO: Carregar schema combinado para descrições manuais
    comb_schema_start = time.perf_counter()
    logger.info(f"Carregando schema combinado de {COMBINED_SCHEMA_FILE} para verificar descrições manuais...")
    combined_schema = load_json_safe(COMBINED_SCHEMA_FILE)
    comb_schema_end = time.perf_counter()
    if not combined_schema:
        logger.warning(f"Não foi possível carregar {COMBINED_SCHEMA_FILE}. Não será possível pular colunas com descrições manuais.")
        combined_schema = {}
    logger.info(f"Schema combinado carregado em: {comb_schema_end - comb_schema_start:.2f}s")

    # NOVO: Carregar descrições AI JÁ GERADAS
    load_ai_desc_start = time.perf_counter()
    initial_processed_ai_identifiers = set()
    results_list = [] # Começa vazia se forçar, senão carrega existentes
    if not args.force_regenerate:
        initial_processed_ai_identifiers, results_list = load_existing_ai_descriptions(args.output)
        logger.info(f"Iniciando com {len(results_list)} descrições pré-existentes geradas por IA.")
    else:
        logger.info("Forçando regeneração: a lista de resultados inicia vazia.")
    load_ai_desc_end = time.perf_counter()
    logger.info(f"Carregamento de descrições AI existentes levou: {load_ai_desc_end - load_ai_desc_start:.2f}s")

    # Preparar itens para processar
    items_to_process = []
    skipped_zero_rows_objects_count = 0 # NOVO: Contador
    logger.info("Preparando lista de itens para processar (verificando contagem de linhas)...") # Log atualizado
    prep_items_start = time.perf_counter()
    for obj_name, obj_data in schema.items():
        # Ignora metadados internos como fk_reference_counts ou itens malformados
        if not isinstance(obj_data, dict) or 'object_type' not in obj_data:
            continue

        # NOVO: Pular objeto se a contagem de linhas for 0
        object_row_count = row_counts.get(obj_name, {}).get('count', -1) # Pega contagem, -1 se não existir
        if object_row_count == 0:
            logger.debug(f"Pulando objeto '{obj_name}' (contagem de linhas = 0).")
            skipped_zero_rows_objects_count += 1
            continue # Pula para o próximo objeto no schema

        obj_type = obj_data['object_type']
        # Processar colunas (somente se o objeto não foi pulado)
        if 'columns' in obj_data and isinstance(obj_data['columns'], list):
            for col_data in obj_data['columns']:
                if isinstance(col_data, dict) and 'name' in col_data:
                    items_to_process.append((obj_type, obj_name, col_data))
                else:
                    logger.warning(f"Coluna sem nome ou malformada encontrada em {obj_name}")
        # TODO: Adicionar tabelas/views se necessário (a lógica de pular objeto já se aplica)

    prep_items_end = time.perf_counter()
    logger.info(f"Preparação de {len(items_to_process)} itens (após filtro de contagem) levou: {prep_items_end - prep_items_start:.2f}s") # Log atualizado

    # NOVO: Limitar itens se --max_items for especificado
    if args.max_items is not None:
        logger.warning(f"*** Limitando processamento aos primeiros {args.max_items} itens. ***")
        items_to_process = items_to_process[:args.max_items]
        logger.info(f"Número de itens após limitação: {len(items_to_process)}")

    # Contadores
    generated_count = 0
    error_count = 0
    skipped_manual_count = 0 # NOVO
    skipped_ai_count = 0     # NOVO
    loop_start_time = time.perf_counter()

    # Usar tqdm para barra de progresso
    pbar = tqdm(items_to_process, desc="Gerando descrições com OpenAI")

    for item_data in pbar:
        obj_type, obj_name, col_data = item_data
        col_name = col_data.get('name')
        if not col_name:
             continue

        identifier = get_item_identifier(obj_type, obj_name, col_name)
        pbar.set_postfix_str(f"Verificando {identifier}...") # Atualiza status

        # --- INÍCIO: Lógica para Pular --- #
        should_skip = False
        if not args.force_regenerate:
            # 1. Verificar Descrição MANUAL
            if obj_name in combined_schema:
                try:
                    column_details_combined = None
                    if 'columns' in combined_schema[obj_name] and isinstance(combined_schema[obj_name]['columns'], list):
                        for c in combined_schema[obj_name]['columns']:
                            if isinstance(c, dict) and c.get('name') == col_name:
                                column_details_combined = c
                                break
                    if column_details_combined:
                        manual_desc = column_details_combined.get('business_description')
                        if manual_desc and manual_desc.strip():
                            pbar.set_postfix_str(f"Pulado (manual): {identifier}")
                            skipped_manual_count += 1
                            should_skip = True
                except Exception as e_comb:
                     logger.warning(f"Erro ao verificar desc manual para {identifier}: {e_comb}")

            # 2. Verificar Descrição AI Existente (só se não pulou pela manual)
            if not should_skip and identifier in initial_processed_ai_identifiers:
                pbar.set_postfix_str(f"Pulado (AI): {identifier}")
                skipped_ai_count += 1
                should_skip = True

        if should_skip:
            continue # Pula para o próximo item
        # --- FIM: Lógica para Pular --- #

        # Se não pulou, GERA a descrição
        pbar.set_postfix_str(f"Gerando: {identifier}")
        prompt = build_prompt(obj_type, obj_name, col_data)

        generation_start = time.perf_counter()
        generated_desc = generate_description_with_openai(prompt)
        generation_end = time.perf_counter()

        if generated_desc:
            generated_count += 1
            result_item = {
                "object_type": obj_type,
                "object_name": obj_name,
                "column_name": col_name,
                "generated_description": generated_desc,
                "model_used": "gpt-3.5-turbo", # ATUALIZADO: Refletir modelo usado
                "generation_timestamp": datetime.utcnow().isoformat() + "Z"
            }
            results_list.append(result_item)
            # Adiciona ao set de processados desta execução para evitar duplicatas na mesma run
            initial_processed_ai_identifiers.add(identifier)
            pbar.set_postfix_str(f"OK: {identifier} ({generation_end - generation_start:.2f}s)")
        else:
            error_count += 1
            pbar.set_postfix_str(f"ERRO: {identifier}")
            logger.error(f"Falha ao gerar descrição para {identifier}")
            # Considerar adicionar uma pausa curta em caso de erro para evitar sobrecarregar a API
            time.sleep(1)

    pbar.close()
    loop_end_time = time.perf_counter()
    logger.info(f"--- Loop de Geração Concluído em: {loop_end_time - loop_start_time:.2f}s ---")

    logger.info(f"--- Resumo da Execução (OpenAI) ---")
    logger.info(f"Novas descrições geradas: {generated_count}")
    logger.info(f"Itens pulados (objetos com 0 linhas): {skipped_zero_rows_objects_count}") # NOVO Log
    logger.info(f"Itens pulados (desc. manual existente): {skipped_manual_count}")
    logger.info(f"Itens pulados (desc. AI existente): {skipped_ai_count}")
    logger.info(f"Erros durante a geração: {error_count}")
    logger.info(f"Total de descrições no arquivo final: {len(results_list)}")

    save_start = time.perf_counter()
    logger.info(f"Salvando resultados em {args.output}...")
    save_json_safe(results_list, args.output)
    save_end = time.perf_counter()
    logger.info(f"Salvamento concluído em: {save_end - save_start:.2f}s")

    script_end_time = time.perf_counter()
    logger.info(f"--- Tempo Total de Execução do Script (OpenAI): {script_end_time - script_start_time:.2f}s ---")

if __name__ == '__main__':
    main()