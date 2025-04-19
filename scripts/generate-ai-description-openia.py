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
from openai import OpenAI, RateLimitError, APIError, APITimeoutError, APIConnectionError
from dotenv import load_dotenv

# --- NOVO: Adiciona a raiz do projeto ao sys.path ---
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM NOVO ---

# Importar funções utilitárias e de geração
from src.core.utils import load_json_safe
# from src.core.ai_integration import generate_description_with_openai
from src.core.logging_config import setup_logging
from src.core.config import (
    MERGED_SCHEMA_FOR_EMBEDDINGS_FILE, 
    AI_DESCRIPTIONS_FILE, 
    ROW_COUNTS_FILE # Mantido se a lógica de pular linhas 0 for útil
)

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
# ARQUIVOS DE ENTRADA/SAÍDA AGORA VÊM DE config.py
# TECHNICAL_SCHEMA_FILE = "data/enhanced_technical_schema.json" # REMOVIDO
# COMBINED_SCHEMA_FILE = "data/combined_schema_details.json" # REMOVIDO
# ROW_COUNTS_FILE = "data/overview_counts.json" # Mantido, vem de config
# OUTPUT_FILE_OPENAI_35TURBO = "data/ai_generated_descriptions_openai_35turbo.json" # REMOVIDO, usar constante de config

# Configurar logging (usando a função centralizada)
setup_logging()
logger = logging.getLogger(__name__)

# --- Funções Auxiliares --- #

# NOVO: Identificador único para itens
def get_item_identifier(obj_type, obj_name, col_name=None):
    # Normalizar para maiúsculas para consistência
    obj_name_norm = obj_name.strip().upper() if obj_name else 'N/A'
    col_name_norm = col_name.strip().upper() if col_name else '__TABLE__'
    return f"{obj_type}:{obj_name_norm}:{col_name_norm}"

# NOVO: Carregar descrições AI existentes (do arquivo de saída)
def load_existing_ai_output(filename):
    """Carrega descrições AI já geradas do ARQUIVO DE SAÍDA para evitar reprocessamento na mesma execução ou se não forçar."""
    existing_data = load_json_safe(filename)
    processed_output_ids = set()
    results_list = []
    if existing_data and isinstance(existing_data, list):
        results_list = existing_data # Retorna a lista para continuar append
        for item in existing_data:
            try:
                if all(k in item for k in ('object_type', 'object_name')):
                    identifier = get_item_identifier(item['object_type'], item['object_name'], item.get('column_name'))
                    processed_output_ids.add(identifier)
                else:
                    logger.warning(f"Item inválido (sem chaves obrigatórias) encontrado no arquivo de saída {filename}: {item}")
            except KeyError:
                logger.warning(f"Item inválido (KeyError) encontrado no arquivo de saída {filename}: {item}")
        logger.info(f"{len(processed_output_ids)} identificadores de descrições AI existentes carregados do arquivo de saída {filename}.")
    return processed_output_ids, results_list

# REFINADO: Prompt mais focado (adaptação mínima necessária, pois a estrutura deve ser similar)
def build_prompt(obj_type, obj_name, col_data=None):
    """Constrói o prompt para gerar a descrição de uma coluna ou tabela/view."""
    if col_data:
        col_name = col_data.get('name', 'N/A')
        prompt = f"""
        Tabela/View: {obj_name}
        Coluna: {col_name}
        Tipo Técnico: {col_data.get('type', 'N/A')}
        """
        # --- NOVO: Adicionar informação de PK/FK ---
        if col_data.get('is_pk'):
            prompt += "\nÉ Chave Primária: Sim"
        if col_data.get('is_fk'):
            prompt += "\nÉ Chave Estrangeira: Sim"
            # Tentar adicionar detalhes da referência
            fk_ref = col_data.get('fk_references') 
            if fk_ref and isinstance(fk_ref, dict):
                ref_table = fk_ref.get('references_table')
                ref_column = fk_ref.get('references_field') # Ou nome similar
                if ref_table:
                    prompt += f" (Referencia: {ref_table}{f' ({ref_column})' if ref_column else ''})"
        # --- FIM NOVO ---
        
        prompt += f"\nDescrição Técnica Original: {col_data.get('description', 'Nenhuma')} "

        # Incluir amostras (verificar se 'sample_values' existe no schema mesclado)
        samples = col_data.get('sample_values', [])
        # Garantir que samples seja uma lista
        if samples and isinstance(samples, list) and samples != ["BOOLEAN_SKIPPED"]:
             sample_limit = 10 # Aumentado para 10 conforme solicitado anteriormente
             # Mapear para string e juntar
             samples_str = ', '.join(map(str, samples[:sample_limit]))
             prompt += f"\nAmostra de Valores ({min(len(samples), sample_limit)}): {samples_str}"
        elif samples == ["BOOLEAN_SKIPPED"]:
             prompt += "\nAmostra de Valores: Ignorada (tipo booleano inferido)"

        # Instrução clara e focada
        prompt += f"\n\nTarefa: Gere uma descrição de negócio clara e concisa (1-2 frases) para a coluna \"{col_name}\" da tabela \"{obj_name}\". Explique seu propósito principal para um usuário de negócio ou analista de dados. Use português brasileiro (pt-BR)."""
    else:
        # Prompt para Tabela/View (mantido, caso seja usado no futuro)
        prompt = f"""
        Objeto: {obj_name}
        Tipo: {obj_type}
        \nTarefa: Gere uma descrição de negócio clara e concisa (1-2 frases) para {obj_type.lower()} \"{obj_name}\". Explique seu propósito principal para um usuário de negócio ou analista de dados. Use português brasileiro (pt-BR)."""

    return prompt

# ATUALIZADO: Para usar OpenAI V1.x (sem alterações lógicas aqui)
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
        # Usa Pathlib para criar diretórios
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Arquivo salvo com sucesso em {output_path}")
    except Exception as e:
        logger.error(f"Erro ao salvar JSON em {path}: {e}")

# --- Função Principal --- #

def main():
    script_start_time = time.perf_counter()

    parser = argparse.ArgumentParser(description="Gera descrições de negócio para colunas de banco de dados usando a API da OpenAI (GPT-3.5-Turbo), lendo do schema mesclado.")
    # ATUALIZADO: Usar constantes de config para padrões
    parser.add_argument("-i", "--input", default=MERGED_SCHEMA_FOR_EMBEDDINGS_FILE, help=f"Caminho para o arquivo JSON com o schema mesclado. Padrão: {MERGED_SCHEMA_FOR_EMBEDDINGS_FILE}")
    parser.add_argument("-o", "--output", default=AI_DESCRIPTIONS_FILE, help=f"Arquivo de saída JSON com as descrições geradas. Padrão: {AI_DESCRIPTIONS_FILE}")
    parser.add_argument("--force_regenerate", action="store_true", help="Força a regeneração de todas as descrições, ignorando as existentes no arquivo de entrada e no arquivo de saída.")
    parser.add_argument("--max_items", type=int, default=None, help="Número máximo de itens (colunas) para processar (para teste rápido).")
    # REMOVIDO: Argumento para schema combinado, não é mais necessário
    args = parser.parse_args()

    logger.info("--- Iniciando Geração de Descrições com OpenAI (a partir do Schema Mesclado) ---")
    logger.info(f"Schema de entrada (mesclado): {args.input}")
    logger.info(f"Arquivo de saída: {args.output}")
    if args.force_regenerate:
        logger.warning("*** MODO FORCE_REGENERATE ATIVO: Todas as descrições serão geradas novamente. ***")

    if not _openai_api_key:
        logger.critical("Chave da API da OpenAI não encontrada. Abortando.")
        return

    # Carregar schema MESCLADO (principal fonte agora)
    schema_load_start = time.perf_counter()
    schema = load_json_safe(args.input)
    schema_load_end = time.perf_counter()
    if not schema:
        logger.critical(f"Schema mesclado em {args.input} não pôde ser carregado. Abortando.")
        return
    logger.info(f"Schema mesclado carregado em: {schema_load_end - schema_load_start:.2f}s")

    # Carregar contagens de linhas (opcional, mas mantido por enquanto)
    counts_load_start = time.perf_counter()
    logger.info(f"Carregando contagens de linhas de {ROW_COUNTS_FILE}...")
    row_counts = load_json_safe(ROW_COUNTS_FILE)
    counts_load_end = time.perf_counter()
    if not row_counts:
        logger.warning(f"Arquivo {ROW_COUNTS_FILE} não encontrado ou inválido. Não será possível pular objetos vazios.")
        row_counts = {} 
    logger.info(f"Contagens de linhas carregadas em: {counts_load_end - counts_load_start:.2f}s")

    # Carregar descrições AI JÁ GERADAS do arquivo de SAÍDA
    load_ai_desc_start = time.perf_counter()
    processed_output_identifiers = set()
    results_list = [] 
    if not args.force_regenerate:
        # Carrega o conteúdo do arquivo de saída para continuar de onde parou
        processed_output_identifiers, results_list = load_existing_ai_output(args.output)
        logger.info(f"Iniciando com {len(results_list)} descrições pré-existentes lidas de {args.output}.")
    else:
        logger.info("Forçando regeneração: a lista de resultados inicia vazia.")
    load_ai_desc_end = time.perf_counter()
    logger.info(f"Carregamento de descrições AI existentes (saída) levou: {load_ai_desc_end - load_ai_desc_start:.2f}s")

    # Preparar itens para processar (iterando sobre o schema mesclado)
    items_to_process = []
    skipped_zero_rows_objects_count = 0
    logger.info("Preparando lista de colunas para processar (a partir do schema mesclado)...")
    prep_items_start = time.perf_counter()
    # ATUALIZADO: Iterar sobre a estrutura do schema mesclado
    if isinstance(schema, dict):
        for obj_name, obj_data in schema.items():
            if not isinstance(obj_data, dict) or 'object_type' not in obj_data:
                 continue # Ignora entradas malformadas ou metadados internos

            # Pular objeto se a contagem de linhas for 0 (lógica mantida)
            object_row_count = row_counts.get(obj_name, {}).get('count', -1)
            if object_row_count == 0:
                logger.debug(f"Pulando objeto '{obj_name}' (contagem de linhas = 0).")
                skipped_zero_rows_objects_count += 1
                continue 

            obj_type = obj_data['object_type']
            if 'columns' in obj_data and isinstance(obj_data['columns'], list):
                for col_data in obj_data['columns']:
                    if isinstance(col_data, dict) and 'name' in col_data:
                         # Guarda a tupla (tipo, nome_obj, dict_coluna)
                        items_to_process.append((obj_type, obj_name, col_data)) 
                    else:
                        logger.warning(f"Coluna sem nome ou malformada encontrada em {obj_name}")
            # Não processamos tabelas/views inteiras por enquanto
    else:
        logger.error("Schema mesclado não tem o formato esperado (dicionário). Abortando.")
        return

    prep_items_end = time.perf_counter()
    logger.info(f"Preparação de {len(items_to_process)} colunas (após filtro de contagem) levou: {prep_items_end - prep_items_start:.2f}s") 

    if args.max_items is not None:
        logger.warning(f"*** Limitando processamento às primeiras {args.max_items} colunas. ***")
        items_to_process = items_to_process[:args.max_items]
        logger.info(f"Número de colunas após limitação: {len(items_to_process)}")

    generated_count = 0
    error_count = 0
    skipped_manual_count = 0 
    skipped_ai_in_input_count = 0 # Renomeado para clareza
    skipped_ai_in_output_count = 0 # Renomeado para clareza
    skipped_empty_samples_count = 0 # NOVO: Contador para amostras vazias
    loop_start_time = time.perf_counter()

    pbar = tqdm(items_to_process, desc="Gerando descrições com OpenAI")

    for item_data in pbar:
        obj_type, obj_name, col_data = item_data # col_data é a referência ao dict da coluna no schema carregado
        col_name = col_data.get('name')
        if not col_name:
             continue

        identifier = get_item_identifier(obj_type, obj_name, col_name)
        pbar.set_postfix_str(f"Verificando {identifier}...") 

        # --- INÍCIO: Lógica para Pular (Atualizada) --- #
        should_skip = False
        skip_reason = ""
        if not args.force_regenerate:
            # 1. Verificar Descrição MANUAL no schema de entrada
            manual_desc = col_data.get('business_description')
            if manual_desc and str(manual_desc).strip(): # Verifica se existe e não é vazia
                skip_reason = "manual (entrada)"
                skipped_manual_count += 1
                should_skip = True

            # 2. Verificar Descrição AI no schema de entrada (só se não pulou pela manual)
            if not should_skip:
                 ai_desc_input = col_data.get('ai_generated_description')
                 if ai_desc_input and str(ai_desc_input).strip():
                     skip_reason = "AI (entrada)"
                     skipped_ai_in_input_count += 1
                     should_skip = True
                     
            # 3. Verificar se já existe no ARQUIVO DE SAÍDA desta execução (só se não pulou antes)
            #    Isso evita duplicar chamadas na mesma execução se houver erro e restart
            if not should_skip and identifier in processed_output_identifiers:
                skip_reason = "AI (saída)"
                skipped_ai_in_output_count += 1
                should_skip = True

        # --- NOVO: Adicionar verificação de Sample Values --- 
        if not should_skip:
            sample_values = col_data.get('sample_values')
            # Pula se sample_values não existe, é None ou é uma lista vazia
            if not sample_values: # Checa None e lista vazia implicitamente
                skip_reason = "amostra vazia"
                skipped_empty_samples_count += 1
                should_skip = True
        # --- FIM NOVO --- 

        if should_skip:
            pbar.set_postfix_str(f"Pulado ({skip_reason}): {identifier}")
            continue 
        # --- FIM: Lógica para Pular --- #

        # Gera a descrição
        pbar.set_postfix_str(f"Gerando: {identifier}")
        prompt = build_prompt(obj_type, obj_name, col_data)

        generation_start = time.perf_counter()
        generated_desc = generate_description_with_openai(prompt)
        generation_end = time.perf_counter()

        if generated_desc:
            generated_count += 1
            result_item = {
                "object_type": obj_type,
                "object_name": obj_name, # Usar nome original para consistência na saída
                "column_name": col_name,
                "generated_description": generated_desc,
                "model_used": "gpt-3.5-turbo", 
                "generation_timestamp": datetime.utcnow().isoformat() + "Z"
            }
            results_list.append(result_item)
            # Adiciona ao set de processados do arquivo de saída para evitar duplicatas na mesma run
            processed_output_identifiers.add(identifier) 
            pbar.set_postfix_str(f"OK: {identifier} ({generation_end - generation_start:.2f}s)")
        else:
            error_count += 1
            pbar.set_postfix_str(f"ERRO: {identifier}")
            logger.error(f"Falha ao gerar descrição para {identifier}")
            time.sleep(1)

    pbar.close()
    loop_end_time = time.perf_counter()
    logger.info(f"--- Loop de Geração Concluído em: {loop_end_time - loop_start_time:.2f}s ---")

    logger.info(f"--- Resumo da Execução (OpenAI) ---")
    logger.info(f"Novas descrições geradas nesta execução: {generated_count}")
    logger.info(f"Itens pulados (objetos com 0 linhas): {skipped_zero_rows_objects_count}") 
    logger.info(f"Itens pulados (desc. manual encontrada na entrada): {skipped_manual_count}")
    logger.info(f"Itens pulados (desc. AI encontrada na entrada): {skipped_ai_in_input_count}")
    logger.info(f"Itens pulados (desc. AI já existente na saída): {skipped_ai_in_output_count}")
    logger.info(f"Itens pulados (sem amostra de valores): {skipped_empty_samples_count}") # NOVO Log
    logger.info(f"Erros durante a geração: {error_count}")
    logger.info(f"Total de descrições no arquivo de saída final: {len(results_list)}")

    save_start = time.perf_counter()
    logger.info(f"Salvando resultados em {args.output}...")
    save_json_safe(results_list, args.output)
    save_end = time.perf_counter()
    logger.info(f"Salvamento concluído em: {save_end - save_start:.2f}s")

    script_end_time = time.perf_counter()
    logger.info(f"--- Tempo Total de Execução do Script (OpenAI): {script_end_time - script_start_time:.2f}s ---")

if __name__ == '__main__':
    main()