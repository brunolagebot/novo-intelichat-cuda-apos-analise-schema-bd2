import os
import json
import logging
import argparse
from datetime import datetime
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import PeftModel
from tqdm import tqdm
import sys
import time # NOVO: Para medir performance
import faiss # NOVO: Para carregar índice FAISS
import numpy as np # NOVO: Para manipular embeddings

# --- NOVO: Adiciona a raiz do projeto ao sys.path ---
# Isso permite que o script encontre módulos como 'src.core'
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- FIM NOVO ---

# Importar funções utilitárias e de geração
from src.core.utils import load_json_safe
from src.core.ai_integration import generate_description_with_adapter
from src.core.logging_config import setup_logging

# Configurar logging (usando a função centralizada)
setup_logging()
logger = logging.getLogger(__name__)

# --- Constantes e Configuração --- #
# TODO: Mover para src/core/config.py se apropriado
BASE_MODEL_NAME = "meta-llama/Meta-Llama-3-8B-Instruct"
ADAPTER_PATH = "./results-llama3-8b-chat-schema-adapter"
TECHNICAL_SCHEMA_FILE = "data/enhanced_technical_schema.json"
COMBINED_SCHEMA_FILE = "data/combined_schema_details.json" # Usado para descrições manuais
# NOVO: Padrões para arquivos de embedding/FAISS
DEFAULT_EMBEDDINGS_FILE = "data/schema_with_embeddings.json"
DEFAULT_FAISS_INDEX_FILE = "data/faiss_column_index.idx"
OUTPUT_FILE = "data/ai_generated_descriptions.json"

# Identificador único para itens já processados (evita reprocessar)
def get_item_identifier(obj_type, obj_name, col_name=None):
    return f"{obj_type}:{obj_name}:{col_name if col_name else '__TABLE__'}"

# --- Funções Auxiliares --- #

def load_model_and_tokenizer(base_model_name, adapter_path):
    """Carrega o modelo base, o tokenizer e aplica o adaptador PEFT."""
    func_start_time = time.perf_counter()

    logger.info(f"Carregando tokenizer para {base_model_name}...")
    tokenizer_start_time = time.perf_counter()
    tokenizer = AutoTokenizer.from_pretrained(base_model_name)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
        logger.info("Tokenizer pad_token configurado para eos_token.")
    tokenizer_end_time = time.perf_counter()
    logger.info(f" -> Tokenizer carregado em: {tokenizer_end_time - tokenizer_start_time:.2f}s")

    logger.info(f"Carregando modelo base {base_model_name}...")
    base_model_start_time = time.perf_counter()
    model = AutoModelForCausalLM.from_pretrained(
        base_model_name,
        torch_dtype=torch.bfloat16, # Carregar em bfloat16 para economizar memória
        device_map="auto" # Tentar usar GPU automaticamente e distribuir se necessário
    )
    base_model_end_time = time.perf_counter()
    logger.info(f" -> Modelo base carregado em: {base_model_end_time - base_model_start_time:.2f}s")

    # MODIFICADO: Só carrega adaptador se o caminho for fornecido
    if adapter_path and os.path.exists(adapter_path):
        logger.info(f"Carregando adaptador PEFT de {adapter_path}...")
        adapter_start_time = time.perf_counter()
        try:
            model = PeftModel.from_pretrained(model, adapter_path)
            adapter_end_time = time.perf_counter()
            logger.info(f" -> Adaptador PEFT carregado com sucesso em: {adapter_end_time - adapter_start_time:.2f}s")
        except Exception as e:
            logger.error(f"Falha ao carregar adaptador PEFT de {adapter_path}: {e}", exc_info=True)
            logger.warning("Erro no adaptador PEFT. Prosseguindo apenas com o modelo base.")
            # Poderia abortar aqui se o adaptador for essencial
    elif adapter_path:
        logger.warning(f"Caminho do adaptador fornecido ({adapter_path}) não encontrado. Usando apenas o modelo base.")
    else:
        logger.info("Nenhum caminho de adaptador fornecido. Usando apenas o modelo base.")

    model.eval()

    # O movimento para GPU é tratado por device_map="auto" durante o from_pretrained
    # Se não usasse device_map="auto", mediríamos model.to(device) aqui
    # device_move_start = time.perf_counter()
    # if torch.cuda.is_available():
    #    logger.info("Movendo modelo para GPU...")
    #    model = model.to('cuda')
    # else:
    #    logger.info("GPU não disponível, usando CPU.")
    # device_move_end = time.perf_counter()
    # logger.info(f" -> Movimento para device levou: {device_move_end - device_move_start:.2f}s")

    func_end_time = time.perf_counter()
    logger.info(f"Tempo total em load_model_and_tokenizer: {func_end_time - func_start_time:.2f}s")
    return model, tokenizer

def build_prompt(obj_type, obj_name, col_data=None, similar_descriptions=None):
    """Constrói o prompt para gerar a descrição de uma tabela ou coluna, opcionalmente com contexto similar."""
    if col_data: # É uma coluna
        prompt = f"Contexto do Banco de Dados:\n"
        prompt += f"- Tabela/View: {obj_name}\n"
        prompt += f"- Coluna: {col_data.get('name', 'N/A')}\n"
        prompt += f"- Tipo Técnico: {col_data.get('type', 'N/A')}\n"
        prompt += f"- Aceita Nulos: {'Sim' if col_data.get('nullable') else 'Não'}\n"
        default_val = col_data.get('default_value')
        if default_val:
            prompt += f"- Valor Padrão: {default_val}\n"
        tech_desc = col_data.get('description')
        if tech_desc:
            prompt += f"- Descrição Técnica: {tech_desc}\n"
        samples = col_data.get('sample_values', [])
        if samples and samples != ["BOOLEAN_SKIPPED"]:
            sample_limit = 5
            prompt += f"- Amostra de Valores ({min(len(samples), sample_limit)} primeiros): {samples[:sample_limit]}\n"

        # NOVO: Adicionar contexto de similaridade se disponível
        if similar_descriptions:
            prompt += f"\n\nContexto Adicional (Descrições Manuais de Colunas Similares):\n"
            prompt += "\n".join(similar_descriptions)
            prompt += "\n"

        prompt += f"\nTarefa: Gere uma descrição de negócio concisa e clara para a coluna \"{col_data.get('name', 'N/A')}\" da tabela/view \"{obj_name}\", em Português brasileiro (pt-br). Explique seu propósito principal no contexto do negócio."

    else: # É uma tabela/view
        # TODO: Aprimorar prompt para tabelas (usar lista de colunas, desc técnica da tabela?)
        prompt = f"Contexto do Banco de Dados:\n"
        prompt += f"- Tipo de Objeto: {obj_type}\n"
        prompt += f"- Nome: {obj_name}\n"
        # tech_desc_table = ... # Buscar descrição técnica da tabela do schema
        # if tech_desc_table:
        #     prompt += f"- Descrição Técnica: {tech_desc_table}\n"
        prompt += f"\nTarefa: Gere uma descrição de negócio concisa e clara para a {obj_type.lower()} \"{obj_name}\", em Português brasileiro (pt-br). Explique seu propósito principal no contexto do negócio."

    return prompt

def load_existing_descriptions(filename):
    """Carrega descrições já geradas para evitar reprocessamento."""
    existing_data = load_json_safe(filename)
    processed_ids = set()
    if existing_data and isinstance(existing_data, list):
        for item in existing_data:
            try:
                identifier = get_item_identifier(item['object_type'], item['object_name'], item.get('column_name'))
                processed_ids.add(identifier)
            except KeyError:
                logger.warning(f"Item inválido encontrado em {filename}: {item}")
        logger.info(f"{len(processed_ids)} descrições existentes carregadas de {filename}.")
    return processed_ids, (existing_data if isinstance(existing_data, list) else [])

# NOVO: Função para carregar FAISS e Embeddings
def load_faiss_and_embeddings(index_path, embeddings_file_path):
    """Carrega o índice FAISS, o arquivo de embeddings e cria o mapeamento ID->Coluna."""
    faiss_index = None
    embeddings_data = None
    index_to_column_map = [] # Lista ordenada [(table, column, embedding_vector)]
    column_to_embedding_map = {} # Mapa para buscar embedding por (table, column)

    load_start_time = time.perf_counter()

    # Carregar índice FAISS
    if os.path.exists(index_path):
        try:
            logger.info(f"Carregando índice FAISS de {index_path}...")
            faiss_start = time.perf_counter()
            faiss_index = faiss.read_index(index_path)
            faiss_end = time.perf_counter()
            logger.info(f" -> Índice FAISS carregado com {faiss_index.ntotal} vetores em {faiss_end - faiss_start:.2f}s.")
        except Exception as e:
            logger.error(f"Falha ao carregar índice FAISS de {index_path}: {e}", exc_info=True)
            faiss_index = None # Garante que está None em caso de erro
    else:
        logger.warning(f"Arquivo de índice FAISS não encontrado em {index_path}. Busca por similaridade desabilitada.")

    # Carregar arquivo JSON com embeddings
    if os.path.exists(embeddings_file_path):
        try:
            logger.info(f"Carregando schema com embeddings de {embeddings_file_path}...")
            embed_load_start = time.perf_counter()
            embeddings_data = load_json_safe(embeddings_file_path)
            embed_load_end = time.perf_counter()
            if not embeddings_data:
                 raise ValueError("Arquivo de embeddings JSON vazio ou inválido.")
            logger.info(f" -> Schema com embeddings carregado em {embed_load_end - embed_load_start:.2f}s.")

            # Criar mapeamentos (assumindo ordem consistente)
            logger.info("Criando mapeamentos ID <-> Coluna e Coluna -> Embedding...")
            map_build_start = time.perf_counter()
            current_index_id = 0
            for table_name, table_data in embeddings_data.items():
                if not isinstance(table_data, dict) or 'columns' not in table_data:
                    continue
                for column_data in table_data.get('columns', []):
                    if isinstance(column_data, dict) and 'name' in column_data and 'embedding' in column_data:
                        col_name = column_data['name']
                        embedding_vector = column_data['embedding']
                        if embedding_vector: # Garante que o embedding não está vazio/None
                            # Adiciona à lista ordenada para mapear ID -> Coluna
                            index_to_column_map.append((table_name, col_name))
                            # Adiciona ao mapa para buscar embedding por Coluna
                            column_to_embedding_map[(table_name, col_name)] = np.array(embedding_vector, dtype='float32')
                            current_index_id += 1
                        else:
                            logger.debug(f"Coluna {table_name}.{col_name} encontrada sem vetor de embedding no arquivo.")
            map_build_end = time.perf_counter()
            logger.info(f" -> Mapeamentos criados em {map_build_end - map_build_start:.2f}s. {len(index_to_column_map)} colunas com embeddings mapeadas.")

            # Validar contagem do FAISS vs Mapeamento
            if faiss_index and faiss_index.ntotal != len(index_to_column_map):
                logger.warning(f"Contagem de vetores no índice FAISS ({faiss_index.ntotal}) difere do número de colunas com embedding mapeadas ({len(index_to_column_map)}). A busca por similaridade pode falhar ou retornar resultados incorretos.")

        except Exception as e:
            logger.error(f"Falha ao carregar ou processar {embeddings_file_path}: {e}", exc_info=True)
            embeddings_data = None
            index_to_column_map = []
            column_to_embedding_map = {}
    else:
        logger.warning(f"Arquivo de schema com embeddings não encontrado em {embeddings_file_path}. Busca por similaridade desabilitada.")

    load_end_time = time.perf_counter()
    logger.info(f"Tempo total em load_faiss_and_embeddings: {load_end_time - load_start_time:.2f}s")

    # Retorna None para o índice se não pôde ser carregado/validado corretamente com o mapa
    valid_faiss_index = faiss_index if (faiss_index and index_to_column_map and faiss_index.ntotal == len(index_to_column_map)) else None
    if faiss_index and not valid_faiss_index:
         logger.error("Índice FAISS será desconsiderado devido à inconsistência com o mapeamento de colunas.")

    return valid_faiss_index, index_to_column_map, column_to_embedding_map

# --- Função Principal --- #

def main():
    script_start_time = time.perf_counter() # NOVO: Início do script

    parser = argparse.ArgumentParser(description="Gera descrições de negócio para metadados usando IA, com opção de enriquecimento via embeddings.")
    parser.add_argument("-i", "--input", default=TECHNICAL_SCHEMA_FILE, help=f"Caminho do schema técnico base (JSON). Padrão: {TECHNICAL_SCHEMA_FILE}")
    parser.add_argument("-o", "--output", default=OUTPUT_FILE, help=f"Caminho do arquivo de saída para descrições geradas (JSON). Padrão: {OUTPUT_FILE}")
    parser.add_argument("-a", "--adapter", default=ADAPTER_PATH, help=f"Caminho para o diretório do adaptador PEFT (opcional). Padrão: {ADAPTER_PATH}")
    parser.add_argument("-b", "--base_model", default=BASE_MODEL_NAME, help=f"Nome ou caminho do modelo base. Padrão: {BASE_MODEL_NAME}")
    # NOVO: Argumentos para embeddings/FAISS
    parser.add_argument("--embeddings_file", default=DEFAULT_EMBEDDINGS_FILE, help=f"Caminho do schema JSON com embeddings. Padrão: {DEFAULT_EMBEDDINGS_FILE}")
    parser.add_argument("--faiss_index", default=DEFAULT_FAISS_INDEX_FILE, help=f"Caminho do arquivo de índice FAISS. Padrão: {DEFAULT_FAISS_INDEX_FILE}")
    parser.add_argument("--enable_similarity_enrichment", action='store_true', help="Ativa o enriquecimento do prompt com descrições de colunas similares via FAISS.")
    parser.add_argument("--similarity_top_k", type=int, default=5, help="Número de vizinhos similares a buscar no FAISS (incluindo a própria coluna).")
    parser.add_argument("--force_regenerate", action='store_true', help="Força a regeneração de todas as descrições, ignorando as existentes no arquivo de saída.")
    parser.add_argument("--max_items", type=int, default=None, help="Número máximo de itens (colunas) para processar (para teste rápido).")
    args = parser.parse_args()

    logger.info("--- Iniciando Geração de Descrições com IA (com logs de performance e opção de embeddings) ---")
    logger.info(f"Usando modelo base: {args.base_model}")
    logger.info(f"Usando adaptador: {args.adapter}")
    logger.info(f"Schema de entrada: {args.input}")
    logger.info(f"Arquivo de saída: {args.output}")

    # Carregar modelo e tokenizer (já tem logs internos)
    model, tokenizer = load_model_and_tokenizer(args.base_model, args.adapter)
    if model is None or tokenizer is None:
        logger.critical("Falha ao carregar modelo ou tokenizer. Abortando.")
        return

    # Carregar schema técnico
    tech_schema_start = time.perf_counter()
    technical_schema = load_json_safe(args.input)
    tech_schema_end = time.perf_counter()
    if not technical_schema:
        logger.critical(f"Falha ao carregar schema técnico de {args.input}. Abortando.")
        return
    logger.info(f"Schema técnico carregado em: {tech_schema_end - tech_schema_start:.2f}s")

    # Carregar schema combinado
    comb_schema_start = time.perf_counter()
    logger.info(f"Carregando schema combinado de {COMBINED_SCHEMA_FILE} para verificar descrições manuais...")
    combined_schema = load_json_safe(COMBINED_SCHEMA_FILE)
    comb_schema_end = time.perf_counter()
    if not combined_schema:
        logger.warning(f"Não foi possível carregar {COMBINED_SCHEMA_FILE}. Não será possível pular colunas com descrições manuais existentes.")
        combined_schema = {}
    logger.info(f"Schema combinado carregado em: {comb_schema_end - comb_schema_start:.2f}s")

    # NOVO: Carregar contagens de linhas
    counts_load_start = time.perf_counter()
    logger.info("Carregando contagens de linhas de data/overview_counts.json...")
    row_counts = load_json_safe(os.path.join(project_root, 'data', 'overview_counts.json'))
    counts_load_end = time.perf_counter()
    if not row_counts:
        logger.warning("Arquivo data/overview_counts.json não encontrado ou inválido. Não será possível pular objetos vazios.")
        row_counts = {} # Define como dict vazio para evitar erros
    logger.info(f"Contagens de linhas carregadas em: {counts_load_end - counts_load_start:.2f}s")

    # Carregar FAISS e Embeddings se ativado
    faiss_index = None
    index_to_column_map = []
    column_to_embedding_map = {}
    if args.enable_similarity_enrichment:
        logger.info("*** Enriquecimento por Similaridade ATIVADO ***")
        faiss_index, index_to_column_map, column_to_embedding_map = load_faiss_and_embeddings(args.faiss_index, args.embeddings_file)
        if not faiss_index:
            logger.warning("Falha ao carregar FAISS/Embeddings ou mapa inconsistente. Enriquecimento por similaridade será desativado.")
            args.enable_similarity_enrichment = False # Desativa se falhar
    else:
         logger.info("Enriquecimento por similaridade DESATIVADO.")

    # Carregar descrições JÁ GERADAS PELA IA
    load_ai_desc_start = time.perf_counter()
    initial_processed_ai_identifiers = set()
    results_list = []
    if not args.force_regenerate:
        initial_processed_ai_identifiers, results_list = load_existing_descriptions(args.output)
        logger.info(f"Iniciando com {len(results_list)} descrições pré-existentes geradas por IA.")
    else:
        logger.info("Forçando regeneração de todas as descrições (não pulará baseado em execuções anteriores de IA).")
    load_ai_desc_end = time.perf_counter()
    logger.info(f"Carregamento de descrições AI existentes levou: {load_ai_desc_end - load_ai_desc_start:.2f}s")

    # Iterar e gerar descrições
    items_to_process = []
    skipped_zero_rows_objects_count = 0
    logger.info("Preparando lista de itens para processar (verificando contagem de linhas)...")
    prep_items_start = time.perf_counter()
    for obj_name, obj_data in technical_schema.items():
        # Ignora metadados internos como fk_reference_counts
        if not isinstance(obj_data, dict) or 'object_type' not in obj_data:
            continue

        # NOVO: Pular objeto se a contagem de linhas for 0
        object_row_count = row_counts.get(obj_name, {}).get('count', -1) # Pega contagem, -1 se não existir
        if object_row_count == 0:
            logger.debug(f"Pulando objeto '{obj_name}' (contagem de linhas = 0).")
            skipped_zero_rows_objects_count += 1
            continue # Pula para o próximo objeto

        obj_type = obj_data['object_type']

        # Processar colunas
        if 'columns' in obj_data and isinstance(obj_data['columns'], list):
            for col_data in obj_data['columns']:
                if 'name' in col_data:
                    items_to_process.append((obj_type, obj_name, col_data))
                else:
                     logger.warning(f"Coluna sem nome encontrada em {obj_name}")
        # TODO: Adicionar lógica para processar a tabela/view em si
        # identifier_table = get_item_identifier(obj_type, obj_name)
        # if identifier_table not in processed_identifiers:
        #     items_to_process.append((obj_type, obj_name, None))

    # Limitar itens para teste se necessário
    if args.max_items is not None:
        logger.warning(f"Limitando processamento a {args.max_items} itens.")
        items_to_process = items_to_process[:args.max_items]

    # Barra de progresso e contadores
    pbar = tqdm(items_to_process, desc="Gerando Descrições")
    generated_count = 0
    error_count = 0
    skipped_manual_count = 0
    skipped_ai_count = 0
    skipped_empty_sample_count = 0
    skipped_no_embedding_count = 0
    enriched_prompt_count = 0
    total_similarity_search_time = 0.0
    total_context_lookup_time = 0.0
    total_prompt_build_time = 0.0
    total_generation_time = 0.0
    loop_start_time = time.perf_counter()

    for item_data in pbar:
        obj_type, obj_name, col_data = item_data
        col_name = col_data.get('name') if col_data else None
        if not col_name:
            logger.warning(f"Item sem nome de coluna encontrado em {obj_name}. Pulando.")
            continue # Pula se não tem nome de coluna
        identifier = get_item_identifier(obj_type, obj_name, col_name)
        current_column_key = (obj_name, col_name) # Chave para mapas

        # === Verificações para Pular Geração ===
        # 1. Descrição MANUAL existente?
        if col_name and obj_name in combined_schema:
            try:
                # Tenta encontrar a coluna no combined_schema. A estrutura pode variar.
                # Adapte esta lógica se a estrutura do seu combined_schema for diferente.
                column_details_combined = None
                # Cenário 1: Estrutura com lista de colunas
                if 'columns' in combined_schema[obj_name] and isinstance(combined_schema[obj_name]['columns'], list):
                    for c in combined_schema[obj_name]['columns']:
                        if isinstance(c, dict) and c.get('name') == col_name:
                            column_details_combined = c
                            break
                # Cenário 2: Estrutura com mapa de colunas (se existir)
                # elif 'columns_map' in combined_schema[obj_name] and isinstance(combined_schema[obj_name]['columns_map'], dict):
                #    column_details_combined = combined_schema[obj_name]['columns_map'].get(col_name)

                if column_details_combined:
                    manual_desc = column_details_combined.get('business_description')
                    # Verifica se a descrição manual existe e não está vazia (após remover espaços)
                    if manual_desc and manual_desc.strip():
                        pbar.set_postfix_str("Pulado (manual existente)")
                        skipped_manual_count += 1
                        continue # Pula para o próximo item
            except KeyError as e:
                logger.debug(f"Chave não encontrada ao verificar desc manual para {identifier} em combined_schema: {e}")
            except Exception as e_comb:
                 logger.warning(f"Erro inesperado ao verificar desc manual para {identifier} em combined_schema: {e_comb}")

        # 2. Descrição AI existente?
        if not args.force_regenerate and identifier in initial_processed_ai_identifiers:
            pbar.set_postfix_str("Pulado (IA existente)")
            skipped_ai_count += 1
            continue # Pula para o próximo item

        # 3. Amostra vazia?
        if col_data and 'sample_values' in col_data and col_data['sample_values'] == ["BOOLEAN_SKIPPED"]:
            pbar.set_postfix_str("Pulado (amostra vazia)")
            skipped_empty_sample_count += 1
            continue # Pula para o próximo item
        # === Fim das Verificações para Pular ===

        # === Enriquecimento por Similaridade (se ativo) ===
        similar_manual_descriptions = []
        if args.enable_similarity_enrichment and faiss_index:
            # 4. Obter embedding da coluna atual
            target_embedding = column_to_embedding_map.get(current_column_key)

            if target_embedding is not None:
                try:
                    # 5. Buscar vizinhos no FAISS
                    search_start = time.perf_counter()
                    # FAISS espera um array 2D (mesmo que seja só 1 vetor)
                    distances, indices = faiss_index.search(np.array([target_embedding]), args.similarity_top_k)
                    search_end = time.perf_counter()
                    total_similarity_search_time += (search_end - search_start)

                    # 6. Processar vizinhos e buscar descrições manuais
                    context_lookup_start = time.perf_counter()
                    neighbor_indices = indices[0] # Índices dos vizinhos para a primeira (e única) query
                    for neighbor_index in neighbor_indices:
                        if neighbor_index < 0: continue # FAISS pode retornar -1 se menos de K vizinhos

                        # Mapear ID do índice FAISS para (table, column)
                        if neighbor_index < len(index_to_column_map):
                            neighbor_table, neighbor_col = index_to_column_map[neighbor_index]
                            neighbor_key = (neighbor_table, neighbor_col)

                            # Não usar a própria coluna como contexto
                            if neighbor_key == current_column_key:
                                continue

                            # Buscar descrição manual do vizinho no combined_schema
                            if neighbor_table in combined_schema:
                                neighbor_table_data = combined_schema[neighbor_table]
                                if isinstance(neighbor_table_data, dict) and 'columns' in neighbor_table_data and isinstance(neighbor_table_data['columns'], list):
                                    for c_neighbor in neighbor_table_data['columns']:
                                        if isinstance(c_neighbor, dict) and c_neighbor.get('name') == neighbor_col:
                                            manual_desc = c_neighbor.get('business_description')
                                            if manual_desc and manual_desc.strip():
                                                similar_manual_descriptions.append(f"- {neighbor_table}.{neighbor_col}: {manual_desc.strip()}")
                                                break # Encontrou a coluna vizinha
                            else:
                                logger.debug(f"Tabela vizinha {neighbor_table} não encontrada no combined_schema durante lookup.")
                        else:
                            logger.warning(f"Índice FAISS {neighbor_index} fora do alcance do mapeamento ({len(index_to_column_map)}). Pulando vizinho.")
                    context_lookup_end = time.perf_counter()
                    total_context_lookup_time += (context_lookup_end - context_lookup_start)

                    if similar_manual_descriptions:
                         enriched_prompt_count += 1
                         logger.debug(f"Enriquecendo prompt para {identifier} com {len(similar_manual_descriptions)} descrições similares.")

                except Exception as e_faiss:
                    logger.error(f"Erro durante busca FAISS ou lookup para {identifier}: {e_faiss}", exc_info=True)
            else:
                # Coluna atual não tem embedding, não pode buscar similares
                skipped_no_embedding_count += 1
                logger.debug(f"Coluna {identifier} não possui embedding. Pulando busca por similaridade.")
        # === Fim do Enriquecimento ===

        # 7. Se não pulou, GERA a descrição
        pbar.set_postfix_str(f"Processando {identifier}")

        # Medir tempo de construção do prompt
        prompt_build_start = time.perf_counter()
        # Passar descrições similares para build_prompt
        prompt = build_prompt(obj_type, obj_name, col_data, similar_descriptions=similar_manual_descriptions)
        prompt_build_end = time.perf_counter()
        total_prompt_build_time += (prompt_build_end - prompt_build_start)

        # Medir tempo de geração (inferência)
        generation_start = time.perf_counter()
        generated_desc = generate_description_with_adapter(prompt, model, tokenizer)
        generation_end = time.perf_counter()

        if generated_desc:
            total_generation_time += (generation_end - generation_start)
            generated_count += 1
            pbar.set_postfix_str(f"OK: {identifier}")
            # Montar resultado
            tech_context = None
            if col_data:
                tech_context = {
                    "type": col_data.get('type'),
                    "nullable": col_data.get('nullable'),
                    "default": col_data.get('default_value'),
                    "samples": col_data.get('sample_values'),
                    "tech_description": col_data.get('description')
                }

            result_item = {
                "object_type": obj_type,
                "object_name": obj_name,
                "column_name": col_name,
                "technical_context": tech_context,
                "generated_description": generated_desc,
                "model_used": os.path.basename(args.adapter) or args.base_model, # Nome curto do adaptador
                "generation_timestamp": datetime.utcnow().isoformat() + "Z"
            }
            results_list.append(result_item)
        else:
            error_count += 1
            pbar.set_postfix_str(f"ERRO: {identifier}")
            logger.error(f"Falha ao gerar descrição para {identifier}")

    pbar.close()
    loop_end_time = time.perf_counter()
    logger.info(f"--- Loop de Geração Concluído em: {loop_end_time - loop_start_time:.2f}s ---")

    # Log de performance do loop
    logger.info(f"Tempo total busca por similaridade (FAISS): {total_similarity_search_time:.2f}s")
    if enriched_prompt_count > 0:
         logger.info(f"  -> Tempo médio por busca: {total_similarity_search_time / enriched_prompt_count:.4f}s")
    logger.info(f"Tempo total lookup de contexto similar: {total_context_lookup_time:.2f}s")
    if enriched_prompt_count > 0:
        logger.info(f"  -> Tempo médio por lookup: {total_context_lookup_time / enriched_prompt_count:.4f}s")
    logger.info(f"Tempo total construindo prompts: {total_prompt_build_time:.2f}s")
    if generated_count > 0:
        logger.info(f"  -> Tempo médio por item: {total_prompt_build_time / generated_count:.4f}s")
    logger.info(f"Tempo total gerando descrições (inferência): {total_generation_time:.2f}s")
    if generated_count > 0:
        logger.info(f"  -> Tempo médio por item: {total_generation_time / generated_count:.4f}s")

    logger.info(f"--- Resumo da Execução ---")
    logger.info(f"Novas descrições geradas: {generated_count}")
    logger.info(f"Prompts enriquecidos com similaridade: {enriched_prompt_count}")
    logger.info(f"Itens pulados (objetos com 0 linhas): {skipped_zero_rows_objects_count}")
    logger.info(f"Itens pulados (desc. manual existente): {skipped_manual_count}")
    logger.info(f"Itens pulados (desc. IA existente): {skipped_ai_count}")
    logger.info(f"Itens pulados (amostra vazia): {skipped_empty_sample_count}")
    logger.info(f"Itens pulados (sem embedding): {skipped_no_embedding_count}")
    logger.info(f"Erros durante a geração: {error_count}")
    logger.info(f"Total de descrições no arquivo final: {len(results_list)}")

    # Salvar resultados
    save_start = time.perf_counter()
    logger.info(f"Salvando resultados em {args.output}...")
    try:
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results_list, f, indent=2, ensure_ascii=False)
        save_end = time.perf_counter()
        logger.info(f"Resultados salvos com sucesso em: {save_end - save_start:.2f}s")
    except Exception as e:
        logger.error(f"Erro ao salvar o arquivo JSON de saída: {e}", exc_info=True)

    script_end_time = time.perf_counter()
    logger.info(f"--- Tempo Total de Execução do Script: {script_end_time - script_start_time:.2f}s ---")

if __name__ == "__main__":
    main() 