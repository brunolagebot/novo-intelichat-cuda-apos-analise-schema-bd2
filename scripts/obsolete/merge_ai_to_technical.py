import os
import sys
import json
import logging
import argparse
from pathlib import Path
from collections import defaultdict

# --- Adiciona o diretório raiz ao sys.path --- #
# Presume que este script está em project_root/scripts/data_preparation/
script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parents[1] # Sobe dois níveis (data_preparation -> scripts -> project_root)
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))
# --- FIM Adição ao sys.path --- #

# Importar configurações e helpers necessários
from src.core.logging_config import setup_logging
from src.utils.json_helpers import load_json, save_json # Corrigido: Usar load_json
from src.core.config import ( # Importar caminhos padrão de config.py
    TECHNICAL_SCHEMA_FILE,
    AI_DESCRIPTIONS_FILE,
    MERGED_SCHEMA_WITH_AI_FILE # Adicionar uma constante para o novo arquivo de saída
)

# Configurar logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Função Principal --- #

def merge_ai_descriptions(technical_schema_path, ai_descriptions_path, output_path):
    """
    Mescla descrições de IA em um arquivo de schema técnico existente.

    Args:
        technical_schema_path (str ou Path): Caminho para o JSON do schema técnico.
        ai_descriptions_path (str ou Path): Caminho para o JSON das descrições de IA (lista de dicts).
        output_path (str ou Path): Caminho para salvar o novo JSON com os dados mesclados.
    """
    logger.info("--- Iniciando Mesclagem de Descrições AI no Schema Técnico ---")
    logger.info(f"Schema Técnico de Entrada: {technical_schema_path}")
    logger.info(f"Descrições AI de Entrada: {ai_descriptions_path}")
    logger.info(f"Arquivo de Saída: {output_path}")

    # 1. Carregar Schema Técnico
    logger.info("Carregando schema técnico...")
    technical_schema = load_json(technical_schema_path)
    if not technical_schema or not isinstance(technical_schema, dict):
        logger.critical(f"Falha ao carregar ou schema técnico inválido em: {technical_schema_path}")
        return False
    logger.info(f"Schema técnico carregado com {len(technical_schema.get('_analysis', {}).get('fk_definitions', 0))} definições de FK (exemplo de chave).") # Log um pouco mais informativo

    # 2. Carregar Descrições AI
    logger.info("Carregando descrições AI...")
    ai_descriptions = load_json(ai_descriptions_path)
    if not ai_descriptions or not isinstance(ai_descriptions, list):
        logger.critical(f"Falha ao carregar ou formato inválido (não é lista) de descrições AI em: {ai_descriptions_path}")
        return False
    logger.info(f"{len(ai_descriptions)} descrições AI carregadas.")

    # 3. Criar um lookup para acesso rápido às descrições AI
    ai_lookup = {}
    skipped_missing_keys = 0
    for item in ai_descriptions:
        try:
            # Usar .strip() para remover espaços extras, mas manter case original
            obj_name = item.get('object_name', '').strip()
            col_name = item.get('column_name', '').strip()
            if obj_name and col_name:
                 # Chave como tupla (object_name, column_name)
                ai_lookup[(obj_name, col_name)] = item
            else:
                 logger.warning(f"Item de descrição AI pulado por falta de 'object_name' ou 'column_name': {item}")
                 skipped_missing_keys += 1
        except Exception as e:
             logger.warning(f"Erro processando item de descrição AI: {item} - {e}")
             skipped_missing_keys += 1

    if skipped_missing_keys > 0:
         logger.warning(f"{skipped_missing_keys} itens de descrição AI foram pulados devido a chaves faltando ou erro.")
    logger.info(f"Criado lookup para {len(ai_lookup)} descrições AI válidas.")

    # 4. Iterar sobre o schema técnico e mesclar
    merged_count = 0
    not_found_count = 0
    logger.info("Iniciando processo de mesclagem...")
    # IMPORTANTE: Iterar sobre uma cópia das chaves se for modificar o dict durante a iteração,
    # mas aqui estamos modificando os valores (listas de colunas), então é seguro.
    # Exceto pela chave '_analysis', que não tem colunas.
    for obj_name, obj_data in technical_schema.items():
        # Ignorar a chave especial de análise
        if obj_name == '_analysis':
            continue

        # Verificar se 'columns' existe e é uma lista
        if 'columns' not in obj_data or not isinstance(obj_data['columns'], list):
            logger.warning(f"Objeto '{obj_name}' no schema técnico não tem uma lista de 'columns'. Pulando.")
            continue

        for col_data in obj_data['columns']:
             try:
                 # Garantir que 'name' existe na coluna
                 if 'name' not in col_data:
                      logger.warning(f"Coluna sem 'name' encontrada em '{obj_name}'. Pulando.")
                      continue

                 col_name = col_data['name'].strip() # Usar nome 'stripado' para lookup
                 lookup_key = (obj_name.strip(), col_name) # Usa nome do objeto 'stripado' também

                 if lookup_key in ai_lookup:
                     ai_desc_item = ai_lookup[lookup_key]
                     # Copiar os campos relevantes
                     col_data['ai_generated_description'] = ai_desc_item.get('generated_description')
                     col_data['ai_model_used'] = ai_desc_item.get('model_used')
                     col_data['ai_generation_timestamp'] = ai_desc_item.get('generation_timestamp')
                     merged_count += 1
                     # Remover a entrada do lookup para contar os não encontrados depois
                     # del ai_lookup[lookup_key] # Não remover, pode haver duplicatas no schema técnico? Melhor não.
                 else:
                      # Se não achou, garante que os campos AI estejam como None (já devem estar, mas por segurança)
                      col_data['ai_generated_description'] = None
                      col_data['ai_model_used'] = None
                      col_data['ai_generation_timestamp'] = None
                      # Contaremos os não encontrados comparando len(ai_lookup) com merged_count ao final
             except Exception as e:
                  logger.error(f"Erro ao processar coluna '{obj_name}.{col_data.get('name', 'N/A')}': {e}", exc_info=True)

    # Calcular não encontrados (descrições AI que não acharam correspondência no schema técnico)
    # Isso pode acontecer se o schema técnico for mais antigo ou incompleto
    not_found_count = len(ai_lookup) - merged_count # Aproximação se não houver duplicatas no AI_Descriptions
    # Uma contagem mais precisa seria verificar quais chaves do ai_lookup não foram usadas.

    logger.info(f"Mesclagem concluída. {merged_count} descrições AI foram incorporadas.")
    if merged_count < len(ai_lookup):
         logger.warning(f"Atenção: {len(ai_lookup) - merged_count} descrições AI não encontraram correspondência no schema técnico.")

    # 5. Salvar o novo schema mesclado
    logger.info(f"Salvando schema mesclado em {output_path}...")
    success = save_json(technical_schema, output_path) # save_json já cria o diretório
    if success:
        logger.info("Schema mesclado salvo com sucesso.")
    else:
        logger.error("Falha ao salvar o schema mesclado.")
        return False

    logger.info("--- Mesclagem Concluída ---")
    return True

# --- Ponto de Entrada do Script --- #

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mescla descrições geradas por IA em um arquivo JSON de schema técnico existente.")

    # Usar os caminhos padrão definidos em config.py
    parser.add_argument(
        "--technical-schema",
        default=TECHNICAL_SCHEMA_FILE,
        help=f"Caminho para o arquivo JSON do schema técnico. Padrão: {TECHNICAL_SCHEMA_FILE}"
    )
    parser.add_argument(
        "--ai-descriptions",
        default=AI_DESCRIPTIONS_FILE,
        help=f"Caminho para o arquivo JSON das descrições AI (lista). Padrão: {AI_DESCRIPTIONS_FILE}"
    )
    parser.add_argument(
        "-o", "--output",
        default=MERGED_SCHEMA_WITH_AI_FILE,
        help=f"Caminho para o novo arquivo JSON de saída mesclado. Padrão: {MERGED_SCHEMA_WITH_AI_FILE}"
    )

    args = parser.parse_args()

    # Executa a função principal
    merge_ai_descriptions(
        technical_schema_path=args.technical_schema,
        ai_descriptions_path=args.ai_descriptions,
        output_path=args.output
    ) 