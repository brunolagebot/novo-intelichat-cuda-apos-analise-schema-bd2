import json
import os
import shutil
from pathlib import Path

METADATA_FILE_PATH = Path("data/metadata/schema_metadata.json")
BACKUP_FILE_PATH = Path(f"{METADATA_FILE_PATH}.bak")
REQUIRED_COLUMN_KEYS = {"business_description", "value_mapping_notes", "source_description"}
SOURCE_NOTES_KEY = "source_notes" # Define the key to merge and remove
MERGE_PREFIX = "[Merged from source_notes: "
MERGE_SUFFIX = "]"
MERGE_SEPARATOR = " | "

print(f"DEBUG: Required keys representation: {[repr(k) for k in REQUIRED_COLUMN_KEYS]}")

def standardize_metadata_keys():
    """
    Ensures all column definitions in schema_metadata.json contain the required keys,
    adding missing keys with an empty string value without overwriting existing ones.
    Includes specific handling for empty column dictionaries {}.
    Merges content from 'source_notes' into 'source_description' and removes 'source_notes'.
    Creates a backup of the original file.
    """
    if not METADATA_FILE_PATH.exists():
        print(f"Erro: Arquivo de metadados não encontrado em {METADATA_FILE_PATH}")
        return

    # Create backup
    try:
        shutil.copy2(METADATA_FILE_PATH, BACKUP_FILE_PATH)
        print(f"Backup do arquivo original criado em: {BACKUP_FILE_PATH}")
    except Exception as e:
        print(f"Erro ao criar backup: {e}")
        return

    try:
        with open(METADATA_FILE_PATH, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON de {METADATA_FILE_PATH}: {e}")
        # Restore from backup if decode fails
        try:
            print(f"Tentando restaurar backup de {BACKUP_FILE_PATH}...")
            shutil.move(BACKUP_FILE_PATH, METADATA_FILE_PATH)
            print("Arquivo original restaurado do backup devido a erro de JSON.")
        except Exception as restore_e:
             print(f"Erro crítico: Falha ao decodificar JSON e ao restaurar backup: {restore_e}")
        return
    except Exception as e:
        print(f"Erro ao ler {METADATA_FILE_PATH}: {e}")
        return

    modified = False
    print("--- Iniciando Iteração (Padronização, Mesclagem, Verificação Explícita {}) ---")
    # Iterate through top-level keys (e.g., 'TABLES', 'VIEWS')
    for top_level_key, top_level_data in metadata.items():
        print(f"  DEBUG: Processando chave de nível superior (repr): {repr(top_level_key)}")
        # print(f"  Processando chave de nível superior: {top_level_key}")
        if top_level_key in ["TABLES", "VIEWS"] and isinstance(top_level_data, dict):
            # print(f"    Entrando em {top_level_key}...")
            # Iterate through items (table/view names and their data)
            for item_name, item_data in top_level_data.items():
                print(f"      DEBUG: Processando item (Tabela/View): {item_name}") # DEBUG 1
                # print(f"      Processando item: {item_name}")
                if isinstance(item_data, dict) and "COLUMNS" in item_data: # Check for uppercase COLUMNS
                    columns_data = item_data["COLUMNS"]
                    print(f"        DEBUG: Acessado COLUMNS para {item_name}") # DEBUG 2
                    if isinstance(columns_data, dict):
                        # Iterate through columns
                        for col_name, col_data in columns_data.items():
                            print(f"          DEBUG: Processando coluna: {item_name}.{col_name}") # DEBUG 3
                            # print(f"      Processando item: {item_name}")
                            if isinstance(col_data, dict):
                                # FORÇAR PADRONIZAÇÃO PARA DICIONÁRIO VAZIO
                                if len(col_data) == 0: # Verificação explícita de tamanho
                                     print(f"!!! DETECTADO DICIONÁRIO VAZIO: Para {top_level_key}.{item_name}.{col_name}. Forçando adição de chaves.")
                                     for key in REQUIRED_COLUMN_KEYS:
                                         print(f"          Adicionando chave padrão '{key}'...")
                                         col_data[key] = ""
                                     modified = True
                                     # Após adicionar, o dicionário não está mais vazio, podemos pular o resto para esta coluna
                                     continue # Pula para a próxima coluna

                                # 1. Ensure required keys exist (for non-empty dicts)
                                current_keys = set(col_data.keys())
                                missing_keys = REQUIRED_COLUMN_KEYS - current_keys
                                if missing_keys:
                                    print(f"!!! DETECTADO AUSENTE (não vazio): Para {top_level_key}.{item_name}.{col_name}. Chaves ausentes: {missing_keys}. Conteúdo atual: {col_data}")
                                    for key in missing_keys:
                                        print(f"          Adicionando chave ausente '{key}'...")
                                        col_data[key] = ""
                                        modified = True
                                        current_keys.add(key) # Add to current keys for step 2

                                # 2. Check for and merge source_notes
                                # print(f"  DEBUG: Verificando '{SOURCE_NOTES_KEY}' em {top_level_key}.{item_name}.{col_name}. Chaves encontradas (repr): {[repr(k) for k in current_keys]}") # DEBUG REPR AQUI
                                if SOURCE_NOTES_KEY in current_keys:
                                    source_notes_value = col_data.get(SOURCE_NOTES_KEY, "")
                                    # Only proceed if source_notes has content
                                    if source_notes_value:
                                        print(f"  * Mesclando '{SOURCE_NOTES_KEY}' em '{REQUIRED_COLUMN_KEYS.intersection(['source_description']).pop()}' para {top_level_key}.{item_name}.{col_name}")
                                        current_source_desc = col_data.get("source_description", "")
                                        merged_note = f"{MERGE_PREFIX}{source_notes_value}{MERGE_SUFFIX}"

                                        if current_source_desc:
                                            col_data["source_description"] = f"{current_source_desc}{MERGE_SEPARATOR}{merged_note}"
                                        else:
                                            col_data["source_description"] = merged_note

                                        del col_data[SOURCE_NOTES_KEY]
                                        modified = True
                                    else:
                                        # If source_notes exists but is empty, just remove it
                                        print(f"  * Removendo '{SOURCE_NOTES_KEY}' vazia de {top_level_key}.{item_name}.{col_name}")
                                        del col_data[SOURCE_NOTES_KEY]
                                        modified = True

                            else:
                                print(f"          Aviso: Dados da coluna '{col_name}' em '{item_name}' não são um dicionário. Pulando coluna.")
                    else:
                         print(f"      Aviso: Valor para 'COLUMNS' em '{item_name}' não é um dicionário. Pulando colunas.")
                # else:
                    # Optional: Add warning if item_data is not a dict or COLUMNS key is missing
                    # print(f"      Aviso: Item '{item_name}' não é um dicionário ou não contém 'COLUMNS'.")
        else:
            print(f"    Ignorando chave '{top_level_key}' (não é TABLES/VIEWS ou não é dicionário).")

    print("--- Fim da Iteração ---")

    if modified:
        try:
            with open(METADATA_FILE_PATH, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            print(f"Arquivo {METADATA_FILE_PATH} padronizado e mesclado com sucesso.")
        except Exception as e:
            print(f"Erro ao salvar o arquivo modificado {METADATA_FILE_PATH}: {e}")
            # Attempt to restore from backup if saving fails
            try:
                print(f"Tentando restaurar backup de {BACKUP_FILE_PATH}...")
                shutil.move(BACKUP_FILE_PATH, METADATA_FILE_PATH)
                print("Arquivo original restaurado do backup.")
            except Exception as restore_e:
                print(f"Erro crítico: Falha ao salvar modificações e ao restaurar backup: {restore_e}")
    else:
        print(f"Nenhuma modificação ou mesclagem necessária em {METADATA_FILE_PATH}.")
        # Optionally remove backup if no changes were made
        # BACKUP_FILE_PATH.unlink(missing_ok=True)

if __name__ == "__main__":
    standardize_metadata_keys() 