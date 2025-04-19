import json
from pathlib import Path
from collections import OrderedDict

ORIGINAL_METADATA_PATH = Path("data/metadata/schema_metadata.json")
NEW_SCHEMA_PATH = Path("data/metadata/generated_schema_structure.json")
DEFAULT_STRING = ""
DEFAULT_TYPE = None
DEFAULT_BOOL = False
DEFAULT_FK_REFS = None

def get_safe_value(data_dict, key, default=DEFAULT_STRING):
    """Safely get a value from a dictionary, returning default if key is missing or value is None."""
    if not isinstance(data_dict, dict):
        return default
    value = data_dict.get(key, default)
    return value if value is not None else default

def generate_new_schema():
    """
    Reads the original schema_metadata.json, transforms it into a new structured format,
    and saves it to generated_schema_structure.json.
    """
    if not ORIGINAL_METADATA_PATH.exists():
        print(f"Erro: Arquivo de metadados original não encontrado em {ORIGINAL_METADATA_PATH}")
        return

    try:
        with open(ORIGINAL_METADATA_PATH, 'r', encoding='utf-8') as f:
            original_metadata = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON de {ORIGINAL_METADATA_PATH}: {e}")
        return
    except Exception as e:
        print(f"Erro ao ler {ORIGINAL_METADATA_PATH}: {e}")
        return

    print("Iniciando a geração do novo schema estruturado...")

    final_schema = {}

    # 1. Process _GLOBAL_CONTEXT (sort keys)
    global_context_data = original_metadata.get("_GLOBAL_CONTEXT") # Use .get for safety
    if isinstance(global_context_data, dict):
        final_schema["_GLOBAL_CONTEXT"] = OrderedDict(sorted(global_context_data.items()))
        print("  - _GLOBAL_CONTEXT processado e ordenado.")
    elif global_context_data is not None: # It exists but is not a dict
        final_schema["_GLOBAL_CONTEXT"] = OrderedDict() # Assign empty ordered dict
        print(f"  - Aviso: _GLOBAL_CONTEXT foi encontrado, mas não é um dicionário (tipo: {type(global_context_data)}). Definindo como dicionário vazio no novo schema.")
    else:
        final_schema["_GLOBAL_CONTEXT"] = OrderedDict() # Assign empty ordered dict
        print("  - Aviso: _GLOBAL_CONTEXT não encontrado no arquivo original. Definindo como dicionário vazio no novo schema.")

    final_schema["schema_objects"] = []
    processed_count = 0

    # 2. Iterate through TABLES and VIEWS
    for object_type_key in ["TABLES", "VIEWS"]:
        if object_type_key in original_metadata and isinstance(original_metadata[object_type_key], dict):
            print(f"  - Processando {object_type_key}...")
            object_type = object_type_key[:-1] # "TABLES" -> "TABLE", "VIEWS" -> "VIEW"

            for name, original_item_data in original_metadata[object_type_key].items():
                if not isinstance(original_item_data, dict):
                    print(f"    - Aviso: Entrada para {object_type} '{name}' não é um dicionário. Pulando.")
                    continue

                # Create the base structure for the table/view object
                new_item_object = OrderedDict([
                    ("name", name),
                    ("type", object_type),
                    ("business_description", get_safe_value(original_item_data, "business_description")),
                    ("value_mapping_notes", DEFAULT_STRING), # Not present at table level in original
                    ("source_description", get_safe_value(original_item_data, "source_description")),
                    ("text_for_embedding", DEFAULT_STRING), # New field
                    ("columns", [])
                ])

                # Process columns
                original_columns_data = original_item_data.get("COLUMNS")
                if isinstance(original_columns_data, dict):
                    for col_name, original_col_data in original_columns_data.items():
                        # original_col_data might be {} or a dict with data

                        new_column_object = OrderedDict([
                            ("name", col_name),
                            ("type", DEFAULT_TYPE), # Not in original
                            ("is_pk", DEFAULT_BOOL), # Not in original
                            ("is_fk", DEFAULT_BOOL), # Not in original
                            ("fk_references", DEFAULT_FK_REFS), # Not in original
                            ("business_description", get_safe_value(original_col_data, "business_description")),
                            ("value_mapping_notes", get_safe_value(original_col_data, "value_mapping_notes")),
                            ("source_description", get_safe_value(original_col_data, "source_description")),
                            ("text_for_embedding", DEFAULT_STRING) # New field
                        ])
                        new_item_object["columns"].append(new_column_object)
                else:
                     print(f"    - Aviso: 'COLUMNS' não é um dicionário válido para {object_type} '{name}'. Nenhuma coluna será adicionada.")


                final_schema["schema_objects"].append(new_item_object)
                processed_count += 1
                print(f"    - {object_type} '{name}' processado.")
        else:
            print(f"  - Aviso: Chave '{object_type_key}' não encontrada ou não é um dicionário no arquivo original.")

    print(f"Total de {processed_count} tabelas/views processadas.")

    # 3. Save the new schema
    try:
        with open(NEW_SCHEMA_PATH, 'w', encoding='utf-8') as f:
            # Use default=str for OrderedDict serialization if needed, though should work directly
            json.dump(final_schema, f, ensure_ascii=False, indent=4)
        print(f"Novo schema estruturado salvo com sucesso em: {NEW_SCHEMA_PATH}")
    except Exception as e:
        print(f"Erro ao salvar o novo schema em {NEW_SCHEMA_PATH}: {e}")

if __name__ == "__main__":
    generate_new_schema() 