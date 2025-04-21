import json
import argparse
import os
import copy
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_json(file_path):
    """Loads a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Error: File not found at {file_path}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error: Could not decode JSON from {file_path}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading {file_path}: {e}")
        return None

def save_json(data, file_path):
    """Saves data to a JSON file."""
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logging.info(f"Successfully saved consolidated schema to {file_path}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving to {file_path}: {e}")

def merge_descriptions(base_schema, ai_description_list):
    """Merges AI descriptions (from a list) into a deep copy of the base schema."""
    if not base_schema or not ai_description_list:
        logging.error("Base schema or AI descriptions list is missing.")
        return None

    if not isinstance(ai_description_list, list):
        logging.error("Error: AI descriptions file is not a list as expected.")
        # Attempt to handle the previously expected dict format as a fallback
        if isinstance(ai_description_list, dict) and 'tables' in ai_description_list:
             logging.warning("AI descriptions file was a dictionary, attempting fallback merge.")
             # Re-implementing the dictionary logic briefly for fallback
             consolidated_schema_dict = copy.deepcopy(base_schema)
             ai_tables_dict = ai_description_list.get('tables', {})
             consolidated_tables_dict = consolidated_schema_dict.get('tables', {})
             fields_to_merge_dict = ["generated_description", "model_used", "generation_timestamp"]
             for table_name_dict, ai_table_data_dict in ai_tables_dict.items():
                 if table_name_dict in consolidated_tables_dict:
                     for field_dict in fields_to_merge_dict:
                         if field_dict in ai_table_data_dict:
                             consolidated_tables_dict[table_name_dict][field_dict] = ai_table_data_dict[field_dict]
                     ai_columns_dict = ai_table_data_dict.get('columns', {})
                     consolidated_columns_dict = consolidated_tables_dict[table_name_dict].get('columns', {})
                     for col_name_dict, ai_col_data_dict in ai_columns_dict.items():
                         if col_name_dict in consolidated_columns_dict:
                             for field_dict in fields_to_merge_dict:
                                 if field_dict in ai_col_data_dict:
                                     consolidated_columns_dict[col_name_dict][field_dict] = ai_col_data_dict[field_dict]
             return consolidated_schema_dict
        else:
            logging.error("AI descriptions file format is neither the expected list nor the fallback dictionary format.")
            return None


    consolidated_schema = copy.deepcopy(base_schema)
    consolidated_tables = consolidated_schema.get('tables', {})
    fields_to_merge = ["generated_description", "model_used", "generation_timestamp"]

    processed_elements = set() # To avoid processing the same table/column multiple times if list has duplicates

    for item in ai_description_list:
        if not isinstance(item, dict):
            logging.warning(f"Skipping non-dictionary item in AI descriptions list: {item}")
            continue

        table_name = item.get("table_name") or item.get("table") # Allow variations
        column_name = item.get("column_name") or item.get("column") # Allow variations

        if not table_name:
            logging.warning(f"Skipping item due to missing 'table_name' or 'table': {item}")
            continue

        element_key = f"{table_name}" if not column_name else f"{table_name}.{column_name}"
        if element_key in processed_elements:
             logging.warning(f"Skipping duplicate description for: {element_key}")
             continue

        if table_name in consolidated_tables:
            if column_name:
                # Column-level description
                consolidated_columns = consolidated_tables[table_name].get('columns', {})
                if column_name in consolidated_columns:
                    target_element = consolidated_columns[column_name]
                    for field in fields_to_merge:
                        if field in item:
                            target_element[field] = item[field]
                        elif field not in target_element: # Initialize if not present
                            target_element[field] = None
                    processed_elements.add(element_key)
                else:
                    logging.warning(f"Column '{column_name}' from AI descriptions list not found in table '{table_name}' of base schema. Item: {item}")
            else:
                # Table-level description
                target_element = consolidated_tables[table_name]
                for field in fields_to_merge:
                    if field in item:
                        target_element[field] = item[field]
                    elif field not in target_element: # Initialize if not present
                        target_element[field] = None
                processed_elements.add(element_key)
        else:
            logging.warning(f"Table '{table_name}' from AI descriptions list not found in base schema. Item: {item}")

    # Ensure all tables/columns in the consolidated schema have the fields, even if null
    for table_name, table_data in consolidated_tables.items():
        for field in fields_to_merge:
            if field not in table_data:
                table_data[field] = None
        for col_name, col_data in table_data.get('columns', {}).items():
            for field in fields_to_merge:
                if field not in col_data:
                    col_data[field] = None

    return consolidated_schema

def main():
    parser = argparse.ArgumentParser(description="Merge AI-generated descriptions into a base schema JSON.")
    parser.add_argument("base_schema_path", help="Path to the base schema JSON file (e.g., metadata.json).")
    parser.add_argument("ai_descriptions_path", help="Path to the AI-generated descriptions JSON file (expected as a list of description objects).")
    parser.add_argument("output_path", help="Path to save the consolidated schema JSON file.")

    args = parser.parse_args()

    logging.info(f"Loading base schema from: {args.base_schema_path}")
    base_schema = load_json(args.base_schema_path)

    logging.info(f"Loading AI descriptions list from: {args.ai_descriptions_path}")
    ai_descriptions = load_json(args.ai_descriptions_path) # Keep name generic

    if base_schema and ai_descriptions:
        logging.info("Merging AI descriptions into the base schema...")
        # Pass the loaded data (expected to be a list now) to the modified function
        consolidated_schema = merge_descriptions(base_schema, ai_descriptions)

        if consolidated_schema:
            logging.info(f"Saving consolidated schema to: {args.output_path}")
            save_json(consolidated_schema, args.output_path)
        else:
            logging.error("Failed to merge descriptions.")
    else:
        logging.error("Failed to load necessary JSON files. Exiting.")

if __name__ == "__main__":
    main() 