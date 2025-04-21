#!/usr/bin/env python3
"""
Script provisório: lista todos os nomes de campo (keys) únicos em um JSON de metadados.
Uso: python scripts/list_metadata_field_names.py
"""
import os
import sys
import json

def collect_keys(obj, keys_set):
    """Coleta recursivamente todas as chaves em dicts e listas."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            keys_set.add(k)
            collect_keys(v, keys_set)
    elif isinstance(obj, list):
        for item in obj:
            collect_keys(item, keys_set)


def main():
    file_path = os.path.join('data', 'metadata', 'manual', 'manual_metadata_master.json')
    if not os.path.isfile(file_path):
        print(f"Arquivo não encontrado: {file_path}", file=sys.stderr)
        sys.exit(1)

    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    keys_set = set()
    collect_keys(data, keys_set)

    print("Campos únicos encontrados no JSON de metadados:")
    for key in sorted(keys_set):
        print(f"- {key}")

if __name__ == '__main__':
    main() 