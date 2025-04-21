#!/usr/bin/env python3
"""
Script provisório: conta quantas vezes a chave 'description' aparece em cada JSON sob data/metadata.
Uso: python scripts/count_json_descriptions.py
"""
import os
import sys
import json

# Definindo as chaves que serão contabilizadas
KEYS_TO_COUNT = ['description', 'business_description', 'value_mapping_notes']

def count_key(obj, key_to_count):
    """Conta recursivamente ocorrências de uma chave específica em dicts e listas."""
    count = 0
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key_to_count:
                count += 1
            count += count_key(v, key_to_count)
    elif isinstance(obj, list):
        for item in obj:
            count += count_key(item, key_to_count)
    return count

def main():
    metadata_dir = os.path.join('data', 'metadata')
    if not os.path.isdir(metadata_dir):
        print(f"Diretório não encontrado: {metadata_dir}", file=sys.stderr)
        sys.exit(1)

    counts = {}
    for fname in os.listdir(metadata_dir):
        if fname.lower().endswith('.json'):
            path = os.path.join(metadata_dir, fname)
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                counts[fname] = {k: count_key(data, k) for k in KEYS_TO_COUNT}
            except Exception as e:
                print(f"Erro em {fname}: {e}", file=sys.stderr)

    if not counts:
        print("Nenhum JSON encontrado em data/metadata.")
        return

    # Exibir contagem de cada chave por arquivo
    print("Contagem de chaves por arquivo:")
    header = ['Arquivo'] + KEYS_TO_COUNT
    print('  '.join(header))
    for fname, cnt_map in counts.items():
        row = [fname] + [str(cnt_map.get(k, 0)) for k in KEYS_TO_COUNT]
        print('  '.join(row))

    # Identificar, para cada chave, o arquivo que possui mais ocorrências
    for k in KEYS_TO_COUNT:
        max_file = max(counts, key=lambda f: counts[f].get(k, 0))
        print(f"Arquivo com mais '{k}': {max_file} ({counts[max_file][k]})")

if __name__ == '__main__':
    main() 