import os
import sys
import time

print(f"--- Executando test_import.py ---")
print(f"Hora atual: {time.time()}")
print(f"Diretório de trabalho atual (antes da modificação): {os.getcwd()}")

# --- Adiciona o diretório raiz ao sys.path --- #
script_dir = os.path.dirname(os.path.abspath(__file__))
print(f"Diretório do script (__file__): {script_dir}")
# Assumindo que 'scripts' está diretamente dentro do root do projeto
project_root = os.path.dirname(script_dir)
print(f"Diretório raiz do projeto calculado: {project_root}")

if project_root not in sys.path:
    print(f"Adicionando {project_root} ao sys.path")
    sys.path.insert(0, project_root)
else:
    print(f"{project_root} já está no sys.path")

print("--- sys.path atualizado ---")
print(sys.path)
print("--------------------------")

print("Tentando importar 'src.core.logging_config'...")
try:
    from src.core.logging_config import setup_logging
    print("Importação de 'src.core.logging_config' bem-sucedida!")
    # Tentar chamar a função importada para garantir que não há erro na carga
    print("Tentando chamar setup_logging()...")
    setup_logging() # Chama a função para teste
    print("Chamada de setup_logging() bem-sucedida!")

except ModuleNotFoundError as e:
    print(f"ERRO: ModuleNotFoundError ao importar 'src.core.logging_config': {e}")
except ImportError as e:
    print(f"ERRO: ImportError ao importar 'src.core.logging_config': {e}")
except Exception as e:
    print(f"ERRO inesperado durante a importação ou chamada: {e}")

print("--- Fim da execução de test_import.py ---") 