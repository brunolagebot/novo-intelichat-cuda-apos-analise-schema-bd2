import os
import sys
import subprocess
import platform

def create_venv():
    """Cria um ambiente virtual se não existir."""
    if not os.path.exists('.venv'):
        print("Criando ambiente virtual...")
        try:
            subprocess.check_call([sys.executable, '-m', 'venv', '.venv'])
            print("Ambiente virtual criado com sucesso!")
        except subprocess.CalledProcessError as e:
            print(f"Erro ao criar ambiente virtual: {e}")
            sys.exit(1)
    else:
        print("Ambiente virtual já existe.")

def get_venv_python():
    """Retorna o caminho do executável Python do venv."""
    if platform.system() == "Windows":
        python_path = os.path.join('.venv', 'Scripts', 'python.exe')
    else:
        python_path = os.path.join('.venv', 'bin', 'python')
    return python_path

def install_dependencies():
    """Instala as dependências no ambiente virtual."""
    python_path = get_venv_python()
    if not os.path.exists(python_path):
        print(f"ERRO: Python não encontrado em {python_path}")
        sys.exit(1)

    print("Instalando dependências...")
    try:
        # Atualiza pip primeiro
        subprocess.check_call([python_path, '-m', 'pip', 'install', '--upgrade', 'pip'])
        # Instala as dependências
        subprocess.check_call([python_path, '-m', 'pip', 'install', '-r', 'requirements.txt'])
        print("Dependências instaladas com sucesso!")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao instalar dependências: {e}")
        sys.exit(1)

def main():
    """Função principal para configurar o ambiente."""
    print("=== Configurando Ambiente de Desenvolvimento ===")
    
    # Verifica se já está em um ambiente virtual
    in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    if in_venv:
        print("AVISO: Você já está em um ambiente virtual.")
        print("Para continuar, saia do ambiente virtual atual e execute este script novamente.")
        sys.exit(1)

    create_venv()
    install_dependencies()
    
    print("\n=== Ambiente Configurado com Sucesso! ===")
    print("\nPara ativar o ambiente virtual:")
    if platform.system() == "Windows":
        print("    .venv\\Scripts\\activate")
    else:
        print("    source .venv/bin/activate")
    
    print("\nDepois de ativar, execute a aplicação com:")
    print("    python app.py")

if __name__ == "__main__":
    main() 