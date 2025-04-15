from src.ollama_integration.client import generate_text

def main():
    """Função principal para testar a integração com Ollama."""
    test_prompt = "Explique o que é uma LLM em uma frase."
    print(f"Enviando prompt: '{test_prompt}'")

    response = generate_text(test_prompt)

    if response:
        print("\nResposta do Ollama:")
        print(response)
    else:
        print("\nFalha ao obter resposta do Ollama.")

if __name__ == "__main__":
    main()