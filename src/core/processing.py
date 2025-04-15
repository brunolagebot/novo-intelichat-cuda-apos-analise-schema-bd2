import re
import logging

def preprocess_user_input(input_text: str) -> str:
    """Realiza pré-processamento básico no texto de entrada do usuário.

    - Remove espaços em branco no início e no fim.
    - Reduz múltiplos espaços entre palavras a um único espaço.

    Args:
        input_text: O texto original do usuário.

    Returns:
        O texto pré-processado.
    """
    if not isinstance(input_text, str):
        logging.warning(f"preprocess_user_input recebeu tipo não string: {type(input_text)}. Retornando como está.")
        return input_text # Retorna o input original se não for string

    logging.debug(f"Texto original para pré-processamento: '{input_text}'")
    
    # Remove espaços do início/fim
    processed_text = input_text.strip()
    
    # Substitui múltiplos espaços por um único espaço
    processed_text = re.sub(r'\s+', ' ', processed_text)
    
    logging.debug(f"Texto após pré-processamento: '{processed_text}'")
    return processed_text 