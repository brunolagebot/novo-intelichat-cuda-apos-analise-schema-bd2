import torch
import transformers
from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModel
import logging
import sys

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configurações ---
BASE_MODEL_ID = "meta-llama/Meta-Llama-3-8B-Instruct"
# Use o caminho relativo ou absoluto para o seu adaptador treinado
# ADAPTER_PATH = "./results-llama3-8b-chat-adapter" # Adaptador antigo
ADAPTER_PATH = "./results-llama3-8b-chat-schema-adapter" # MODIFICADO: Usar adaptador de schema
# Certifique-se que este caminho está correto!

# Verificar disponibilidade de bfloat16 (melhor para Ampere GPUs ou mais recentes)
dtype = torch.bfloat16 if torch.cuda.is_available() and torch.cuda.is_bf16_supported() else torch.float16
logging.info(f"Usando dtype: {dtype}")

# --- Carregamento ---
def load_model_and_tokenizer(base_model_id, adapter_path):
    """Carrega o modelo base, o adaptador LoRA e o tokenizer."""
    device = "cuda:0" if torch.cuda.is_available() else "cpu"
    logging.info(f"Usando dispositivo: {device}")
    try:
        logging.info(f"Carregando tokenizer de {base_model_id}...")
        tokenizer = AutoTokenizer.from_pretrained(base_model_id)
        # Llama 3 Instruct usa pad_token = eos_token por padrão, o que é bom.
        # Se não tivesse, seria necessário: tokenizer.pad_token = tokenizer.eos_token

        logging.info(f"Carregando modelo base {base_model_id} em {device}...")
        model = AutoModelForCausalLM.from_pretrained(
            base_model_id,
            torch_dtype=dtype,
            device_map=device # Usar o dispositivo explicitamente
        )

        logging.info(f"Carregando e aplicando adaptador LoRA de {adapter_path}...")
        # Carrega o PeftModel, que combina o base model com o adaptador
        model = PeftModel.from_pretrained(model, adapter_path)
        logging.info("Modelo base e adaptador carregados com sucesso.")

        # Coloca o modelo em modo de avaliação (desativa dropout, etc.)
        model.eval()

        return model, tokenizer

    except Exception as e:
        logging.error(f"Erro ao carregar modelo ou tokenizer: {e}", exc_info=True)
        sys.exit(1)

# --- Geração de Texto ---
def generate_response(model, tokenizer, prompt, max_new_tokens=150):
    """Formata o prompt, gera e decodifica a resposta do modelo."""
    device = model.device # Pega o dispositivo do modelo carregado
    try:
        # Formatar o prompt usando o template de chat (essencial para Instruct models)
        # Nota: Usamos um formato simples aqui. Para conversas multi-turn, a estrutura seria mais complexa.
        messages = [
            {"role": "user", "content": prompt}
        ]
        # `add_generation_prompt=True` adiciona os tokens especiais que indicam ao modelo para gerar uma resposta
        formatted_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)

        logging.debug(f"Prompt formatado: {formatted_prompt}")

        # Tokenizar o prompt formatado e mover explicitamente para a GPU
        inputs = tokenizer(formatted_prompt, return_tensors="pt").to(device)
        logging.info(f"Input tensors movidos para {inputs.input_ids.device}")

        logging.info("Gerando resposta na GPU...")
        # Gerar a resposta
        with torch.no_grad(): # Desabilita cálculo de gradientes para inferência
            outputs = model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                eos_token_id=tokenizer.eos_token_id,
                do_sample=True,
                temperature=0.6,
                top_p=0.9,
                pad_token_id=tokenizer.eos_token_id # Adicionado para evitar aviso
            )
        
        # Decodificar a resposta gerada
        # outputs[0] contém todo o texto (prompt + resposta). Selecionamos apenas a parte gerada.
        response_ids = outputs[0][inputs.input_ids.shape[1]:] 
        response = tokenizer.decode(response_ids, skip_special_tokens=True)
        logging.info("Resposta gerada.")
        logging.debug(f"Resposta decodificada: {response}")
        
        return response

    except Exception as e:
        logging.error(f"Erro durante a geração da resposta: {e}", exc_info=True)
        return "Desculpe, ocorreu um erro ao gerar a resposta."

# --- Loop Principal ---
if __name__ == "__main__":
    model, tokenizer = load_model_and_tokenizer(BASE_MODEL_ID, ADAPTER_PATH)

    print("\nModelo ajustado carregado. Digite 'sair' para terminar.")
    while True:
        try:
            user_prompt = input("Você: ")
            if user_prompt.lower() == 'sair':
                break
            if not user_prompt:
                continue

            model_response = generate_response(model, tokenizer, user_prompt)
            print(f"Modelo: {model_response}")

        except KeyboardInterrupt:
            print("\nSaindo...")
            break
        except Exception as e:
            logging.error(f"Erro no loop principal: {e}", exc_info=True)
            print("Ocorreu um erro inesperado. Verifique os logs.")

    print("Programa encerrado.") 