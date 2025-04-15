# scripts/run_finetune.py
import torch
import os
import sys
from datasets import load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    BitsAndBytesConfig,
    TrainingArguments,
    pipeline,
    logging as hf_logging, # Renomeia para evitar conflito com logging padrão
)
from peft import LoraConfig, PeftModel, get_peft_model
from trl import SFTTrainer
import logging # Usa o logging padrão

# Configuração do logging padrão
log_level_name = os.getenv('LOG_LEVEL', 'INFO').upper()
log_level = logging.getLevelName(log_level_name)
if not isinstance(log_level, int):
    log_level = logging.INFO
logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')

# Aumenta a verbosidade do log do transformers
hf_logging.set_verbosity_debug()
logging.info("Verbosity do log Transformers definida para DEBUG.")

# --- Configurações ---
# Modelo base do Ollama (certifique-se que ele foi baixado: ollama pull llama3)
# A biblioteca transformers pode precisar de um nome compatível do Hugging Face Hub.
# Para llama3:8b, um nome comum é "meta-llama/Meta-Llama-3-8B-Instruct"
# Verifique se este nome corresponde ao modelo que você quer usar.
# Se você baixou um GGUF direto, o processo pode ser diferente.
# Vamos assumir que você quer ajustar a versão Instruct do Hub.
base_model_name = "meta-llama/Meta-Llama-3-8B-Instruct"

# Arquivo de dados preparado
dataset_file = "finetune_data.jsonl" # Gerado pelo prepare_finetune_data.py

# Novo nome para o modelo ajustado (adaptador LoRA)
adapter_output_dir = "./results-llama3-8b-chat-adapter"

# Configurações QLoRA (para reduzir uso de VRAM)
use_4bit = True # Ativar quantização de 4 bits
bnb_4bit_compute_dtype = "float16" # Tipo de computação para camadas quantizadas
bnb_4bit_quant_type = "nf4" # Tipo de quantização (nf4 recomendado)
use_nested_quant = False # Usar quantização dupla (pode economizar mais, mas experimental)

# Configurações LoRA
lora_r = 8 # Rank da matriz LoRA (REDUZIDO MAIS PARA 8GB VRAM)
lora_alpha = 16 # Alpha de escalonamento LoRA (geralmente lora_r * 2)
lora_dropout = 0.05 # Dropout para camadas LoRA

# Configurações de Treinamento (MUITO IMPORTANTE PARA 8GB VRAM!)
num_train_epochs = 1
per_device_train_batch_size = 1
# Aumenta a acumulação para reduzir VRAM
gradient_accumulation_steps = 16 
optim = "paged_adamw_32bit"
save_strategy = "epoch"
logging_steps = 10
learning_rate = 2e-4
weight_decay = 0.001
fp16 = False
bf16 = False
torch_compile = False
max_grad_norm = 0.3
max_steps = -1
warmup_ratio = 0.03
group_by_length = True
lr_scheduler_type = "cosine"

# Configurações SFTTrainer
# Reduz o comprimento da sequência drasticamente
max_seq_length = 512 
packing = False

# --- Script ---

# Verifica disponibilidade da GPU
device_map = {"": 0} # Força uso da GPU 0
if not torch.cuda.is_available():
    logging.error("ERRO: CUDA não está disponível. O fine-tuning com QLoRA requer uma GPU NVIDIA.")
    sys.exit(1)
logging.info(f"CUDA disponível. Usando GPU: {torch.cuda.get_device_name(0)}")

# 1. Carregar Dataset
dataset_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), dataset_file)
logging.info(f"Carregando dataset de '{dataset_path}'...")
# O SFTTrainer espera uma coluna chamada 'text' ou que os dados já estejam formatados
# como conversação. O formato JSONL com {"messages": [...]} é suportado diretamente.
try:
    dataset = load_dataset("json", data_files=dataset_path, split="train")
    if not dataset:
         raise ValueError("Dataset vazio ou não carregado corretamente.")
    logging.info(f"Dataset carregado: {dataset}")
    # logging.info(f"Exemplo de dado: {dataset[0]}") # Descomente para depurar formato
except Exception as e:
    logging.exception(f"Erro ao carregar dataset: {e}")
    logging.error("Verifique se o arquivo '{dataset_file}' existe na raiz e está no formato JSON Lines correto.")
    sys.exit(1)


# 2. Configurar Quantização (BitsAndBytes)
compute_dtype = getattr(torch, bnb_4bit_compute_dtype)
bnb_config = BitsAndBytesConfig(
    load_in_4bit=use_4bit,
    bnb_4bit_quant_type=bnb_4bit_quant_type,
    bnb_4bit_compute_dtype=compute_dtype,
    bnb_4bit_use_double_quant=use_nested_quant,
)
logging.info("Configuração BitsAndBytes criada.")

# 3. Carregar Modelo Base Quantizado
logging.info(f"Carregando modelo base '{base_model_name}' com quantização...")
# Define o cache dir para evitar baixar novamente se já existir em outro local
hf_cache_dir = os.path.expanduser("~/.cache/huggingface/hub") 

try:
    model = AutoModelForCausalLM.from_pretrained(
        base_model_name,
        quantization_config=bnb_config,
        device_map=device_map,
        cache_dir=hf_cache_dir,
        token=os.getenv("HF_TOKEN") # Adiciona token se necessário (modelos Llama podem exigir)
    )
    model.config.use_cache = False # Necessário para PEFT
    model.config.pretraining_tp = 1 # Tende a funcionar melhor com PEFT
    logging.info("Modelo base carregado.")
except Exception as e:
    logging.exception(f"Erro ao carregar modelo base: {e}")
    logging.error("Verifique o nome do modelo, sua conexão e se tem permissão.")
    logging.error("Para modelos Llama3, pode ser necessário fazer login com 'huggingface-cli login' ou definir a variável de ambiente HF_TOKEN.")
    sys.exit(1)

# 4. Carregar Tokenizer
logging.info(f"Carregando tokenizer para '{base_model_name}'...")
try:
    tokenizer = AutoTokenizer.from_pretrained(base_model_name, trust_remote_code=True, cache_dir=hf_cache_dir, token=os.getenv("HF_TOKEN"))
    # Llama 3 usa PAD = EOS
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
        logging.info("Tokenizer pad_token definido como eos_token.")
    tokenizer.padding_side = "right" # Necessário para evitar problemas com flash attention
    logging.info("Tokenizer carregado.")
except Exception as e:
    logging.exception(f"Erro ao carregar tokenizer: {e}")
    sys.exit(1)


# 5. Configurar PEFT (LoRA)
# Encontra todos os módulos lineares para aplicar LoRA (mais robusto)
target_modules = []
for name, module in model.named_modules():
    if isinstance(module, torch.nn.Linear) and "lm_head" not in name:
         # Verifica se é uma camada linear quantizada pelo bitsandbytes
         if hasattr(module, 'weight') and hasattr(module.weight, 'quant_state'): 
              target_modules.append(name.split('.')[-1]) # Pega o nome final do módulo
# Remove duplicatas e garante módulos comuns (verificar documentação específica do modelo se necessário)
target_modules = sorted(list(set(target_modules + ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"])))

peft_config = LoraConfig(
    lora_alpha=lora_alpha,
    lora_dropout=lora_dropout,
    r=lora_r, # Usa o valor reduzido
    bias="none",
    task_type="CAUSAL_LM",
    target_modules=target_modules
)
logging.info(f"Configuração PEFT (LoRA) criada. Target modules: {target_modules}")
# Aplica PEFT ao modelo - não precisa mais fazer get_peft_model separado, SFTTrainer faz isso
# model = get_peft_model(model, peft_config)


# 6. Configurar Argumentos de Treinamento
output_dir_full = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), adapter_output_dir)

training_arguments = TrainingArguments(
    output_dir=output_dir_full,
    num_train_epochs=num_train_epochs,
    per_device_train_batch_size=per_device_train_batch_size,
    gradient_accumulation_steps=gradient_accumulation_steps, # Usa valor aumentado
    optim=optim,
    save_strategy=save_strategy,
    logging_steps=logging_steps,
    learning_rate=learning_rate,
    weight_decay=weight_decay,
    fp16=fp16,
    bf16=bf16,
    torch_compile=torch_compile,
    max_grad_norm=max_grad_norm,
    max_steps=max_steps,
    warmup_ratio=warmup_ratio,
    group_by_length=group_by_length,
    lr_scheduler_type=lr_scheduler_type,
    report_to="none",
    # HABILITA Gradient Checkpointing para economizar VRAM!
    gradient_checkpointing=True,
    gradient_checkpointing_kwargs={"use_reentrant": False} # Recomendado para PEFT
)
logging.info("Argumentos de treinamento configurados (com Gradient Checkpointing ATIVO).")

# 7. Instanciar o SFTTrainer
# O SFTTrainer formata automaticamente os dados se a coluna for "messages" ou similar
try:
    trainer = SFTTrainer(
        model=model,
        train_dataset=dataset,
        peft_config=peft_config,
        args=training_arguments,
    )
    logging.info("SFTTrainer instanciado.")
except Exception as e:
    logging.exception(f"Erro ao instanciar SFTTrainer: {e}")
    sys.exit(1)

# 8. Iniciar Treinamento
logging.info("--- Iniciando Fine-tuning --- (-- / ! \ -- Pode demorar e usar muita VRAM! -- / ! \ --)")
try:
    train_result = trainer.train()
    logging.info("--- Treinamento Concluído --- ")

    # 9. Salvar Métricas e Modelo (Adaptador LoRA)
    metrics = train_result.metrics
    trainer.log_metrics("train", metrics)
    trainer.save_metrics("train", metrics)

    logging.info(f"Salvando adaptador LoRA treinado em '{output_dir_full}'...")
    trainer.save_model(output_dir_full) # Salva apenas o adaptador LoRA
    logging.info("Adaptador LoRA salvo.")

    # (Opcional) Salvar estado do trainer
    # trainer.save_state()

except torch.cuda.OutOfMemoryError:
    logging.error("-----------------------------------------------------")
    logging.error("ERRO FATAL: CUDA Out Of Memory!")
    logging.error("Sua GPU não tem VRAM suficiente com as configurações atuais.")
    logging.error("Tente reduzir ainda mais:")
    logging.error(f"  - max_seq_length (atualmente: {max_seq_length})")
    logging.error(f"  - lora_r (atualmente: {lora_r})")
    logging.error(f"  - per_device_train_batch_size (atualmente: {per_device_train_batch_size})")
    logging.error(f"  - Aumentar gradient_accumulation_steps (atualmente: {gradient_accumulation_steps})")
    logging.error("Considere habilitar gradient_checkpointing=True nos TrainingArguments (mais lento).")
    logging.error("Considere usar um modelo base ainda menor (ex: Phi-3 Mini)." )
    logging.error("-----------------------------------------------------")
    sys.exit(1)
except Exception as e:
    logging.exception(f"Erro inesperado durante o treinamento: {e}")
    sys.exit(1)


# --- Limpeza (Opcional) ---
# Libera memória da GPU
logging.info("Liberando recursos da GPU...")
del model
del trainer
import gc
gc.collect()
with torch.no_grad():
    torch.cuda.empty_cache()
logging.info("Recursos liberados.")

# --- Teste Rápido do Adaptador (Opcional) --- 
# Recarrega o modelo base e aplica o adaptador treinado para um teste rápido
logging.info("\n--- Testando o adaptador treinado --- (Pode falhar se houver pouca VRAM restante)")
hf_logging.set_verbosity(hf_logging.CRITICAL) # Reduz verbosidade do pipeline

# Carrega o modelo base novamente (sem quantização agora para inferência rápida, se possível)
# Se ainda tiver problemas de memória, pode precisar recarregar com quantização
try:
    logging.info("Recarregando modelo base para teste...")
    base_model_reload = AutoModelForCausalLM.from_pretrained(
       base_model_name,
       device_map=device_map,
       token=os.getenv("HF_TOKEN"),
       cache_dir=hf_cache_dir,
       torch_dtype=torch.float16 # Carrega em float16 para economizar memória
    )
    logging.info("Modelo base recarregado para teste.")

    # Carrega o adaptador LoRA
    logging.info(f"Carregando adaptador de '{output_dir_full}'...")
    model_with_adapter = PeftModel.from_pretrained(base_model_reload, output_dir_full)
    model_with_adapter = model_with_adapter.merge_and_unload() # Mescla adaptadores para inferência mais rápida
    logging.info("Adaptador LoRA carregado e mesclado no modelo base.")

    # Cria o pipeline de geração de texto
    logging.info("Criando pipeline de text-generation...")
    pipe = pipeline(task="text-generation", model=model_with_adapter, tokenizer=tokenizer, max_new_tokens=50) # Limita a resposta

    # Exemplo de prompt (use um similar aos seus dados de treino)
    if len(dataset) > 0 and 'messages' in dataset[0] and len(dataset[0]['messages']) > 0:
        test_prompt_messages = dataset[0]['messages']
        # Formata o prompt como o modelo Llama-3-Instruct espera
        formatted_prompt = tokenizer.apply_chat_template(test_prompt_messages[:-1], tokenize=False, add_generation_prompt=True)
        logging.info(f"\nPrompt de teste (baseado no primeiro dado):\n{formatted_prompt}")

        result = pipe(formatted_prompt)
        generated_text = result[0]['generated_text']
        logging.info(f"\nResposta do Modelo Ajustado:\n{generated_text}")
    else:
        logging.warning("Não foi possível gerar prompt de teste a partir do dataset.")

except ImportError as e:
     logging.warning(f"Pipeline de teste pulado devido a erro de importação: {e}")
except Exception as e:
    logging.exception(f"\nErro durante o teste do adaptador: {e}")
    logging.warning("Pode ser necessário ajustar o carregamento do modelo ou o prompt de teste.")
    logging.warning("Pode indicar falta de VRAM para carregar o modelo não quantizado.")


logging.info("\n--- Script de Fine-tuning Concluído --- ") 