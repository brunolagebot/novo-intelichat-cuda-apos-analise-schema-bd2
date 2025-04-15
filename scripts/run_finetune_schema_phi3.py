# scripts/run_finetune_schema_phi3.py
# (Cópia de run_finetune_schema.py, modificada para Phi-3 Mini)
import torch
# ... (importações) ...
from datasets import load_dataset
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    # BitsAndBytesConfig, # REMOVIDO: Não usaremos QLoRA
    TrainingArguments,
    pipeline,
    logging as hf_logging,
)
from peft import LoraConfig, PeftModel
from trl import SFTTrainer
import logging
import os
import sys

# ... (Configuração de logging igual) ...
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
hf_logging.set_verbosity_debug()
logging.info("Verbosity do log Transformers definida para DEBUG.")

# --- Configurações --- 
# MODIFICADO: Usar Phi-3 Mini
base_model_name = "microsoft/Phi-3-mini-4k-instruct"

# Dataset de schema (mesmo)
dataset_file = "data/schema_dataset.jsonl" 

# MODIFICADO: Novo diretório de saída para o adaptador Phi-3
adapter_output_dir = "./results-phi3-mini-chat-schema-adapter"

# --- Configurações QLoRA REMOVIDAS ---
# use_4bit = False
# bnb_4bit_compute_dtype = "float16"
# bnb_4bit_quant_type = "nf4"
# use_nested_quant = False

# --- Configurações LoRA (Podem ser mantidas ou ajustadas) ---
lora_r = 8
lora_alpha = 16
lora_dropout = 0.05

# --- Configurações de Treinamento ---
num_train_epochs = 1 # Geralmente suficiente
per_device_train_batch_size = 2 # MODIFICADO: Podemos aumentar um pouco com modelo menor
gradient_accumulation_steps = 8 # MODIFICADO: Podemos diminuir com batch size maior
# optim = "adamw_torch" # Otimizador padrão funciona bem
optim = "adamw_8bit" # MODIFICADO: Tentar otimizador 8bit para eficiencia (requer bitsandbytes)
save_strategy = "epoch"
logging_steps = 10
learning_rate = 2e-4
weight_decay = 0.001
fp16 = False
bf16 = torch.cuda.is_bf16_supported() # Habilitar bf16 se suportado (melhor para Ampere+)
torch_compile = False # Deixar False por simplicidade
max_grad_norm = 0.3
max_steps = -1
warmup_ratio = 0.03
group_by_length = True
lr_scheduler_type = "cosine"

# --- Configurações SFTTrainer ---
max_seq_length = 1024 # MODIFICADO: Phi-3 suporta mais, podemos aumentar um pouco
packing = False # Manter False por simplicidade

# --- dtype para o modelo ---
dtype = torch.bfloat16 if bf16 else torch.float16
logging.info(f"Usando dtype: {dtype}")

# --- Script ---

# Verifica disponibilidade da GPU
device_map = {"": 0}
if not torch.cuda.is_available():
    logging.error("ERRO: CUDA não está disponível.") # Não mencionamos QLoRA aqui
    sys.exit(1)
logging.info(f"CUDA disponível. Usando GPU: {torch.cuda.get_device_name(0)}")

# 1. Carregar Dataset (Igual)
dataset_path = dataset_file 
logging.info(f"Carregando dataset de '{dataset_path}'...")
try:
    dataset = load_dataset("json", data_files=dataset_path, split="train")
    if not dataset:
         raise ValueError("Dataset vazio ou não carregado corretamente.")
    logging.info(f"Dataset carregado: {dataset}")
except Exception as e:
    logging.exception(f"Erro ao carregar dataset: {e}")
    logging.error(f"Verifique se o arquivo '{dataset_file}' existe e está no formato JSON Lines correto.")
    sys.exit(1)

# 2. Configurar Quantização REMOVIDA

# 3. Carregar Modelo Base (sem quantização)
logging.info(f"Carregando modelo base '{base_model_name}'...")
hf_cache_dir = os.path.expanduser("~/.cache/huggingface/hub") 
try:
    model = AutoModelForCausalLM.from_pretrained(
        base_model_name,
        # quantization_config=bnb_config, # REMOVIDO
        device_map=device_map,
        trust_remote_code=True, # Necessário para Phi-3
        torch_dtype=dtype, # Usar bfloat16 ou float16
        cache_dir=hf_cache_dir,
        token=os.getenv("HF_TOKEN")
    )
    model.config.use_cache = False # Necessário para PEFT
    # model.config.pretraining_tp = 1 # Menos relevante sem quantização
    logging.info("Modelo base carregado.")
except Exception as e:
    logging.exception(f"Erro ao carregar modelo base: {e}")
    sys.exit(1)

# 4. Carregar Tokenizer (Igual, mas verificar padding do Phi-3)
logging.info(f"Carregando tokenizer para '{base_model_name}'...")
try:
    # Phi-3 pode precisar de trust_remote_code=True também
    tokenizer = AutoTokenizer.from_pretrained(base_model_name, trust_remote_code=True, cache_dir=hf_cache_dir, token=os.getenv("HF_TOKEN")) 
    if tokenizer.pad_token is None:
        logging.warning("Tokenizer não tem pad_token. Definindo como eos_token.")
        tokenizer.pad_token = tokenizer.eos_token
        # Phi-3 pode preferir padding_side='left'
    tokenizer.padding_side = "left" # MODIFICADO: Testar com 'left' para Phi-3
    logging.info(f"Tokenizer carregado com padding_side='{tokenizer.padding_side}'.")
except Exception as e:
    logging.exception(f"Erro ao carregar tokenizer: {e}")
    sys.exit(1)

# 5. Configurar PEFT (LoRA) (Igual, mas recalcular target_modules)
# Recalcular target modules pois Phi-3 tem nomes de camadas diferentes
# Abordagem comum é buscar por 'Linear' e excluir 'lm_head' ou 'output'
# Mas uma lista explícita para Phi-3 é mais segura:
# target_modules = ["qkv_proj", "o_proj", "gate_up_proj", "down_proj"]
# OU tentar encontrar dinamicamente (mais genérico):
target_modules = []
for name, module in model.named_modules():
    # Adaptar para as camadas lineares do Phi-3 (pode precisar de inspeção)
    if isinstance(module, torch.nn.Linear) and "lm_head" not in name and "output" not in name:
         target_modules.append(name.split('.')[-1])
# Uma lista comum para Phi-3:
target_modules = sorted(list(set(target_modules + ["qkv_proj", "o_proj", "gate_up_proj", "down_proj"])))
if not target_modules:
    logging.warning("Não foram encontrados módulos lineares para LoRA automaticamente. Usando lista padrão para Phi-3.")
    target_modules = ["qkv_proj", "o_proj", "gate_up_proj", "down_proj"]

peft_config = LoraConfig(
    lora_alpha=lora_alpha,
    lora_dropout=lora_dropout,
    r=lora_r,
    bias="none",
    task_type="CAUSAL_LM",
    target_modules=target_modules
)
logging.info(f"Configuração PEFT (LoRA) criada. Target modules: {target_modules}")

# 6. Configurar Argumentos de Treinamento (Sem Gradient Checkpointing)
output_dir_full = adapter_output_dir 
training_arguments = TrainingArguments(
    output_dir=output_dir_full,
    num_train_epochs=num_train_epochs,
    per_device_train_batch_size=per_device_train_batch_size,
    gradient_accumulation_steps=gradient_accumulation_steps,
    optim=optim,
    save_strategy=save_strategy,
    logging_steps=logging_steps,
    learning_rate=learning_rate,
    weight_decay=weight_decay,
    fp16=fp16, # bf16 é controlado separadamente agora
    bf16=bf16,
    torch_compile=torch_compile,
    max_grad_norm=max_grad_norm,
    max_steps=max_steps,
    warmup_ratio=warmup_ratio,
    group_by_length=group_by_length,
    lr_scheduler_type=lr_scheduler_type,
    report_to="none",
    # gradient_checkpointing=True, # REMOVIDO: Não deve ser necessário
    # gradient_checkpointing_kwargs={"use_reentrant": False}
)
logging.info("Argumentos de treinamento configurados (SEM Gradient Checkpointing).")

# 7. Instanciar o SFTTrainer (Sem args removidos anteriormente)
try:
    trainer = SFTTrainer(
        model=model,
        train_dataset=dataset,
        peft_config=peft_config,
        # dataset_text_field="text", # Não necessário se usar formato messages
        max_seq_length=max_seq_length, # Passar aqui para truncamento
        tokenizer=tokenizer,
        args=training_arguments,
        # packing=packing, # Packing pode ser útil mas manter False por simplicidade
    )
    logging.info("SFTTrainer instanciado.")
except Exception as e:
    logging.exception(f"Erro ao instanciar SFTTrainer: {e}")
    sys.exit(1)

# 8. Iniciar Treinamento (Deve ser rápido)
logging.info("--- Iniciando Fine-tuning do SCHEMA (Phi-3 Mini) --- ")
try:
    train_result = trainer.train()
    logging.info("--- Treinamento do SCHEMA (Phi-3 Mini) Concluído --- ")

    # 9. Salvar (Igual)
    metrics = train_result.metrics
    trainer.log_metrics("train", metrics)
    trainer.save_metrics("train", metrics)

    logging.info(f"Salvando adaptador LoRA (schema, Phi-3) treinado em '{output_dir_full}'...")
    trainer.save_model(output_dir_full)
    logging.info("Adaptador LoRA (schema, Phi-3) salvo.")

except torch.cuda.OutOfMemoryError:
    logging.error("-----------------------------------------------------")
    logging.error("ERRO FATAL: CUDA Out Of Memory!")
    logging.error("Mesmo com Phi-3? Tente reduzir per_device_train_batch_size ou max_seq_length.")
    logging.error("-----------------------------------------------------")
    sys.exit(1)
except Exception as e:
    logging.exception(f"Erro inesperado durante o treinamento do schema (Phi-3): {e}")
    sys.exit(1)

# Teste pós-treinamento REMOVIDO por simplicidade agora

print(f"\nFine-tuning do schema (Phi-3 Mini) concluído. Adaptador salvo em {output_dir_full}.") 