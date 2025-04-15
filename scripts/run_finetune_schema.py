# scripts/run_finetune_schema.py
# (Cópia de run_finetune.py, modificada para treinar com dados de schema)
import torch
# ... (restante das importações iguais a run_finetune.py) ...
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
import os
import sys

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
base_model_name = "meta-llama/Meta-Llama-3-8B-Instruct"

# MODIFICADO: Usar o dataset de schema gerado
dataset_file = "data/schema_dataset.jsonl" 

# MODIFICADO: Novo diretório de saída para o adaptador de schema
adapter_output_dir = "./results-llama3-8b-chat-schema-adapter"

# --- Configurações QLoRA, LoRA, Treinamento, SFTTrainer (IGUAIS A run_finetune.py) ---
# (Mantemos as mesmas configurações de VRAM otimizadas por enquanto)

# Configurações QLoRA
use_4bit = True
bnb_4bit_compute_dtype = "float16"
bnb_4bit_quant_type = "nf4"
use_nested_quant = False

# Configurações LoRA
lora_r = 8
lora_alpha = 16
lora_dropout = 0.05

# Configurações de Treinamento
num_train_epochs = 1 # Pode precisar ajustar para dataset de schema
per_device_train_batch_size = 1
gradient_accumulation_steps = 16 
optim = "adamw_torch"
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
max_seq_length = 512 # Manter baixo por enquanto
packing = False

# --- Script (Lógica principal IGUAL A run_finetune.py) ---

# Verifica disponibilidade da GPU
device_map = {"": 0}
if not torch.cuda.is_available():
    logging.error("ERRO: CUDA não está disponível. O fine-tuning com QLoRA requer uma GPU NVIDIA.")
    sys.exit(1)
logging.info(f"CUDA disponível. Usando GPU: {torch.cuda.get_device_name(0)}")

# 1. Carregar Dataset
# dataset_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), dataset_file)
# Simplifica caminho assumindo execução da raiz do projeto
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
hf_cache_dir = os.path.expanduser("~/.cache/huggingface/hub") 
try:
    model = AutoModelForCausalLM.from_pretrained(
        base_model_name,
        quantization_config=bnb_config,
        device_map=device_map,
        cache_dir=hf_cache_dir,
        token=os.getenv("HF_TOKEN")
    )
    model.config.use_cache = False
    model.config.pretraining_tp = 1
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
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token
        logging.info("Tokenizer pad_token definido como eos_token.")
    tokenizer.padding_side = "right"
    logging.info("Tokenizer carregado.")
except Exception as e:
    logging.exception(f"Erro ao carregar tokenizer: {e}")
    sys.exit(1)

# 5. Configurar PEFT (LoRA)
target_modules = []
for name, module in model.named_modules():
    if isinstance(module, torch.nn.Linear) and "lm_head" not in name:
         if hasattr(module, 'weight') and hasattr(module.weight, 'quant_state'): 
              target_modules.append(name.split('.')[-1])
target_modules = sorted(list(set(target_modules + ["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"])))

peft_config = LoraConfig(
    lora_alpha=lora_alpha,
    lora_dropout=lora_dropout,
    r=lora_r,
    bias="none",
    task_type="CAUSAL_LM",
    target_modules=target_modules
)
logging.info(f"Configuração PEFT (LoRA) criada. Target modules: {target_modules}")

# 6. Configurar Argumentos de Treinamento
# output_dir_full = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), adapter_output_dir)
# Simplifica caminho
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
    fp16=fp16,
    bf16=bf16,
    torch_compile=torch_compile,
    max_grad_norm=max_grad_norm,
    max_steps=max_steps,
    warmup_ratio=warmup_ratio,
    group_by_length=group_by_length,
    lr_scheduler_type=lr_scheduler_type,
    report_to="none",
    gradient_checkpointing=True,
    gradient_checkpointing_kwargs={"use_reentrant": False}
)
logging.info("Argumentos de treinamento configurados (com Gradient Checkpointing ATIVO).")

# 7. Instanciar o SFTTrainer
try:
    trainer = SFTTrainer(
        model=model,
        train_dataset=dataset,
        peft_config=peft_config,
        # dataset_text_field="text", # Não necessário se usar formato messages
        # max_seq_length=max_seq_length, # REMOVIDO: Argumento não mais aceito diretamente
        # tokenizer=tokenizer, # REMOVIDO: Argumento não mais aceito diretamente com PEFT?
        args=training_arguments,
        # packing=packing, # REMOVIDO: Argumento não mais aceito diretamente?
    )
    logging.info("SFTTrainer instanciado.")
except Exception as e:
    logging.exception(f"Erro ao instanciar SFTTrainer: {e}")
    sys.exit(1)

# 8. Iniciar Treinamento
logging.info("--- Iniciando Fine-tuning do SCHEMA --- ")
try:
    train_result = trainer.train()
    logging.info("--- Treinamento do SCHEMA Concluído --- ")

    # 9. Salvar Métricas e Modelo (Adaptador LoRA)
    metrics = train_result.metrics
    trainer.log_metrics("train", metrics)
    trainer.save_metrics("train", metrics)

    logging.info(f"Salvando adaptador LoRA (schema) treinado em '{output_dir_full}'...")
    trainer.save_model(output_dir_full)
    logging.info("Adaptador LoRA (schema) salvo.")

except torch.cuda.OutOfMemoryError:
    logging.error("-----------------------------------------------------")
    logging.error("ERRO FATAL: CUDA Out Of Memory!")
    logging.error("Sua GPU não tem VRAM suficiente com as configurações atuais.")
    # ... (mensagens de erro de memória iguais) ...
    logging.error("-----------------------------------------------------")
    sys.exit(1)
except Exception as e:
    logging.exception(f"Erro inesperado durante o treinamento do schema: {e}")
    sys.exit(1)

logging.info("--- Teste Rápido Pós-Treinamento do Schema (Opcional) ---")
# Opcional: Carregar o adaptador e testar
try:
    prompt = "Descreva a estrutura da tabela TABELA_CIDADE." # Use uma tabela real do seu dataset
    logging.info(f"Gerando resposta de teste para: {prompt}")
    
    # Formatar prompt para o modelo instruct
    messages = [{"role": "user", "content": prompt}]
    formatted_prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    
    pipe = pipeline(task="text-generation", model=model, tokenizer=tokenizer, max_length=200)
    result = pipe(formatted_prompt)
    logging.info("Resposta do Modelo (Schema) Ajustado:")
    print(result[0]['generated_text'])
except Exception as e:
    logging.warning(f"Não foi possível executar o teste pós-treinamento: {e}", exc_info=True)

print(f"\nFine-tuning do schema concluído. Adaptador salvo em {output_dir_full}.") 