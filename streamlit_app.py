import streamlit as st
import json
import os
import logging
import re # NOVO: Para limpar tipo
from collections import OrderedDict, defaultdict # NOVO: defaultdict
import datetime # NOVO: Para timestamps
import pandas as pd # NOVO: Para o DataFrame da visão geral
import fdb # NOVO: Para conectar ao Firebird
import subprocess # NOVO: Para executar o script externo
import sys # NOVO: Para obter o executável python correto
import io # NOVO: Para manipulação de bytes em memória (Excel)
import numpy as np # NOVO: Para manipulação de vetores
import faiss # NOVO: Para busca por similaridade
import copy # NOVO: Para deepcopy
import time # NOVO: Para medir o tempo
import argparse # NOVO
import uuid # NOVO: Para IDs de mensagem
from src.utils.json_helpers import load_json, save_json # NOVO: Importa funções auxiliares

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# NOVO: Tentar importar a função de chat (lidar com erro se não existir)
try:
    from src.ollama_integration.client import chat_completion
    # NOVO: Tentar importar função de embedding
    try:
        from src.ollama_integration.client import get_embedding
        OLLAMA_EMBEDDING_AVAILABLE = True
        logger.info("Função de embedding Ollama (get_embedding) carregada.")
    except ImportError:
        OLLAMA_EMBEDDING_AVAILABLE = False
        logger.warning("Função get_embedding não encontrada em src.ollama_integration.client. Busca semântica no chat desabilitada.")
        def get_embedding(text): # Define dummy
            st.error("Função de embedding Ollama não encontrada.")
            return None

    OLLAMA_AVAILABLE = True
    logger.info("Integração Ollama carregada com sucesso.")
except ImportError:
    OLLAMA_AVAILABLE = False
    logger.warning("src.ollama_integration.client não encontrado. Funcionalidades de IA estarão desabilitadas.")
    # Define uma função dummy para evitar erros NameError
    def chat_completion(messages, stream=False):
        st.error("Integração Ollama não configurada/encontrada.")
        return None
except Exception as e:
    OLLAMA_AVAILABLE = False
    logger.error(f"Erro inesperado ao importar Ollama: {e}")
    def chat_completion(messages, stream=False):
        st.error(f"Erro na integração Ollama: {e}")
        return None

METADATA_FILE = 'etapas-sem-gpu/schema_metadata.json'
TECHNICAL_SCHEMA_FILE = 'data/combined_schema_details.json' # Fallback schema without embeddings
EMBEDDED_SCHEMA_FILE = 'data/schema_with_embeddings.json' # Schema WITH embeddings
OVERVIEW_COUNTS_FILE = 'data/overview_counts.json' # NOVO: Arquivo para contagens cacheadas
# NOVO: Definir o nome do arquivo de saída do merge para usar na mensagem
OUTPUT_COMBINED_FILE = 'data/combined_schema_details.json'
CHAT_HISTORY_FILE = 'data/chat_history.json' # NOVO
CHAT_FEEDBACK_FILE = 'data/chat_feedback.json' # NOVO

# --- Configurações Padrão de Conexão (Podem ser sobrescritas na interface) ---
DEFAULT_DB_PATH = r"C:\Projetos\DADOS.FDB" # Use raw string para evitar problemas com barras invertidas
DEFAULT_DB_USER = "SYSDBA"
# !! ATENÇÃO: Senha hardcoded não é seguro para produção !!
DEFAULT_DB_CHARSET = "WIN1252"

# --- Dicionário de Explicações de Tipos SQL (pt-br) ---
TYPE_EXPLANATIONS = {
    "INTEGER": "Número inteiro (sem casas decimais).",
    "VARCHAR": "Texto de tamanho variável.",
    "CHAR": "Texto de tamanho fixo.",
    "DATE": "Data (ano, mês, dia).",
    "TIMESTAMP": "Data e hora.",
    "BLOB": "Dados binários grandes (ex: imagem, texto longo).",
    "SMALLINT": "Número inteiro pequeno.",
    "BIGINT": "Número inteiro grande.",
    "FLOAT": "Número de ponto flutuante (aproximado).",
    "DOUBLE PRECISION": "Número de ponto flutuante com maior precisão.",
    "NUMERIC": "Número decimal exato (precisão definida).",
    "DECIMAL": "Número decimal exato (precisão definida).",
    "TIME": "Hora (hora, minuto, segundo)."
}

# --- NOVO: Constantes FAISS ---
FAISS_INDEX_FILE = 'data/faiss_column_index.idx' # Opcional: Salvar/Carregar índice pré-construído
EMBEDDING_DIMENSION = 768 # Ajuste conforme a dimensão do seu modelo ('nomic-embed-text' usa 768)

def get_type_explanation(type_string):
    """Tenta encontrar uma explicação para o tipo SQL base."""
    if not type_string:
        return ""
    base_type = re.match(r"^([A-Z\s_]+)", type_string.upper())
    if base_type:
        explanation = TYPE_EXPLANATIONS.get(base_type.group(1).strip())
        return f"*{explanation}*" if explanation else ""
    return ""

# --- Funções Auxiliares --- NOVO: load_technical_schema
@st.cache_data # Cache para estrutura técnica (não muda na sessão)
def load_technical_schema(file_path):
    logger.info(f"---> EXECUTANDO load_technical_schema para: {file_path}") # Log de diagnóstico
    """Carrega o schema técnico (combinado) do arquivo JSON."""
    if not os.path.exists(file_path):
        st.error(f"Erro: Arquivo de schema técnico não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f, object_pairs_hook=OrderedDict)
            logger.info(f"Schema técnico carregado de {file_path}")
            return data
    except json.JSONDecodeError as e:
        st.error(f"Erro ao decodificar JSON do schema técnico {file_path}: {e}")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao carregar schema técnico {file_path}: {e}")
        return None


@st.cache_data # Cache para evitar recarregar a cada interação
def load_metadata(file_path):
    logger.info(f"---> EXECUTANDO load_metadata para: {file_path}") # Log de diagnóstico
    """Carrega o arquivo JSON de metadados."""
    if not os.path.exists(file_path):
        st.error(f"Erro: Arquivo de metadados não encontrado em '{file_path}'")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Usar OrderedDict para tentar manter a ordem original das chaves
            data = json.load(f, object_pairs_hook=OrderedDict)
            logger.info(f"Metadados carregados de {file_path}")
            return data
    except json.JSONDecodeError as e:
        st.error(f"Erro ao decodificar JSON do arquivo {file_path}: {e}")
        return None
    except IOError as e:
        st.error(f"Erro ao ler o arquivo {file_path}: {e}")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao carregar {file_path}: {e}")
        logger.exception(f"Erro inesperado ao carregar {file_path}")
        return None

def save_metadata(data, file_path):
    """Salva os dados (dicionário) de volta no arquivo JSON."""
    try:
        # Garante que o diretório exista
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        logger.info(f"Metadados salvos em {file_path}")
        return True
    except IOError as e:
        st.error(f"Erro ao salvar o arquivo JSON em {file_path}: {e}")
        return False
    except Exception as e:
        st.error(f"Erro inesperado ao salvar o JSON em {file_path}: {e}")
        logger.exception(f"Erro inesperado ao salvar o JSON em {file_path}")
        return False

# --- Função para gerar descrição via IA (Adaptada de view_schema_app.py) ---
def generate_ai_description(prompt):
    """Chama a API Ollama para gerar uma descrição e limpa a resposta."""
    if not OLLAMA_AVAILABLE:
        st.warning("Funcionalidade de IA não disponível.")
        return None
        
    logger.debug(f"Enviando prompt para IA: {prompt}")
    messages = [{"role": "user", "content": prompt}]
    try:
        with st.spinner("🧠 Pensando..."):
            response = chat_completion(messages=messages, stream=False)
        if response:
            cleaned_response = response.strip().strip('"').strip('\'').strip()
            logger.debug(f"Resposta da IA (limpa): {cleaned_response}")
            return cleaned_response
        else:
            logger.warning("Falha ao obter descrição da IA (resposta vazia).")
            st.toast("😕 A IA não retornou uma sugestão.")
            return None
    except Exception as e:
        logger.exception("Erro ao chamar a API Ollama:")
        st.error(f"Erro ao contatar a IA: {e}")
        return None

# --- Função find_existing_info (Adaptada e Renomeada) ---
def find_existing_info(metadata, schema_data, current_object_name, target_col_name):
    """
    Procura por informações existentes (descrição e notas) para uma coluna:
    1. Busca por nome exato em outras tabelas/views (para descrição e notas).
    2. Se for FK, busca a descrição da PK referenciada (apenas descrição).
    3. Se for PK, busca a descrição de uma coluna FK que a referencie (apenas descrição).
    4. Verifica comentário do banco de dados (apenas descrição).

    Retorna: (desc_sugerida, fonte_desc, notas_sugeridas, fonte_notas)
    """
    if not metadata or not schema_data or not target_col_name or not current_object_name:
        return None, None, None, None # Retorna None para tudo

    # --- 1. Verificar Comentário do Banco de Dados (APENAS para Descrição) ---
    current_object_info = schema_data.get(current_object_name)
    if current_object_info:
        # Encontra a info técnica da coluna alvo
        tech_col_info = None
        for col_def in current_object_info.get('columns', []):
            if col_def.get('name') == target_col_name:
                tech_col_info = col_def
                break
        # Verifica se a descrição técnica (comentário DB) existe
        if tech_col_info:
            db_comment_raw = tech_col_info.get('description') # Pode retornar None
            if db_comment_raw: # Checa se não é None ou string vazia
                db_comment = db_comment_raw.strip()
                if db_comment: # Checa se não ficou vazio após strip
                    # Pega o tipo de objeto para acessar metadados corretamente
                    obj_type = current_object_info.get('object_type', 'TABLE') # Default para TABLE se não achar
                    obj_type_key = obj_type + "S"
                    # Checa se a descrição manual JÁ está preenchida - SÓ USA COMENTÁRIO SE MANUAL VAZIA
                    manual_desc = metadata.get(obj_type_key, {}).get(current_object_name, {}).get('COLUMNS', {}).get(target_col_name, {}).get('description','').strip()
                    if not manual_desc: # Somente se a descrição manual estiver vazia
                        logger.debug(f"Heurística: Descrição encontrada via comentário do DB para {current_object_name}.{target_col_name}")
                        return db_comment, "database comment", None, None # MODIFICADO: Retorna None para notas
                    # else: Se manual_desc existe, ignora o comentário do DB e segue para outras heurísticas
    # --- FIM NOVO --- 

    # 2. Busca por nome exato (prioridade se comentário DB falhar)
    for obj_type_key in ['TABLES', 'VIEWS']:
        for obj_name, obj_meta in metadata.get(obj_type_key, {}).items():
            if obj_name == current_object_name: continue
            
            # --- DEBUGGING LOG --- #
            logger.debug(f"[find_existing_info] Checking {obj_type_key}.{obj_name}: Type={type(obj_meta)}, Value='{str(obj_meta)[:200]}...'" ) # Log type and truncated value
            # --- END DEBUGGING LOG ---

            # --- CORREÇÃO: Verificar se obj_meta é um dicionário --- 
            if not isinstance(obj_meta, dict):
                logger.warning(f"[find_existing_info] Esperava um dicionário para {obj_type_key}.{obj_name}, mas encontrou {type(obj_meta)}. Pulando este objeto.")
                continue # Pula para o próximo objeto se não for dict
            # --- FIM CORREÇÃO ---

            col_meta = obj_meta.get('COLUMNS', {}).get(target_col_name)
            # MODIFICADO: Verifica descrição E notas
            if col_meta:
                found_desc = col_meta.get('description', '').strip()
                found_notes = col_meta.get('value_mapping_notes', '').strip()
                if found_desc or found_notes: # Se encontrou algo útil
                    source = f"nome exato em `{obj_name}`"
                    logger.debug(f"Heurística: Informação encontrada por {source} para {current_object_name}.{target_col_name}")
                    # Retorna o que encontrou (descrição e/ou notas)
                    return found_desc, source, found_notes, source

    # Se não achou por nome exato, tenta via FKs (precisa do schema_data técnico)
    current_object_info = schema_data.get(current_object_name)
    if not current_object_info:
        logger.warning(f"Schema técnico não encontrado para {current_object_name} ao buscar heurística FK.")
        return None, None, None, None # MODIFICADO
    
    current_constraints = current_object_info.get('constraints', {})
    current_pk_cols = [col for pk in current_constraints.get('primary_key', []) for col in pk.get('columns', [])]

    # 3. Busca Direta (Se target_col é FK)
    for fk in current_constraints.get('foreign_keys', []):
        fk_columns = fk.get('columns', [])
        ref_table = fk.get('references_table')
        ref_columns = fk.get('references_columns', [])
        if target_col_name in fk_columns and ref_table and ref_columns:
            try:
                idx = fk_columns.index(target_col_name)
                ref_col_name = ref_columns[idx]
                # Busca descrição da PK referenciada
                ref_object_info = schema_data.get(ref_table) # Precisa info técnica da tabela referenciada
                if not ref_object_info:
                     logger.warning(f"Schema técnico não encontrado para tabela referenciada {ref_table}")
                     continue
                ref_obj_type = ref_object_info.get('object_type', 'TABLE')
                ref_obj_type_key = ref_obj_type + "S"
                
                ref_col_meta = metadata.get(ref_obj_type_key, {}).get(ref_table, {}).get('COLUMNS', {}).get(ref_col_name)
                if ref_col_meta and ref_col_meta.get('description', '').strip():
                    desc = ref_col_meta['description']
                    source = f"chave estrangeira para `{ref_table}.{ref_col_name}`"
                    logger.debug(f"Heurística: Descrição encontrada por {source} para {current_object_name}.{target_col_name}")
                    return desc, source, None, None # MODIFICADO: Retorna None para notas
            except (IndexError, ValueError): continue

    # 4. Busca Inversa (Se target_col é PK)
    if current_object_info: # Reutiliza a variável carregada
        current_constraints = current_object_info.get('constraints', {})
        current_pk_cols = [col for pk in current_constraints.get('primary_key', []) for col in pk.get('columns', [])]
        if target_col_name in current_pk_cols:
            for other_obj_name, other_obj_info in schema_data.items():
                if other_obj_name == current_object_name: continue
                other_constraints = other_obj_info.get('constraints', {})
                for other_fk in other_constraints.get('foreign_keys', []):
                     if other_fk.get('references_table') == current_object_name and \
                        target_col_name in other_fk.get('references_columns', []):
                         referencing_columns = other_fk.get('columns', [])
                         ref_pk_columns = other_fk.get('references_columns', [])
                         try:
                             idx_pk = ref_pk_columns.index(target_col_name)
                             referencing_col_name = referencing_columns[idx_pk]
                             other_obj_type = other_obj_info.get('object_type', 'TABLE')
                             other_obj_type_key = other_obj_type + "S"
                             other_col_meta = metadata.get(other_obj_type_key, {}).get(other_obj_name, {}).get('COLUMNS', {}).get(referencing_col_name)
                             if other_col_meta and other_col_meta.get('description', '').strip():
                                 desc = other_col_meta['description']
                                 source = f"coluna `{referencing_col_name}` em `{other_obj_name}` (ref. esta PK)"
                                 logger.debug(f"Heurística: Descrição encontrada por {source} para {current_object_name}.{target_col_name}")
                                 return desc, source, None, None # MODIFICADO: Retorna None para notas
                         except (IndexError, ValueError): continue

    return None, None, None, None # MODIFICADO: Nenhuma informação encontrada

# --- Função get_column_concept (Adaptada de view_schema_app.py) ---
def get_column_concept(schema_data, obj_name, col_name):
    """Determina o conceito raiz (PK referenciada ou a própria PK/coluna)."""
    if not schema_data or obj_name not in schema_data:
        return (obj_name, col_name) # Retorna ela mesma se não achar info
    
    obj_info = schema_data[obj_name]
    constraints = obj_info.get('constraints', {})
    pk_cols = [col for pk in constraints.get('primary_key', []) for col in pk.get('columns', [])]
    
    # É FK? Retorna a PK referenciada
    for fk in constraints.get('foreign_keys', []):
        fk_columns = fk.get('columns', [])
        ref_table = fk.get('references_table')
        ref_columns = fk.get('references_columns', [])
        if col_name in fk_columns and ref_table and ref_columns:
             try:
                idx = fk_columns.index(col_name)
                # Retorna tupla (tabela_ref, coluna_pk_ref)
                return (ref_table, ref_columns[idx]) 
             except (IndexError, ValueError): pass # Ignora FKs malformadas
    
    # É PK ou coluna normal? Retorna ela mesma (tupla tabela_atual, coluna_atual)
    return (obj_name, col_name)

# --- NOVAS Funções para Visão Geral ---
@st.cache_data # Cache para contagens (não devem mudar frequentemente sem ação externa)
def load_overview_counts(file_path):
    """Carrega as contagens e timestamps da visão geral."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logger.warning(f"Aviso: Arquivo de contagens '{file_path}' inválido.")
            return {}
        except Exception as e:
            logger.error(f"Erro inesperado ao carregar contagens: {e}")
            return {}
    else:
        logger.info(f"Arquivo de contagens '{file_path}' não encontrado. Contagens não serão exibidas.")
        return {}

# REMOVIDO CACHE - Calcular a cada vez para refletir edições nos metadados
#@st.cache_data(depends_on=[st.session_state.get('metadata')]) # Recalcular se metadata mudar
def generate_documentation_overview(technical_schema, metadata, overview_counts):
    """Gera DataFrame da visão geral, incluindo contagens/timestamps do cache."""
    logger.info("Gerando visão geral da documentação...")
    overview_data = []
    total_objects_processed = 0

    for name, tech_info in technical_schema.items():
        object_type = tech_info.get('object_type')
        if object_type not in ["TABLE", "VIEW"]:
             continue

        total_objects_processed += 1
        columns_tech = tech_info.get('columns', [])
        total_cols = len(columns_tech)
        
        # Acessa metadados com segurança
        key_type = object_type + "S" if object_type else None
        object_meta = metadata.get(key_type, {}).get(name, {})
        object_columns_meta = object_meta.get('COLUMNS', {})
        obj_desc_exists = bool(object_meta.get('description', '').strip())
        
        described_cols = 0
        noted_cols = 0
        if total_cols > 0:
            for col_def in columns_tech:
                col_name = col_def.get('name')
                if col_name:
                    col_meta = object_columns_meta.get(col_name, {})
                    if col_meta.get('description', '').strip(): described_cols += 1
                    if col_meta.get('value_mapping_notes', '').strip(): noted_cols += 1
            desc_perc = (described_cols / total_cols) * 100
            notes_perc = (noted_cols / total_cols) * 100
        else:
            desc_perc = 0; notes_perc = 0

        # Recupera contagem e timestamp do cache
        count_info = overview_counts.get(name, {})
        row_count_val = count_info.get("count", "N/A")
        timestamp_val = count_info.get("timestamp")

        # Formata contagem para exibição E extrai valor raw
        row_count_display = row_count_val
        raw_count = np.nan # Default para NaN se não for número válido
        if isinstance(row_count_val, int) and row_count_val >= 0:
             row_count_display = f"{row_count_val:,}".replace(",", ".") # Formato brasileiro
             raw_count = row_count_val # Guarda o int original
        elif isinstance(row_count_val, str) and row_count_val.startswith("Erro"):
            row_count_display = "Erro" # Simplifica exibição de erro
            # raw_count permanece NaN
        elif row_count_val == "N/A":
             row_count_display = "N/A"
             # raw_count permanece NaN

        # Formata timestamp para exibição
        timestamp_display = "-"
        if timestamp_val:
            try:
                dt_obj = datetime.datetime.fromisoformat(timestamp_val)
                timestamp_display = dt_obj.strftime("%d/%m/%y %H:%M") # Formato mais curto
            except ValueError:
                 timestamp_display = "Inválido"

        overview_data.append({
            'Objeto': name,
            'Tipo': object_type,
            'Descrição?': "✅" if obj_desc_exists else "❌",
            'Total Colunas': total_cols,
            'Linhas (Cache)': row_count_display,
            'Contagem Em': timestamp_display,
            'Col. Descritas': described_cols,
            '% Descritas': f"{desc_perc:.1f}%",
            'Col. c/ Notas': noted_cols,
            '% c/ Notas': f"{notes_perc:.1f}%",
            '_Linhas_Raw': raw_count # NOVO: Adiciona coluna raw
        })

    df_overview = pd.DataFrame(overview_data)
    if not df_overview.empty:
        # NOVO: Converte coluna raw para numérico
        df_overview['_Linhas_Raw'] = pd.to_numeric(df_overview['_Linhas_Raw'], errors='coerce')

        # Ordenar colunas para melhor visualização (mantém ordem original)
        cols_order = ['Objeto', 'Tipo', 'Descrição?', 'Total Colunas', 'Linhas (Cache)', 'Contagem Em',
                      'Col. Descritas', '% Descritas', 'Col. c/ Notas', '% c/ Notas']
        # Remove colunas que não existem mais ou ajusta a ordem
        cols_order = [col for col in cols_order if col in df_overview.columns]
        
        # NOVO: Ordena o DataFrame pelos dados raw (desc) e depois por tipo/objeto
        # Mantém NaNs por último na ordenação decrescente
        df_overview = df_overview.sort_values(
            by=['_Linhas_Raw', 'Tipo', 'Objeto'], 
            ascending=[False, True, True],
            na_position='last' # Garante que erros/N/A fiquem no final
        ).reset_index(drop=True)
        
        # Retorna apenas as colunas visíveis (sem _Linhas_Raw)
        df_overview_display = df_overview[cols_order]
    else:
        df_overview_display = pd.DataFrame(columns=cols_order) # Retorna DF vazio com colunas certas se não houver dados
        
    logger.info(f"Visão geral gerada. Shape: {df_overview_display.shape}")
    return df_overview_display # Retorna o DF pronto para exibição

# --- NOVA Função para buscar Timestamp da Última NFS --- 
# REMOVIDO CACHE - Buscar sob demanda
#@st.cache_data(ttl=300) # Cache de 5 minutos
def fetch_latest_nfs_timestamp(db_path, user, password, charset):
    """Busca a data/hora da última NFS emitida da VIEW_DASH_NFS."""
    conn = None
    logger.info("Tentando buscar timestamp da última NFS...")
    try:
        conn = fdb.connect(dsn=db_path, user=user, password=password, charset=charset)
        cur = conn.cursor()
        # Query para buscar a data e hora mais recentes
        sql = '''
            SELECT FIRST 1 NFS_DATA_EMISSAO, HORA_EMISSAO 
            FROM VIEW_DASH_NFS 
            ORDER BY NFS_DATA_EMISSAO DESC, HORA_EMISSAO DESC
        '''
        cur.execute(sql)
        result = cur.fetchone()
        cur.close()
        conn.close()
        logger.info(f"Resultado da query de timestamp: {result}")

        if result:
            nfs_date, nfs_time = result
            # Tenta combinar data e hora
            if isinstance(nfs_date, datetime.date) and isinstance(nfs_time, datetime.time):
                # Combinação padrão se ambos forem tipos corretos
                combined_dt = datetime.datetime.combine(nfs_date, nfs_time)
                logger.info(f"Timestamp combinado: {combined_dt}")
                return combined_dt
            elif isinstance(nfs_date, datetime.date):
                 # Se a hora não for um tipo time, tenta interpretar como string HH:MM:SS
                 if isinstance(nfs_time, str):
                     try:
                         time_obj = datetime.datetime.strptime(nfs_time, '%H:%M:%S').time()
                         combined_dt = datetime.datetime.combine(nfs_date, time_obj)
                         logger.info(f"Timestamp combinado (data+str_hora): {combined_dt}")
                         return combined_dt
                     except ValueError:
                         logger.warning(f"Não foi possível parsear HORA_EMISSAO '{nfs_time}' como HH:MM:SS. Retornando apenas data.")
                         return nfs_date # Retorna apenas a data se hora for inválida
                 else:
                    logger.warning(f"HORA_EMISSAO não é datetime.time nem string reconhecível: {type(nfs_time)}. Retornando apenas data.")
                    return nfs_date # Retorna apenas a data se a hora não for válida
            else:
                logger.warning(f"NFS_DATA_EMISSAO não é datetime.date: {type(nfs_date)}. Não foi possível determinar timestamp.")
                return "Data Inválida"
        else:
            logger.info("Nenhum registro encontrado em VIEW_DASH_NFS.")
            return "Nenhum Registro"
            
    except fdb.Error as e:
        logger.error(f"Erro do Firebird ao buscar timestamp NFS: {e}", exc_info=True)
        # Retorna a mensagem de erro para exibição
        return f"Erro DB: {e.fb_message if hasattr(e, 'fb_message') else e}" 
    except Exception as e:
        logger.exception("Erro inesperado ao buscar timestamp NFS:")
        return f"Erro App: {e}"
    finally:
        if conn and not conn.closed:
            try: conn.close()
            except Exception: pass

# --- NOVA Função para buscar amostra de dados --- 
def fetch_sample_data(db_path, user, password, charset, table_name, num_rows=10):
    """Busca as N primeiras linhas de uma tabela/view específica."""
    conn = None
    logger.info(f"Tentando buscar amostra de dados para {table_name} ({num_rows} linhas)...")
    if num_rows <= 0:
        logger.warning("Número de linhas para buscar deve ser positivo.")
        return pd.DataFrame() # Retorna DataFrame vazio

    try:
        conn = fdb.connect(dsn=db_path, user=user, password=password, charset=charset)
        cur = conn.cursor()
        # Usar placeholders seguros para o nome da tabela NÃO é suportado diretamente
        # para nomes de tabelas/identificadores pelo DB-API. Precisamos ter cuidado.
        # Validar table_name minimamente (evitar injeção MUITO básica)
        if not re.match(r"^[A-Z0-9_]+$", table_name.upper()):
            raise ValueError(f"Nome de tabela inválido fornecido: {table_name}")
            
        # Construir a query com segurança (sem format string direta)
        # Firebird 3.0+ suporta FETCH FIRST N ROWS ONLY
        sql = f"SELECT * FROM \"{table_name}\" FETCH FIRST {int(num_rows)} ROWS ONLY"
        
        logger.debug(f"Executando query de amostra: {sql}")
        cur.execute(sql)
        
        # Obter nomes das colunas da descrição do cursor
        colnames = [desc[0] for desc in cur.description]
        
        # Obter os dados
        data = cur.fetchall()
        
        cur.close()
        conn.close()
        logger.info(f"Amostra de dados obtida para {table_name}.")
        
        # Criar DataFrame Pandas
        df = pd.DataFrame(data, columns=colnames)
        return df
        
    except fdb.Error as e:
        error_msg = f"Erro DB ao buscar amostra para {table_name}: {e.fb_message if hasattr(e, 'fb_message') else e}"
        logger.error(error_msg, exc_info=True)
        return error_msg # Retorna a string de erro
    except ValueError as e:
        error_msg = f"Erro ao buscar amostra para {table_name}: {e}"
        logger.error(error_msg)
        return error_msg # Retorna a string de erro
    except Exception as e:
        error_msg = f"Erro inesperado ao buscar amostra para {table_name}: {e}"
        logger.exception(error_msg)
        return error_msg # Retorna a string de erro
    finally:
        if conn and not conn.closed:
            try: conn.close()
            except Exception: pass
# --- FIM NOVA Função ---

# --- NOVA Função para Heurística Global ---
def apply_heuristics_globally(metadata_dict, technical_schema):
    """Aplica a heurística find_existing_info a todas as colunas vazias."""
    logger.info("Iniciando aplicação global da heurística...")
    updated_desc_count = 0
    updated_notes_count = 0
    already_filled_desc_count = 0
    already_filled_notes_count = 0
    not_found_count = 0 # Contador geral para colunas onde nada foi encontrado
    columns_processed = 0

    objects_to_process = {}
    for obj_type_key in ['TABLES', 'VIEWS']:
         if obj_type_key in metadata_dict:
              objects_to_process.update(metadata_dict[obj_type_key])

    total_objects = len(objects_to_process)
    processed_objects = 0
    # Placeholder para possível barra de progresso se necessário
    # progress_bar = st.progress(0.0, text="Iniciando heurística...") 

    for obj_name, obj_meta in objects_to_process.items():
        processed_objects += 1
        # progress = processed_objects / total_objects
        # progress_bar.progress(progress, text=f"Processando {obj_name} ({processed_objects}/{total_objects})")

        if 'COLUMNS' not in obj_meta:
            continue

        columns_meta = obj_meta['COLUMNS']
        for col_name, col_meta_target in columns_meta.items(): # Renomeado para clareza
            columns_processed += 1
            current_desc = col_meta_target.get('description', '').strip()
            current_notes = col_meta_target.get('value_mapping_notes', '').strip()
            found_something_new = False

            # Só busca se algo estiver faltando (descrição OU notas)
            if not current_desc or not current_notes:
                # --- DEBUG CALL SITE (GLOBAL HEURISTICS) --- #
                logger.debug(f"[GLOBAL HEURISTICS] Calling find_existing_info for {obj_name}.{col_name}")
                logger.debug(f"[GLOBAL HEURISTICS] metadata_dict type: {type(metadata_dict)}, value[:200]: '{str(metadata_dict)[:200]}...'")
                # --- END DEBUG CALL SITE ---
                # Procura informação existente (descrição E/OU notas)
                suggested_desc, desc_source, suggested_notes, notes_source = find_existing_info(
                    metadata_dict, technical_schema, obj_name, col_name
                )

                # Aplica descrição se vazia e sugestão encontrada
                if not current_desc and suggested_desc:
                    logger.debug(f"Heurística global (Descrição): Atualizando '{obj_name}.{col_name}' com base em '{desc_source}'")
                    col_meta_target['description'] = suggested_desc
                    updated_desc_count += 1
                    found_something_new = True
                elif current_desc:
                    already_filled_desc_count += 1

                # Aplica notas se vazias e sugestão encontrada
                if not current_notes and suggested_notes:
                    logger.debug(f"Heurística global (Notas): Atualizando '{obj_name}.{col_name}' com base em '{notes_source}'")
                    col_meta_target['value_mapping_notes'] = suggested_notes
                    updated_notes_count += 1
                    found_something_new = True
                elif current_notes:
                    already_filled_notes_count += 1

                # Se não encontrou nada novo para esta coluna
                if not found_something_new and (not current_desc or not current_notes):
                    not_found_count += 1
            else:
                # Ambos já estavam preenchidos
                already_filled_desc_count += 1
                already_filled_notes_count += 1

    # Ajusta contagem de "já preenchidos" para não contar duas vezes a mesma coluna
    # total_columns = columns_processed # Ou calcular total de colunas de outra forma
    # already_filled_count = min(already_filled_desc_count, already_filled_notes_count) # Aproximação

    logger.info(f"Aplicação global da heurística concluída.")
    logger.info(f"  Descrições: {updated_desc_count} atualizadas, {already_filled_desc_count} já preenchidas.")
    logger.info(f"  Notas: {updated_notes_count} atualizadas, {already_filled_notes_count} já preenchidas.")
    logger.info(f"  Colunas onde nenhuma sugestão foi encontrada (para campos vazios): {not_found_count}")

    # progress_bar.progress(1.0, text="Heurística Concluída!")
    # Retorna contagens separadas para melhor feedback
    return updated_desc_count, updated_notes_count
# --- FIM NOVA Função ---

# --- NOVO: Função para Preencher Descrições via Chaves FK -> PK ---
def populate_descriptions_from_keys(metadata_dict, technical_schema):
    """Preenche descrições de FKs vazias com base nas descrições das PKs referenciadas."""
    logger.info("Iniciando preenchimento de descrições via chaves FK -> PK...")
    updated_count = 0
    processed_fk_cols = 0

    # Iterar sobre todas as tabelas/views no schema técnico
    for table_name, table_data in technical_schema.items():
        if not isinstance(table_data, dict) or table_data.get('object_type') not in ['TABLE', 'VIEW']:
            continue

        obj_type = table_data.get('object_type', 'TABLE') # Default para TABLE
        obj_type_key = obj_type + "S"
        constraints = table_data.get('constraints', {})
        foreign_keys = constraints.get('foreign_keys', [])

        # Acessar metadados da tabela atual (garantir que exista)
        if obj_type_key not in metadata_dict: metadata_dict[obj_type_key] = OrderedDict()
        if table_name not in metadata_dict[obj_type_key]: metadata_dict[obj_type_key][table_name] = OrderedDict({'description': '', 'COLUMNS': OrderedDict()})
        if 'COLUMNS' not in metadata_dict[obj_type_key][table_name]: metadata_dict[obj_type_key][table_name]['COLUMNS'] = OrderedDict()
        current_table_meta_cols = metadata_dict[obj_type_key][table_name]['COLUMNS']

        # Iterar sobre as chaves estrangeiras da tabela atual
        for fk in foreign_keys:
            fk_cols = fk.get('columns', [])
            ref_table = fk.get('references_table')
            ref_cols = fk.get('references_columns', [])

            if not ref_table or len(fk_cols) != len(ref_cols):
                logger.warning(f"FK malformada em {table_name}: {fk}")
                continue # Pula esta FK se estiver inconsistente

            # Encontrar o tipo de objeto da tabela referenciada
            ref_table_data = technical_schema.get(ref_table)
            if not ref_table_data:
                logger.warning(f"Tabela referenciada {ref_table} não encontrada no schema técnico.")
                continue
            ref_obj_type = ref_table_data.get('object_type', 'TABLE')
            ref_obj_type_key = ref_obj_type + "S"

            # Iterar sobre as colunas da FK
            for i, fk_col_name in enumerate(fk_cols):
                processed_fk_cols += 1
                ref_col_name = ref_cols[i]

                # Garantir que a coluna FK exista nos metadados
                if fk_col_name not in current_table_meta_cols:
                    current_table_meta_cols[fk_col_name] = OrderedDict()
                fk_col_meta = current_table_meta_cols[fk_col_name]

                # Verificar se a descrição da FK está vazia
                current_fk_desc = fk_col_meta.get('description', '').strip()
                if not current_fk_desc:
                    # Buscar descrição da PK referenciada
                    ref_table_meta = metadata_dict.get(ref_obj_type_key, {}).get(ref_table, {})
                    ref_col_meta = ref_table_meta.get('COLUMNS', {}).get(ref_col_name, {})
                    ref_pk_desc = ref_col_meta.get('description', '').strip()

                    # Se encontrou descrição na PK, aplica na FK
                    if ref_pk_desc:
                        source_str = f"key -> {ref_table}.{ref_col_name}"
                        logger.debug(f"Preenchendo '{table_name}.{fk_col_name}' via {source_str}")
                        fk_col_meta['description'] = ref_pk_desc
                        updated_count += 1

    logger.info(f"Preenchimento via chaves concluído. Colunas FK processadas: {processed_fk_cols}. Descrições atualizadas: {updated_count}")
    return updated_count
# --- FIM Função FK -> PK ---


# --- NOVO: Funções FAISS ---
@st.cache_resource # Cache do índice FAISS para performance
def build_faiss_index(schema_data):
    logger.info("---> EXECUTANDO build_faiss_index") # Log de diagnóstico
    """Constrói um índice FAISS a partir dos embeddings das colunas no schema_data."""
    embeddings = []
    index_to_key = [] # Mapeia o índice interno do FAISS para (table_name, col_index)

    logger.info("Construindo índice FAISS a partir dos embeddings...")
    items_with_embeddings = 0
    items_without_embeddings = 0

    for obj_name, obj_data in schema_data.items():
        if isinstance(obj_data, dict) and 'columns' in obj_data:
            for i, col_data in enumerate(obj_data['columns']):
                embedding = col_data.get('embedding')
                if embedding and isinstance(embedding, list) and len(embedding) == EMBEDDING_DIMENSION:
                    embeddings.append(embedding)
                    index_to_key.append((obj_name, i))
                    items_with_embeddings += 1
                else:
                    # Guardar espaço no mapeamento mesmo sem embedding válido,
                    # ou pular? Optamos por pular para simplificar.
                    items_without_embeddings += 1
                    # logger.debug(f"Coluna {obj_name}.{col_data.get('name', i)} sem embedding válido.")

    if not embeddings:
        logger.warning("Nenhum embedding válido encontrado para construir o índice FAISS.")
        return None, []

    embeddings_np = np.array(embeddings).astype('float32') # FAISS requer float32
    dimension = embeddings_np.shape[1]
    if dimension != EMBEDDING_DIMENSION:
        logger.warning(f"Dimensão dos embeddings ({dimension}) difere da esperada ({EMBEDDING_DIMENSION}). Ajuste EMBEDDING_DIMENSION.")
        # Poderia tentar continuar, mas é mais seguro parar se a dimensão estiver errada.
        # return None, []

    # Usar IndexFlatL2 para busca exata por distância L2 (Euclidiana)
    # Para datasets muito grandes, considerar índices aproximados como IndexIVFFlat
    index = faiss.IndexFlatL2(dimension)
    index.add(embeddings_np)

    logger.info(f"Índice FAISS construído com {index.ntotal} vetores. {items_without_embeddings} colunas ignoradas por falta de embedding.")

    # Opcional: Salvar índice para carregamento rápido futuro
    # try:
    #     faiss.write_index(index, FAISS_INDEX_FILE)
    #     logger.info(f"Índice FAISS salvo em {FAISS_INDEX_FILE}")
    # except Exception as e:
    #     logger.error(f"Erro ao salvar índice FAISS: {e}")

    return index, index_to_key

def find_similar_columns(faiss_index, schema_data, index_to_key_map, target_embedding, k=5):
    """Busca as k colunas mais similares no índice FAISS que possuem descrição."""
    if faiss_index is None or not isinstance(target_embedding, np.ndarray):
        return []

    target_embedding_np = target_embedding.astype('float32').reshape(1, -1)
    try:
        # Busca k+1 vizinhos (incluindo o próprio item)
        distances, indices = faiss_index.search(target_embedding_np, k + 1)
    except Exception as e:
        logger.error(f"Erro durante a busca FAISS: {e}")
        return []

    similar_columns = []
    # indices[0] contém a lista de índices dos vizinhos mais próximos
    for i in range(1, len(indices[0])): # Pula o primeiro resultado (ele mesmo)
        idx = indices[0][i]
        if idx == -1: # FAISS pode retornar -1 se não encontrar vizinhos suficientes
            continue

        try:
            table_name, col_index = index_to_key_map[idx]
            column_data = schema_data.get(table_name, {}).get('columns', [])[col_index]
            col_name = column_data.get('name', 'N/A')
            description = column_data.get('business_description', '').strip()

            if description: # Adiciona apenas se tiver descrição
                similar_columns.append({
                    'table': table_name,
                    'column': col_name,
                    'description': description,
                    'distance': float(distances[0][i]) # Distância Euclidiana ao quadrado (L2)
                })
                if len(similar_columns) == k: # Para se já achou k vizinhos com descrição
                    break
        except IndexError:
            logger.warning(f"Índice FAISS {idx} fora dos limites do mapeamento index_to_key_map.")
            continue
        except Exception as e:
            logger.error(f"Erro ao processar resultado FAISS com índice {idx}: {e}")
            continue

    return similar_columns

# --- NOVO: Função Wrapper para Embedding da Query ---
def get_query_embedding(text: str) -> np.ndarray | None:
    """Gera embedding para um texto usando a função Ollama e trata erros."""
    if not OLLAMA_EMBEDDING_AVAILABLE:
        logger.warning("Tentativa de gerar embedding sem função disponível.")
        return None
    try:
        with st.spinner("Gerando embedding para a pergunta..."): # Feedback
            embedding_list = get_embedding(text) # Chama a função importada
        
        if embedding_list and isinstance(embedding_list, list):
            embedding_np = np.array(embedding_list).astype('float32')
            if embedding_np.shape[0] == EMBEDDING_DIMENSION:
                logger.info(f"Embedding gerado para a query (Shape: {embedding_np.shape})")
                return embedding_np
            else:
                logger.error(f"Erro: Dimensão do embedding da query ({embedding_np.shape[0]}) diferente da esperada ({EMBEDDING_DIMENSION}).")
                st.toast(f"Erro na dimensão do embedding gerado pela IA ({embedding_np.shape[0]} vs {EMBEDDING_DIMENSION}).", icon="❌")
                return None
        else:
            logger.error(f"Função get_embedding não retornou uma lista válida: {type(embedding_list)}")
            st.toast("Erro ao gerar embedding da pergunta (resposta inválida da IA).", icon="❌")
            return None
    except Exception as e:
        logger.exception("Erro ao chamar get_embedding:")
        st.toast(f"Erro ao gerar embedding da pergunta: {e}", icon="❌")
        return None
# --- FIM: Função Wrapper Embedding ---

# --- NOVO: Função para Comparar Metadados ---
def compare_metadata_changes(initial_meta, current_meta):
    """Compara dois dicionários de metadados e conta novas descrições/notas."""
    new_descriptions = 0
    new_notes = 0
    if not initial_meta or not current_meta:
        logger.warning("Metadados iniciais ou atuais ausentes para comparação.")
        return 0, 0

    # Iterar sobre tipos de objeto (TABLES, VIEWS)
    for obj_type_key in list(current_meta.keys()): # Usar list() para evitar erro de modificação durante iteração
        if obj_type_key not in ['TABLES', 'VIEWS']:
            continue # Pular outras chaves como _GLOBAL_CONTEXT

        current_objects = current_meta.get(obj_type_key, {})
        initial_objects = initial_meta.get(obj_type_key, {})

        for obj_name, current_obj_data in current_objects.items():
            initial_obj_data = initial_objects.get(obj_name, {})
            current_cols = current_obj_data.get('COLUMNS', {})
            initial_cols = initial_obj_data.get('COLUMNS', {})

            for col_name, current_col_data in current_cols.items():
                initial_col_data = initial_cols.get(col_name, {})

                # Compara Descrição
                current_desc = current_col_data.get('description', '').strip()
                initial_desc = initial_col_data.get('description', '').strip()
                if current_desc and not initial_desc:
                    new_descriptions += 1

                # Compara Notas de Mapeamento
                current_notes = current_col_data.get('value_mapping_notes', '').strip()
                initial_notes = initial_col_data.get('value_mapping_notes', '').strip()
                if current_notes and not initial_notes:
                    new_notes += 1

    logger.info(f"Comparação de metadados: {new_descriptions} novas descrições, {new_notes} novas notas.")
    return new_descriptions, new_notes
# --- FIM Função Comparar ---

# --- FIM Função Comparar ---

# --- NOVO: Funções para Análise Estrutural e Importância ---

@st.cache_data # Cacheia a análise estrutural, pois só depende do schema técnico
def analyze_key_structure(schema_data):
    logger.info("---> EXECUTANDO analyze_key_structure") # Log de diagnóstico
    """Analisa o schema_data para identificar tipos de chaves e calcular importância inicial."""
    logger.info("Analisando estrutura de chaves do schema...")
    composite_pk_tables = {}
    junction_tables = {}
    composite_fk_details = {}
    column_roles = defaultdict(lambda: {'role': 'Normal', 'importance_score': 0, 'details': ''}) # Default para coluna normal

    fk_ref_counts = schema_data.get('fk_reference_counts', {})

    for table_name, table_data in schema_data.items():
        if not isinstance(table_data, dict) or table_data.get('object_type') not in ['TABLE', 'VIEW']:
            continue

        constraints = table_data.get('constraints', {})
        primary_keys = constraints.get('primary_key', [])
        foreign_keys = constraints.get('foreign_keys', [])
        columns_in_table = {col.get('name') for col in table_data.get('columns', []) if col.get('name')}

        # 1. Analisar Chaves Primárias
        pk_column_names = set()
        is_composite_pk = False
        if primary_keys:
            pk_def = primary_keys[0] # Assume-se uma PK por tabela para simplificar
            pk_cols = pk_def.get('columns', [])
            pk_column_names.update(pk_cols)
            if len(pk_cols) > 1:
                is_composite_pk = True
                composite_pk_tables[table_name] = pk_cols
                for col_name in pk_cols:
                    column_roles[(table_name, col_name)]['role'] = 'PK Comp'
                    column_roles[(table_name, col_name)]['importance_score'] += 5 # Alta importância base
            elif len(pk_cols) == 1:
                 col_name = pk_cols[0]
                 column_roles[(table_name, col_name)]['role'] = 'PK'
                 column_roles[(table_name, col_name)]['importance_score'] += 3 # Importância base média

        # 2. Analisar Chaves Estrangeiras e Tabelas de Junção
        is_junction_table = False
        junction_fk_details = []
        fk_columns_in_table = set()

        for fk in foreign_keys:
            fk_cols = fk.get('columns', [])
            ref_table = fk.get('references_table')
            ref_cols = fk.get('references_columns', [])
            fk_columns_in_table.update(fk_cols)

            if len(fk_cols) > 1:
                # FK Composta
                for i, col_name in enumerate(fk_cols):
                    if col_name in column_roles[(table_name, col_name)] and column_roles[(table_name, col_name)]['role'] == 'PK Comp':
                         column_roles[(table_name, col_name)]['role'] = 'PK/FK Comp' # Promove se for PK e FK composta
                         column_roles[(table_name, col_name)]['importance_score'] += 2 # Bônus
                    elif col_name not in pk_column_names: # Só marca como FK Comp se não for PK simples
                        column_roles[(table_name, col_name)]['role'] = 'FK Comp'
                    column_roles[(table_name, col_name)]['importance_score'] += 1 # Leve aumento por ser parte de FK composta
                    # Armazena detalhes da FK composta
                    try: ref_col_name = ref_cols[i] if ref_cols and i < len(ref_cols) else 'N/A'
                    except IndexError: ref_col_name = 'N/A'
                    detail_str = f"parte de FK composta referenciando {ref_table}.{ref_col_name}"
                    column_roles[(table_name, col_name)]['details'] = detail_str
                    composite_fk_details[(table_name, col_name)] = detail_str
            elif len(fk_cols) == 1:
                # FK Simples
                col_name = fk_cols[0]
                if col_name in pk_column_names:
                    # É PK e FK (potencial tabela de junção)
                    if column_roles[(table_name, col_name)]['role'] == 'PK Comp':
                        column_roles[(table_name, col_name)]['role'] = 'PK/FK Comp' # Promove se for PK Comp e FK simples
                        column_roles[(table_name, col_name)]['importance_score'] += 2
                    else:
                         column_roles[(table_name, col_name)]['role'] = 'PK/FK'
                         column_roles[(table_name, col_name)]['importance_score'] += 4 # Alta importância base
                    junction_fk_details.append(f"{col_name} -> {ref_table}.{ref_cols[0] if ref_cols else 'N/A'}")
                else:
                    # Apenas FK simples
                    column_roles[(table_name, col_name)]['role'] = 'FK'
                    column_roles[(table_name, col_name)]['importance_score'] += 1 # Baixa importância base
                    try: ref_col_name = ref_cols[0] if ref_cols else 'N/A'
                    except IndexError: ref_col_name = 'N/A'
                    column_roles[(table_name, col_name)]['details'] = f"-> {ref_table}.{ref_col_name}"

            # Checa se a coluna da FK também é parte da PK (para identificar junção)
            if pk_column_names.intersection(fk_cols):
                 is_junction_table = True

        # Se a tabela tem PK e todas as colunas da PK são também FKs, é uma tabela de junção
        if is_junction_table and pk_column_names and pk_column_names.issubset(fk_columns_in_table):
             junction_tables[table_name] = junction_fk_details
             # Aumenta a importância das colunas PK/FK em tabelas de junção
             for col_name in pk_column_names:
                  column_roles[(table_name, col_name)]['importance_score'] += 2

    # 3. Ajustar Score de Importância baseado na Contagem de Referências
    # Define limites para categorias de contagem (ajustar conforme necessário)
    HIGH_REF_THRESHOLD = 50
    MEDIUM_REF_THRESHOLD = 10

    for (table_name, col_name), role_data in column_roles.items():
        full_col_name = f"{table_name}.{col_name}"
        ref_count = fk_ref_counts.get(full_col_name, 0)
        
        # Bônus por ser referenciado
        if ref_count >= HIGH_REF_THRESHOLD:
            role_data['importance_score'] += 3
        elif ref_count >= MEDIUM_REF_THRESHOLD:
            role_data['importance_score'] += 2
        elif ref_count > 0:
            role_data['importance_score'] += 1
            
        # Ajuste fino: PKs simples muito referenciadas são muito importantes
        if role_data['role'] == 'PK' and ref_count >= HIGH_REF_THRESHOLD:
            role_data['importance_score'] += 3 # Bônus extra
            
        # Ajuste fino: Colunas normais em tabelas muito referenciadas (indica tabela importante)
        table_ref_count_approx = sum(fk_ref_counts.get(f"{table_name}.{c}", 0) for c in columns_in_table if f"{table_name}.{c}" in fk_ref_counts)
        if role_data['role'] == 'Normal' and table_ref_count_approx > HIGH_REF_THRESHOLD * 2: # Heurística grosseira
             role_data['importance_score'] += 1
             
    # 4. Definir Nível de Importância (Texto)
    for role_data in column_roles.values():
        score = role_data['importance_score']
        if score >= 8:
            role_data['importance_level'] = 'Máxima'
        elif score >= 5:
            role_data['importance_level'] = 'Alta'
        elif score >= 2:
            role_data['importance_level'] = 'Média'
        else:
             role_data['importance_level'] = 'Baixa'

    logger.info(f"Análise estrutural concluída. PKs Comp: {len(composite_pk_tables)}, Junção: {len(junction_tables)}, FKs Comp: {len(composite_fk_details)}")
    # Converter defaultdict para dict antes de retornar para ser picklable
    return composite_pk_tables, junction_tables, composite_fk_details, dict(column_roles)

# --- FIM Funções Análise Estrutural ---


# --- Função Principal / Carregamento de Dados ---
# --- FIM: Obter info de PK/FK ---
# --- NOVO: Função para lidar com a mudança do Toggle de Embeddings ---
def handle_embedding_toggle():
    """Callback para o toggle 'Usar Embeddings'. Carrega/descarrega o schema com embeddings."""
    use_embeddings = st.session_state.get('use_embeddings', False)
    logger.info(f"Toggle 'Usar Embeddings' mudou para: {use_embeddings}")
    st.spinner_text = "Atualizando schema e estruturas..." # Define texto para spinner
    with st.spinner(st.spinner_text): # Usar st.spinner para feedback
        if use_embeddings:
            # Tentar carregar schema com embeddings
            logger.info(f"Tentando carregar schema com embeddings de: {EMBEDDED_SCHEMA_FILE}")
            schema_embedded = load_technical_schema(EMBEDDED_SCHEMA_FILE)
            if schema_embedded:
                st.session_state.technical_schema = schema_embedded
                logger.info("Schema com embeddings carregado.")
                # Limpar caches e reconstruir estruturas com embeddings
                build_faiss_index.clear()
                analyze_key_structure.clear()
                logger.info("Caches FAISS e Análise de Chaves limpos.")
                st.session_state.faiss_index, st.session_state.index_to_key_map = build_faiss_index(st.session_state.technical_schema)
                st.session_state.key_analysis = analyze_key_structure(st.session_state.technical_schema)
                logger.info("Índice FAISS e Análise de Chaves reconstruídos com embeddings.")
                st.toast("Schema com embeddings carregado e estruturas atualizadas.", icon="✅")
            else:
                logger.error(f"Falha ao carregar schema com embeddings de {EMBEDDED_SCHEMA_FILE}.")
                st.error(f"Erro ao carregar {EMBEDDED_SCHEMA_FILE}. Verifique o arquivo e os logs. Revertendo para schema base.", icon="❌")
                st.session_state.use_embeddings = False # Desliga o toggle se falhar
                # Recarrega o schema base (garante consistência)
                st.session_state.technical_schema = load_technical_schema(TECHNICAL_SCHEMA_FILE)
                build_faiss_index.clear()
                analyze_key_structure.clear()
                st.session_state.faiss_index, st.session_state.index_to_key_map = build_faiss_index(st.session_state.technical_schema)
                st.session_state.key_analysis = analyze_key_structure(st.session_state.technical_schema)

        else:
            # Voltar para o schema base
            logger.info(f"Carregando schema base de: {TECHNICAL_SCHEMA_FILE}")
            schema_base = load_technical_schema(TECHNICAL_SCHEMA_FILE)
            if schema_base:
                 st.session_state.technical_schema = schema_base
                 logger.info("Schema base carregado.")
                 # Limpar caches e reconstruir estruturas com schema base
                 build_faiss_index.clear()
                 analyze_key_structure.clear()
                 logger.info("Caches FAISS e Análise de Chaves limpos.")
                 st.session_state.faiss_index, st.session_state.index_to_key_map = build_faiss_index(st.session_state.technical_schema)
                 st.session_state.key_analysis = analyze_key_structure(st.session_state.technical_schema)
                 logger.info("Índice FAISS e Análise de Chaves reconstruídos com schema base.")
                 st.toast("Usando schema base. Busca por similaridade desativada/limitada.", icon="ℹ️")
            else:
                 # Isso não deveria acontecer se o carregamento inicial funcionou, mas por segurança:
                 logger.critical("Falha crítica ao recarregar o schema base! O app pode ficar instável.")
                 st.error("Erro GRAVE ao recarregar o schema base. Verifique os logs.", icon="🚨")
                 # O que fazer aqui? Talvez parar o app? Por ora, apenas log/erro.


# --- Função Principal / Carregamento de Dados ---
def load_and_process_data():
    # --- NOVO: Configuração da Barra de Progresso e Tempos ---
    total_steps = 6
    progress_bar = st.progress(0.0, text="Iniciando carregamento...")
    start_time_total = time.time()
    step_times = {}
    current_step = 0

    def update_progress(step_name, step_start_time):
        nonlocal current_step
        duration = time.time() - step_start_time
        step_times[step_name] = duration
        current_step += 1
        progress_value = float(current_step) / total_steps
        progress_bar.progress(progress_value, text=f"({current_step}/{total_steps}) {step_name} concluída em {duration:.2f}s...")
        logger.info(f"Etapa '{step_name}' concluída em {duration:.2f}s")

    # --- Etapa 1: Carregar Schema Base --- *MODIFICADO*
    step_name = "Carregando Schema Base"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    # Carrega SEMPRE o schema técnico base primeiro
    schema_base = load_technical_schema(TECHNICAL_SCHEMA_FILE)
    if schema_base is None:
        st.error(f"Falha crítica: Não foi possível carregar o arquivo de schema técnico base obrigatório em '{TECHNICAL_SCHEMA_FILE}'.")
        st.stop()
    # update_progress(step_name, start_time_step) # REMOVIDO - Movido para depois de atribuir ao estado

    # --- Etapa 2: Carregar Metadados ---
    step_name = "Carregando Metadados"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    metadata_dict = load_metadata(METADATA_FILE)
    if metadata_dict is None:
        metadata_dict = {"TABLES": {}, "VIEWS": {}}
    update_progress(step_name, start_time_step)

    # --- Etapa 3: Carregar Contagens da Visão Geral ---
    step_name = "Carregando Contagens (Cache)"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    overview_counts = load_overview_counts(OVERVIEW_COUNTS_FILE)
    update_progress(step_name, start_time_step)

    # --- Etapa 4: Construir Índice FAISS (Base) --- *MODIFICADO*
    step_name = "Construindo Índice FAISS (Base)"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    # Constrói índice inicial com schema base (pode não ter embeddings)
    faiss_index, index_to_key_map = build_faiss_index(schema_base)
    update_progress(step_name, start_time_step)

    # --- Etapa 5: Analisar Estrutura de Chaves (Base) --- *MODIFICADO*
    step_name = "Analisando Estrutura de Chaves (Base)"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")
    # Passa o schema base para análise inicial
    key_analysis_result = analyze_key_structure(schema_base)
    update_progress(step_name, start_time_step)


    # --- Etapa 6: Inicializar Estado da Sessão ---
    step_name = "Inicializando Estado da Sessão"
    start_time_step = time.time()
    progress_bar.progress(float(current_step)/total_steps, text=f"({current_step+1}/{total_steps}) Executando: {step_name}...")

    # --- NOVO: Inicializa estado do toggle de auto-save e tempo --- #
    if 'auto_save_enabled' not in st.session_state:
        st.session_state.auto_save_enabled = False # Começa desligado
    if 'last_save_time' not in st.session_state:
        st.session_state.last_save_time = time.time() # Marca o tempo inicial

    # --- NOVO: Inicializa estado do toggle de embeddings --- # (Mantido)
    if 'use_embeddings' not in st.session_state:
        st.session_state.use_embeddings = False # Começa desligado

    # Armazenar estado inicial dos metadados (DENTRO da etapa de inicialização)
    if 'initial_metadata' not in st.session_state:
        logger.info("Armazenando estado inicial dos metadados.")
        try:
            st.session_state.initial_metadata = copy.deepcopy(metadata_dict)
        except Exception as e:
            logger.error(f"Erro ao fazer deepcopy dos metadados iniciais: {e}")
            st.session_state.initial_metadata = {}

    # --- Inicializa/Atualiza st.session_state ---
    # Armazena metadados editáveis
    if 'metadata' not in st.session_state:
        st.session_state.metadata = metadata_dict
    # Armazena o schema carregado INICIALMENTE (o base)
    if 'technical_schema' not in st.session_state:
        st.session_state.technical_schema = schema_base # MODIFICADO: Usa o schema base carregado
    # Armazena contagens
    if 'overview_counts' not in st.session_state:
        st.session_state.overview_counts = overview_counts if overview_counts else {}
    # Estados da UI
    if 'unsaved_changes' not in st.session_state:
        st.session_state.unsaved_changes = False
    if 'current_view' not in st.session_state:
        st.session_state.current_view = 'overview'
    if 'selected_object' not in st.session_state:
        st.session_state.selected_object = None
    if 'selected_column_index' not in st.session_state:
        st.session_state.selected_column_index = None
    if 'selected_object_type' not in st.session_state:
        st.session_state.selected_object_type = None
    # Estados de configuração
    if 'ollama_enabled' not in st.session_state:
        st.session_state.ollama_enabled = False
    if 'db_path' not in st.session_state:
        st.session_state.db_path = DEFAULT_DB_PATH
    if 'db_user' not in st.session_state:
        st.session_state.db_user = DEFAULT_DB_USER
    if 'db_password' not in st.session_state:
        st.session_state.db_password = os.getenv("FIREBIRD_PASSWORD", "")
    if 'db_charset' not in st.session_state:
        st.session_state.db_charset = DEFAULT_DB_CHARSET
    # Estado do timestamp DB
    if 'latest_db_timestamp' not in st.session_state:
        st.session_state.latest_db_timestamp = None
    # Armazenar índice FAISS e mapeamento (inicial, baseado no schema base)
    if 'faiss_index' not in st.session_state:
         st.session_state.faiss_index = faiss_index
    if 'index_to_key_map' not in st.session_state:
         st.session_state.index_to_key_map = index_to_key_map
    # Armazenar resultados da análise estrutural (inicial, baseado no schema base)
    if 'key_analysis' not in st.session_state:
        st.session_state.key_analysis = key_analysis_result # USA o resultado calculado

    update_progress(step_name, start_time_step) # Marca o fim da inicialização

    # --- Finalização ---
    total_time = time.time() - start_time_total
    progress_bar.empty() # Limpa a barra de progresso
    st.toast(f"Carregamento inicial concluído em {total_time:.2f}s!", icon="🎉")
    logger.info(f"Carregamento inicial concluído em {total_time:.2f}s.")
    # Exibe tempos individuais (opcional, pode ser comentado se ficar muito verboso)
    with st.expander("Detalhes do Tempo de Carregamento Inicial", expanded=False):
        for name, duration in step_times.items():
            st.write(f"- {name}: {duration:.3f}s")
        st.write(f"**- Tempo Total:** {total_time:.3f}s")

# --- Interface Streamlit ---
st.set_page_config(layout="wide", page_title="Editor de Metadados de Schema")

# --- Carregamento Inicial e Inicialização do Estado ---
# Chama a função para carregar dados e inicializar o estado da sessão
load_and_process_data()
# --- FIM: Carregamento Inicial ---


# --- NOVO: Carrega contagens cacheadas (Movido para dentro de load_and_process_data) ---
# if 'overview_counts' not in st.session_state:
#     st.session_state.overview_counts = load_overview_counts(OVERVIEW_COUNTS_FILE)

# --- NOVO: Inicializa estado para timestamp sob demanda (Movido para dentro de load_and_process_data) ---
# if 'latest_db_timestamp' not in st.session_state:
#     st.session_state.latest_db_timestamp = None # Inicializa como None

# --- Referência local aos dados no estado da sessão ---
metadata_dict = st.session_state.metadata
technical_schema_data = st.session_state.technical_schema # NOVO: Usar do estado da sessão

# --- NOVO: Inicializar estado para Ollama (Movido para dentro de load_and_process_data) ---
# if 'ollama_enabled' not in st.session_state:
#     st.session_state.ollama_enabled = False # MUDANÇA: Padrão para desabilitado

# --- Barra Lateral ---
st.sidebar.title("Navegação e Ações")

# Seletor de Modo
app_mode = st.sidebar.radio(
    "Modo de Operação",
    ["Editar Metadados", "Visão Geral", "Análise", "Chat com Schema"], # NOVO: Chat
    key='app_mode_selector'
)
st.sidebar.divider()

# --- NOVO: Exibição do Timestamp da Última NFS ---
st.sidebar.subheader("Referência Banco de Dados")

# Obtém parâmetros de conexão (podem vir de inputs ou defaults)
# !! Usando defaults hardcoded por enquanto !!
db_path_for_ts = DEFAULT_DB_PATH
db_user_for_ts = DEFAULT_DB_USER
# REMOVIDO: db_password_for_ts = DEFAULT_DB_PASSWORD # ATENÇÃO: Senha insegura
db_charset_for_ts = DEFAULT_DB_CHARSET

# NOVO: Lógica para obter a senha de st.secrets ou env var
try:
    # Prioridade: st.secrets (para deploy)
    db_password_for_ts = st.secrets.get("database", {}).get("password")
    if not db_password_for_ts:
        # Fallback: Variável de ambiente (para local)
        db_password_for_ts = os.getenv("FIREBIRD_PASSWORD")
        if not db_password_for_ts:
            st.error("Senha do banco Firebird não configurada em st.secrets ([database] > password) ou na variável de ambiente FIREBIRD_PASSWORD.")
            st.stop()
        else:
            st.sidebar.warning("Usando senha da variável de ambiente FIREBIRD_PASSWORD.", icon="🔑")
except Exception as e:
    st.error(f"Erro ao tentar obter a senha do banco: {e}")
    logger.error(f"Erro ao acessar st.secrets ou env var para senha: {e}")
    st.stop()

# Botão de atualização para o timestamp
if st.sidebar.button("Atualizar Referência DB", key="refresh_db_ts"):
    # Busca o novo timestamp e atualiza o estado da sessão
    st.session_state.latest_db_timestamp = fetch_latest_nfs_timestamp(
        db_path_for_ts, db_user_for_ts, db_password_for_ts, db_charset_for_ts
    )
    st.sidebar.success("Referência DB atualizada!", icon="✅")
    st.rerun() # Rerun para exibir o novo valor

# Busca o timestamp apenas se ainda não estiver no estado da sessão
if st.session_state.latest_db_timestamp is None:
    logger.info("Buscando timestamp inicial do DB...")
    st.session_state.latest_db_timestamp = fetch_latest_nfs_timestamp(
        db_path_for_ts, db_user_for_ts, db_password_for_ts, db_charset_for_ts
    )

# Busca e exibe o timestamp (ou erro) do ESTADO DA SESSÃO
latest_ts_result = st.session_state.latest_db_timestamp

if isinstance(latest_ts_result, datetime.datetime):
    # Formata para Data e Hora Brasileiras
    ts_display = latest_ts_result.strftime("%d/%m/%Y %H:%M:%S")
    st.sidebar.metric(label="Última NFS Emitida", value=ts_display)
elif isinstance(latest_ts_result, datetime.date):
    # Se só retornou data
    ts_display = latest_ts_result.strftime("%d/%m/%Y")
    st.sidebar.metric(label="Última NFS (Data)", value=ts_display, help="Não foi possível obter a hora.")
elif isinstance(latest_ts_result, str):
    # Se retornou uma string (erro ou "Nenhum Registro")
    st.sidebar.metric(label="Última NFS Emitida", value="-")
    st.sidebar.caption(f"Status: {latest_ts_result}")
    if "Erro DB" in latest_ts_result:
        st.sidebar.warning(f"Erro ao conectar/consultar o banco para obter a data de referência. Verifique as configurações e o log. {latest_ts_result}", icon="⚠️")
else:
    st.sidebar.metric(label="Última NFS Emitida", value="-")
    st.sidebar.caption("Status: Desconhecido")

st.sidebar.divider()

# --- NOVO: Toggle para Embeddings e IA ---
st.sidebar.subheader("Recursos Otimizados")
# Verifica se o arquivo de embeddings existe para habilitar/desabilitar
embeddings_file_exists = os.path.exists(EMBEDDED_SCHEMA_FILE)
if embeddings_file_exists:
    st.sidebar.toggle(
        "Usar Embeddings (Schema Otimizado)",
        key='use_embeddings',
        value=st.session_state.get('use_embeddings', False), # Garante que use o valor do estado
        help=f"Carrega `{EMBEDDED_SCHEMA_FILE}` para busca por similaridade e análise aprimorada. Pode levar um momento para atualizar.",
        on_change=handle_embedding_toggle # Define o callback
    )
else:
    st.sidebar.toggle(
        "Usar Embeddings (Schema Otimizado)",
        key='use_embeddings',
        help=f"Arquivo `{EMBEDDED_SCHEMA_FILE}` não encontrado. Execute `scripts/generate_embeddings.py` para habilitar.",
        value=False, # Força desligado
        disabled=True # Desabilita o toggle
    )
    if st.session_state.get('use_embeddings'): # Garante que o estado seja False se o arquivo sumir
        st.session_state.use_embeddings = False

# Toggle Ollama
if OLLAMA_AVAILABLE:
    st.sidebar.toggle("Habilitar Sugestões IA (Ollama)", 
                      key='ollama_enabled', 
                      value=st.session_state.get('ollama_enabled', False), # Usa valor do estado
                      help="Desabilitar pode melhorar a performance se não precisar das sugestões.")
else:
    st.sidebar.caption("Sugestões IA (Ollama) indisponíveis.")
# --- FIM: Toggles ---

st.sidebar.divider()

# --- Conteúdo Principal (Condicional ao Modo) ---

if app_mode == "Visão Geral":
    st.header("Visão Geral da Documentação e Contagens (Cache)")
    st.caption(f"Metadados de: `{METADATA_FILE}` | Schema de: `{TECHNICAL_SCHEMA_FILE}` | Contagens de: `{OVERVIEW_COUNTS_FILE}`")
    
    # --- NOVO: Botão para Executar Contagem --- 
    st.divider()
    st.subheader("Atualizar Contagem de Linhas")
    st.warning("Executar a contagem pode levar vários minutos dependendo do tamanho do banco.", icon="⏱️")
    
    if st.button("Executar Cálculo de Contagem Agora", key="run_count_script"):
        script_path = os.path.join("scripts", "calculate_row_counts.py")
        if not os.path.exists(script_path):
            st.error(f"Erro: Script de contagem não encontrado em '{script_path}'")
        else:
            st.info(f"Executando '{script_path}'... Acompanhe o progresso abaixo.")
            # Placeholder para a barra de progresso e status
            progress_bar = st.progress(0.0, text="Iniciando...")
            status_text = st.empty() # Para mostrar a tabela atual
            error_messages = [] # Para coletar erros do stderr
            final_stdout = ""
            
            try:
                python_executable = sys.executable 
                # NOVO: Construir comando com argumentos para credenciais
                cmd_list = [
                    python_executable, 
                    script_path,
                    "--db-path", db_path_for_ts,      # Passa o caminho do DB
                    "--db-user", db_user_for_ts,      # Passa o usuário
                    "--db-password", db_password_for_ts, # Passa a senha
                    "--db-charset", db_charset_for_ts   # Passa o charset
                ]
                logger.info(f"Executando comando: {' '.join(cmd_list[:5])} --db-password **** ...") # Log sem senha
                
                process = subprocess.Popen(
                    cmd_list, # CORRIGIDO: Usa a lista construída com argumentos
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    text=True, # Decodificar como texto
                    encoding='utf-8', # Usar UTF-8 explicitamente
                    errors='replace', # Substituir erros de decodificação
                    bufsize=1 # Modo de linha bufferizada para ler progresso
                )
                
                # Ler stdout linha por linha para progresso
                for line in process.stdout:
                    line = line.strip()
                    final_stdout += line + "\n" # Acumula stdout completo
                    logger.debug(f"Linha lida do script: {line}") # Log para depuração
                    if line.startswith("PROGRESS:"):
                        try:
                            parts = line.split(':')
                            progress_part = parts[1].split('/')
                            current = int(progress_part[0])
                            total = int(progress_part[1])
                            current_table = parts[2]
                            progress_value = float(current) / float(total) if total > 0 else 0.0
                            progress_text = f"Contando: {current_table} ({current}/{total})"
                            progress_bar.progress(progress_value, text=progress_text)
                            status_text.text(progress_text) # Atualiza texto abaixo da barra
                        except (IndexError, ValueError) as e:
                            logger.warning(f"Não foi possível parsear linha de progresso '{line}': {e}")
                    elif line.startswith("DONE:"):
                         logger.info(f"Script reportou conclusão: {line}")
                         break # Sai do loop de leitura de stdout
                    else:
                        # Pode logar outras linhas se necessário
                         logger.debug(f"Output não reconhecido do script: {line}")
                
                # Ler qualquer erro remanescente
                stderr = process.stderr.read()
                if stderr:
                    error_messages.append(stderr)
                    logger.error(f"Erro stderr do script de contagem:\n{stderr}")

                # Esperar o processo realmente terminar (importante)
                process.wait()
                status_text.empty() # Limpa o texto de status

                if process.returncode == 0:
                    progress_bar.progress(1.0, text="Contagem Concluída!")
                    st.success(f"Script '{script_path}' executado com sucesso!")
                    logger.info(f"Saída final stdout do script:\n{final_stdout}")
                    # Força o recarregamento das contagens e da página
                    load_overview_counts.clear()
                    st.session_state.overview_counts = load_overview_counts(OVERVIEW_COUNTS_FILE)
                    st.rerun()
                else:
                    progress_bar.progress(1.0, text="Erro na Contagem!")
                    st.error(f"Erro ao executar '{script_path}' (Código de saída: {process.returncode}).")
                    if error_messages:
                        st.text_area("Erro(s) Reportado(s) pelo Script:", "\n".join(error_messages), height=150)
                    # Mesmo com erro, tenta recarregar caso o arquivo tenha sido parcialmente escrito
                    load_overview_counts.clear()
                    st.session_state.overview_counts = load_overview_counts(OVERVIEW_COUNTS_FILE)
                    st.rerun() # Rerun para mostrar o estado atualizado do arquivo (mesmo com erro)

            except Exception as e:
                st.error(f"Erro inesperado ao tentar executar/ler o script: {e}")
                logger.exception("Erro ao executar subprocesso de contagem")
                progress_bar.progress(1.0, text="Erro Inesperado!") # Atualiza barra em caso de erro geral

    st.caption("Este botão executa um script que se conecta ao banco de dados, recalcula a contagem de linhas de todas as tabelas/views e salva o resultado no arquivo de cache (`overview_counts.json`). Pode ser demorado.")

    st.divider() # Separador antes da tabela
    # --- FIM: Botão para Executar Contagem ---
    
    st.info("A coluna 'Linhas (Cache)' mostra a última contagem salva no arquivo. Para atualizar, use o botão acima.")
    
    df_overview = generate_documentation_overview(
        technical_schema_data,
        metadata_dict,
        st.session_state.overview_counts
    )
    
    st.dataframe(df_overview, use_container_width=True)
    
    # Botão para recarregar apenas as contagens
    if st.button("Recarregar Contagens do Arquivo", key="refresh_counts_overview"):
        load_overview_counts.clear() # Limpa cache da função
        st.session_state.overview_counts = load_overview_counts(OVERVIEW_COUNTS_FILE)
        st.success("Contagens recarregadas.")
        st.rerun()
    st.caption("Este botão apenas recarrega os dados do último cálculo de contagem salvo no arquivo (`overview_counts.json`), sem se conectar ao banco. É rápido e útil se o arquivo foi atualizado externamente.")

elif app_mode == "Editar Metadados":
    st.header("Editor de Metadados")
    st.caption(f"Editando o arquivo: `{METADATA_FILE}` | Contexto técnico de: `{TECHNICAL_SCHEMA_FILE}`")
    
    # --- Seleção do Objeto --- (Lógica adaptada da versão anterior)
    all_technical_objects = {}
    for name, data in technical_schema_data.items():
        obj_type = data.get('object_type')
        if obj_type in ["TABLE", "VIEW"]: all_technical_objects[name] = obj_type

    if not all_technical_objects: st.error("Nenhuma tabela/view no schema técnico."); st.stop()

    object_types_available = sorted(list(set(all_technical_objects.values())))
    selected_type_display = st.radio("Filtrar por Tipo:", ["Todos"] + object_types_available, horizontal=True, index=0)

    if selected_type_display == "Todos": object_names = sorted(list(all_technical_objects.keys()))
    elif selected_type_display in object_types_available: object_names = sorted([name for name, type in all_technical_objects.items() if type == selected_type_display])
    else: object_names = []

    if not object_names: st.warning(f"Nenhum objeto do tipo '{selected_type_display}'."); selected_object = None
    else: selected_object = st.selectbox("Selecione o Objeto para Editar", object_names)

    st.divider()

    # --- Edição dos Metadados --- (Lógica existente, adaptada para garantir estrutura)
    if selected_object:
        selected_object_technical_type = all_technical_objects.get(selected_object)
        metadata_key_type = selected_object_technical_type + "S" if selected_object_technical_type else None
        tech_obj_data = technical_schema_data.get(selected_object)

        # Garante estrutura no metadata_dict
        if metadata_key_type and metadata_key_type not in metadata_dict: metadata_dict[metadata_key_type] = OrderedDict()
        if metadata_key_type and selected_object not in metadata_dict[metadata_key_type]:
             metadata_dict[metadata_key_type][selected_object] = OrderedDict({'description': '', 'COLUMNS': OrderedDict()})

        obj_data = metadata_dict.get(metadata_key_type, {}).get(selected_object, {})
        
        if not tech_obj_data: st.error(f"Dados técnicos não encontrados para '{selected_object}'"); 
        else:
            st.subheader(f"Editando: `{selected_object}` ({tech_obj_data.get('object_type', 'Desconhecido')})", divider='rainbow')
            # ... (Restante da lógica de edição com col1, col2, abas, etc. - SEM ALTERAÇÕES SIGNIFICATIVAS AQUI) ...
            # A lógica interna das abas de coluna (heurística, IA, propagar) já foi implementada
            # Apenas garantir que a referência `obj_data` e `metadata_key_type` estejam corretas
            
            # --- Bloco de Edição Objeto --- 
            col1_edit, col2_edit = st.columns([1, 2])
            with col1_edit:
                st.markdown("**Descrição do Objeto**")
                obj_desc_key = f"desc_{selected_object_technical_type}_{selected_object}"
                if "description" not in obj_data: obj_data["description"] = ""
                desc_obj_area, btn_ai_obj_area = st.columns([4, 1])
                with desc_obj_area:
                    new_obj_desc = st.text_area(
                        "Descrição Geral", value=obj_data.get("description", ""), 
                        key=obj_desc_key, height=100, label_visibility="collapsed"
                    )
                    obj_data["description"] = new_obj_desc
                with btn_ai_obj_area:
                    if st.button("Sugerir IA", key=f"btn_ai_obj_{selected_object}", use_container_width=True, disabled=not OLLAMA_AVAILABLE or not st.session_state.get('ollama_enabled', True)):
                        # Adapta prompt para objeto
                        prompt_object = f"Sugira descrição concisa pt-br para o objeto de banco de dados '{selected_object}' (tipo: {selected_object_technical_type}). Propósito? Responda só descrição."
                        suggestion = generate_ai_description(prompt_object)
                        if suggestion:
                             st.session_state.metadata[metadata_key_type][selected_object]['description'] = suggestion
                             st.rerun()
                             
            # --- Bloco de Edição Colunas --- 
            with col2_edit:
                st.markdown("**Descrição das Colunas**")
                obj_data.setdefault('COLUMNS', OrderedDict())
                columns_dict_meta = obj_data["COLUMNS"]
                technical_columns = tech_obj_data.get("columns", [])
                if not technical_columns: st.write("*Nenhuma coluna no schema técnico.*")
                else:
                    # MUDANÇA: Remover sorted() para usar a ordem física do DB
                    # technical_column_names = sorted([c['name'] for c in technical_columns if 'name' in c])
                    technical_column_names = [c['name'] for c in technical_columns if 'name' in c]
                    column_tabs = st.tabs(technical_column_names)
                    for i, col_name in enumerate(technical_column_names):
                        with column_tabs[i]:
                            # ... (Lógica interna das abas existente: info técnica, heurística, edição, IA, propagar) ...
                            # Garantir que col_meta_data seja pego/criado corretamente
                            if col_name not in columns_dict_meta: columns_dict_meta[col_name] = OrderedDict()
                            col_meta_data = columns_dict_meta[col_name]
                            if "description" not in col_meta_data: col_meta_data["description"] = ""
                            if "value_mapping_notes" not in col_meta_data: col_meta_data["value_mapping_notes"] = ""

                            # Obter dados técnicos da coluna
                            tech_col_data = next((c for c in technical_columns if c['name'] == col_name), None)
                            if not tech_col_data: st.warning(f"Dados técnicos não encontrados para coluna '{col_name}'."); continue # Pula esta aba

                            col_type = tech_col_data.get('type', 'N/A')
                            col_nullable = tech_col_data.get('nullable', True)
                            type_explanation = get_type_explanation(col_type)

                            # --- INÍCIO: Obter info de PK/FK ---
                            constraints = tech_obj_data.get('constraints', {})
                            key_info = []
                            # Check Primary Key
                            for pk in constraints.get('primary_key', []):
                                if col_name in pk.get('columns', []):
                                    key_info.append("🔑 PK")
                                    break # Sai do loop PK
                            # Check Foreign Keys (só se não for PK)
                            if not key_info: 
                                for fk in constraints.get('foreign_keys', []):
                                    if col_name in fk.get('columns', []):
                                        try:
                                            idx = fk['columns'].index(col_name)
                                            ref_table = fk.get('references_table', '?')
                                            # Garante que references_columns existe e tem o índice
                                            ref_cols = fk.get('references_columns', [])
                                            ref_col = ref_cols[idx] if idx < len(ref_cols) else '?'
                                            key_info.append(f"🔗 FK -> {ref_table}.{ref_col}")
                                        except (IndexError, ValueError, KeyError):
                                            key_info.append("🔗 FK (Erro ao mapear ref)")
                                        break # Sai do loop FK

                            key_info_str = f" | {' | '.join(key_info)}" if key_info else ""
                            # --- FIM: Obter info de PK/FK ---

                            # Exibe Tipo, Nulidade e Chaves
                            st.markdown(f"**Tipo:** `{col_type}` {type_explanation} | **Anulável:** {'Sim' if col_nullable else 'Não'}{key_info_str}")
                            st.markdown("--- Descrição --- ")

                            # --- Garantir Inicialização das Variáveis de Heurística --- # (CORREÇÃO)
                            current_col_desc_saved = col_meta_data.get('description', '').strip()
                            description_value_to_display = current_col_desc_saved
                            current_col_notes_saved = col_meta_data.get('value_mapping_notes', '').strip()
                            notes_value_to_display = current_col_notes_saved
                            heuristic_desc_source = None
                            heuristic_notes_source = None
                            # --- Fim Inicialização ---

                            # Só busca heurística se um dos campos estiver vazio
                            if not current_col_desc_saved or not current_col_notes_saved:
                                # --- DEBUG CALL SITE --- #
                                logger.debug(f"[CALL SITE] Calling find_existing_info for {selected_object}.{col_name}")
                                logger.debug(f"[CALL SITE] metadata_dict type: {type(metadata_dict)}, value[:200]: '{str(metadata_dict)[:200]}...'")
                                # --- END DEBUG CALL SITE ---
                                suggested_desc, desc_source_from_func, suggested_notes, notes_source_from_func = find_existing_info(
                                    metadata_dict, technical_schema_data, selected_object, col_name
                                )

                                # Preenche descrição se vazia e sugestão encontrada
                                if not current_col_desc_saved and suggested_desc:
                                    description_value_to_display = suggested_desc
                                    heuristic_desc_source = desc_source_from_func # Usa variável renomeada
                                    logger.info(f"Preenchendo DESC '{selected_object}.{col_name}' com sugestão via {desc_source_from_func}")

                                # Preenche notas se vazias e sugestão encontrada
                                if not current_col_notes_saved and suggested_notes:
                                    notes_value_to_display = suggested_notes
                                    heuristic_notes_source = notes_source_from_func # Usa variável renomeada
                                    logger.info(f"Preenchendo NOTES '{selected_object}.{col_name}' com sugestão via {notes_source_from_func}")

                            # Exibe caption para descrição sugerida
                            if heuristic_desc_source:
                                st.caption(f"ℹ️ Sugestão de DESCRIÇÃO preenchida ({heuristic_desc_source}). Edite abaixo.")

                            # Heurística e Área de Texto (Código existente, adaptado para usar estado da sessão)

                            # Heurística, Desc Area, Botões IA/Propagar
                            col_desc_key = f"desc_{selected_object_technical_type}_{selected_object}_{col_name}"
                            # --- INÍCIO: Código de Text Area para Descrição e Notas (Re-inserido) ---
                            current_col_desc_saved = col_meta_data.get('description', '').strip()
                            description_value_to_display = current_col_desc_saved
                            heuristic_source = None

                            if not current_col_desc_saved:
                                existing_desc, source, notes, notes_source = find_existing_info(metadata_dict, technical_schema_data, selected_object, col_name)
                                if existing_desc:
                                    description_value_to_display = existing_desc
                                    heuristic_source = source
                                    logger.info(f"Preenchendo '{selected_object}.{col_name}' com sugestão via {source}")

                                    st.caption(f"ℹ️ Sugestão preenchida ({heuristic_source}). Pode editar abaixo.")

                            # --- NOVO: Busca por Similaridade FAISS ---
                            col_embedding_data = tech_col_data.get('embedding') # Usa tech_col_data que já temos
                            if st.session_state.get('faiss_index') and col_embedding_data:
                                if st.button("🔍 Buscar Descrições Similares (FAISS)", key=f"faiss_search_{selected_object}_{col_name}"):
                                    # Garantir que o embedding seja um array numpy float32
                                    try:
                                        target_embedding = np.array(col_embedding_data).astype('float32')
                                        if target_embedding.shape[0] != EMBEDDING_DIMENSION:
                                            st.error(f"Erro: Dimensão do embedding ({target_embedding.shape[0]}) diferente da esperada ({EMBEDDING_DIMENSION}). Verifique os embeddings.")
                                            target_embedding = None # Impede a busca
                                    except Exception as e:
                                        st.error(f"Erro ao converter embedding para busca: {e}")
                                        target_embedding = None

                                    if target_embedding is not None:
                                        with st.spinner("Buscando colunas similares..."):
                                            similar_cols = find_similar_columns(
                                                st.session_state.faiss_index,
                                                st.session_state.technical_schema, # Usar schema técnico para obter nomes e descrições
                                                st.session_state.index_to_key_map,
                                                target_embedding,
                                                k=5 # Buscar as 5 mais similares com descrição
                                            )
                                        if similar_cols:
                                            with st.expander("💡 Colunas Similares Encontradas", expanded=True):
                                                for sim_col in similar_cols:
                                                    # Usar markdown para melhor formatação
                                                    st.markdown(f"**`{sim_col['table']}.{sim_col['column']}`**")
                                                    # Adicionar distância formatada
                                                    st.caption(f"(Distância L2²: {sim_col['distance']:.4f})")
                                                    # Usar st.markdown ou st.text_area para a descrição, dependendo do tamanho
                                                    st.markdown(f"> _{sim_col['description']}_")
                                                    st.markdown("---") # Separador visual
                                        else:
                                            st.info("Nenhuma coluna similar com descrição preenchida foi encontrada.")
                            elif not col_embedding_data:
                                st.caption("_(Sem embedding disponível para esta coluna para busca por similaridade)_")
                            elif not st.session_state.get('faiss_index'):
                                st.caption("_(Índice FAISS não disponível para busca por similaridade)_")
                            # --- FIM Busca FAISS ---

                            # Layout Descrição + Botões IA/Propagar
                            desc_col_area, btns_col_area = st.columns([4, 1])
                            with desc_col_area:
                                current_value = st.text_area(
                                    f"Descrição Coluna `{col_name}`", # Label atualizado
                                    value=description_value_to_display, # Valor inicial pode ser heurístico
                                    key=col_desc_key,
                                    height=75,
                                    label_visibility="collapsed", # Esconde label repetido
                                    help="Descreva o que esta coluna representa."
                                )
                                # Atualiza estado SE diferente do que foi carregado/sugerido inicialmente
                                if current_value != description_value_to_display:
                                    col_meta_data["description"] = current_value
                                    col_meta_data.pop('source_description', None) # Remove marcador se editado manualmente
                                elif heuristic_source and not current_col_desc_saved: # Se heuristica foi usada e campo estava vazio, salva heuristica
                                    col_meta_data["description"] = description_value_to_display
                                    col_meta_data['source_description'] = f"heuristic: {heuristic_source}" # Adiciona marcador
                                # else: # Garante que o valor salvo seja mantido se não editado - JÁ FEITO pela inicialização
                                #    col_meta_data["description"] = current_col_desc_saved

                            with btns_col_area:
                                if st.button("Sugerir IA", key=f"btn_ai_col_{col_name}", use_container_width=True, disabled=not OLLAMA_AVAILABLE or not st.session_state.get('ollama_enabled', True)):
                                    prompt_column = (f"Sugira descrição concisa pt-br para coluna '{col_name}' ({col_type}) do objeto '{selected_object}'. Significado? Responda só descrição.")
                                    suggestion = generate_ai_description(prompt_column)
                                    if suggestion:
                                        st.session_state.metadata[metadata_key_type][selected_object]['COLUMNS'][col_name]['description'] = suggestion
                                        st.rerun()
                                
                                # Botão Propagar
                                description_to_propagate = col_meta_data.get('description', '').strip()
                                notes_to_propagate = col_meta_data.get('value_mapping_notes', '').strip()
                                if description_to_propagate:
                                    if st.button("Propagar 🔁", key=f"propagate_{col_name}", help="Preenche esta descrição e notas em colunas vazias equivalentes", use_container_width=True):
                                        source_concept = get_column_concept(technical_schema_data, selected_object, col_name)
                                        propagated_count = 0
                                        # Iterar sobre todos os objetos e colunas nos metadados para propagar
                                        for obj_type_prop in st.session_state.metadata:
                                            if obj_type_prop == "_GLOBAL_CONTEXT": continue
                                            for obj_name_prop, obj_meta_prop in st.session_state.metadata[obj_type_prop].items():
                                                if obj_name_prop not in technical_schema_data: continue
                                                if 'COLUMNS' not in obj_meta_prop: continue
                                                for col_name_prop, col_meta_prop_target in obj_meta_prop['COLUMNS'].items(): # Renomeado para evitar conflito
                                                    if obj_name_prop == selected_object and col_name_prop == col_name: continue
                                                    # MUDANÇA: Condição baseada apenas na descrição vazia
                                                    is_target_desc_empty = not col_meta_prop_target.get('description', '').strip()
                                                    if is_target_desc_empty:
                                                        target_concept = get_column_concept(technical_schema_data, obj_name_prop, col_name_prop)
                                                        if target_concept == source_concept:
                                                            # MUDANÇA: Propaga descrição E notas
                                                            st.session_state.metadata[obj_type_prop][obj_name_prop]['COLUMNS'][col_name_prop]['description'] = description_to_propagate
                                                            st.session_state.metadata[obj_type_prop][obj_name_prop]['COLUMNS'][col_name_prop]['value_mapping_notes'] = notes_to_propagate
                                                            propagated_count += 1
                                        if propagated_count > 0:
                                            # MUDANÇA: Mensagem atualizada
                                            st.toast(f"Descrição e Notas propagadas para {propagated_count} coluna(s) com descrição vazia.", icon="✅")
                                        else: 
                                            st.toast("Nenhuma coluna correspondente com descrição vazia encontrada.", icon="ℹ️")

                            # Notas de Mapeamento
                            st.markdown("--- Notas de Mapeamento --- ")
                            # Exibe caption para notas sugeridas (ANTES da área de texto)
                            if heuristic_notes_source:
                                st.caption(f"ℹ️ Sugestão de NOTAS preenchida ({heuristic_notes_source}). Edite abaixo.")

                            col_notes_key = f"notes_{selected_object_technical_type}_{selected_object}_{col_name}"
                            current_notes_value = st.text_area(
                                f"Notas Mapeamento (`{col_name}`)",
                                value=col_meta_data.get("value_mapping_notes", ""),
                                key=col_notes_key,
                                height=75,
                                label_visibility="collapsed", # Esconde label repetido
                                help="Explique valores específicos (ex: 1=Ativo) ou formatos."
                            )
                            # Atualiza estado SE diferente do que foi carregado/sugerido inicialmente
                            if current_notes_value != notes_value_to_display:
                                col_meta_data["value_mapping_notes"] = current_notes_value
                                col_meta_data.pop('source_notes', None) # Remove marcador se editado manualmente
                            elif heuristic_notes_source and not current_col_notes_saved: # CORRIGIDO: Usa heuristic_notes_source e current_col_notes_saved
                                col_meta_data["value_mapping_notes"] = notes_value_to_display # CORRIGIDO: Usa notes_value_to_display
                                col_meta_data['source_notes'] = f"heuristic: {heuristic_notes_source}" # CORRIGIDO: Usa heuristic_notes_source
                            # else: # Garante que o valor salvo seja mantido se não editado - JÁ FEITO pela inicialização
                            #     col_meta_data["value_mapping_notes"] = current_col_notes_saved

                            # --- FIM: Código de Text Area para Descrição e Notas (Re-inserido) ---

            st.divider() # Separador antes da pré-visualização

            # --- NOVO: Seção de Pré-visualização de Dados ---
            with st.expander("👁️ Pré-Visualização de Dados", expanded=False):
                num_rows_to_fetch = st.number_input(
                    "Número de linhas para buscar:",
                    min_value=1,
                    value=10,
                    step=1,
                    key=f"num_rows_{selected_object}",
                    help="Digite o número de linhas desejado. Valores muito altos podem impactar o desempenho."
                )
                st.caption("⚠️ Solicitar muitas linhas pode tornar a aplicação lenta ou consumir muita memória.")

                col_load, col_export_txt = st.columns(2) # Colunas para botões

                with col_load:
                    if st.button("Carregar Amostra na Tela", key=f"load_sample_{selected_object}"):
                        # Limpa estado de exportação anterior (Excel e TXT)
                        st.session_state[f'excel_export_data_{selected_object}'] = None
                        st.session_state[f'excel_export_filename_{selected_object}'] = None
                        st.session_state[f'excel_export_error_{selected_object}'] = None
                        st.session_state[f'txt_export_bytes_{selected_object}'] = None
                        st.session_state[f'txt_export_filename_{selected_object}'] = None
                        st.session_state[f'txt_export_error_{selected_object}'] = None
                        # Busca dados para exibir
                        sample_data_display = fetch_sample_data(
                            db_path_for_ts, db_user_for_ts, db_password_for_ts,
                            db_charset_for_ts, selected_object, num_rows_to_fetch
                        )
                        # Armazena no estado para exibição persistente
                        st.session_state[f'sample_data_display_{selected_object}'] = sample_data_display

                # Exibe o DataFrame ou erro armazenado no estado
                sample_data_result = st.session_state.get(f'sample_data_display_{selected_object}')
                if isinstance(sample_data_result, pd.DataFrame):
                    if sample_data_result.empty:
                        st.info(f"Nenhuma amostra de dados retornada para '{selected_object}'. A tabela pode estar vazia.")
                    else:
                        st.dataframe(sample_data_result, use_container_width=True)
                elif isinstance(sample_data_result, str): # Se for uma string de erro
                    st.error(f"Falha ao carregar amostra: {sample_data_result}")

                # --- NOVO: Botão Exportar TXT ---
                with col_export_txt:
                    if st.button("Gerar Amostra para Exportar (TXT)", key=f"generate_export_txt_{selected_object}"):
                        logger.info(f"Gerando amostra TXT de {num_rows_to_fetch} linhas para exportar de {selected_object}...")
                        # Limpa estado de exportação anterior (Excel e TXT) para evitar mostrar botões antigos
                        st.session_state[f'excel_export_data_{selected_object}'] = None
                        st.session_state[f'excel_export_filename_{selected_object}'] = None
                        st.session_state[f'excel_export_error_{selected_object}'] = None
                        st.session_state[f'txt_export_bytes_{selected_object}'] = None
                        st.session_state[f'txt_export_filename_{selected_object}'] = None
                        st.session_state[f'txt_export_error_{selected_object}'] = None

                        export_data_txt = fetch_sample_data(
                            db_path_for_ts, db_user_for_ts, db_password_for_ts,
                            db_charset_for_ts, selected_object, num_rows_to_fetch
                        )

                        if isinstance(export_data_txt, pd.DataFrame):
                            if export_data_txt.empty:
                                st.warning(f"Nenhum dado retornado para '{selected_object}'. O arquivo TXT não será gerado.")
                            else:
                                try:
                                    # Trata BLOBs para TXT também
                                    df_to_export_txt = export_data_txt.copy()
                                    for col in df_to_export_txt.columns:
                                        if df_to_export_txt[col].dtype == 'object':
                                            first_non_null = df_to_export_txt[col].dropna().iloc[0] if not df_to_export_txt[col].dropna().empty else None
                                            if isinstance(first_non_null, bytes):
                                                df_to_export_txt[col] = df_to_export_txt[col].apply(lambda x: "[BLOB Data]" if isinstance(x, bytes) else x)

                                    # Converte para string formatada
                                    txt_string = df_to_export_txt.to_string(index=False)
                                    # Codifica para bytes
                                    st.session_state[f'txt_export_bytes_{selected_object}'] = txt_string.encode('utf-8')
                                    st.session_state[f'txt_export_filename_{selected_object}'] = f"amostra_{selected_object}.txt"
                                    logger.info(f"Amostra TXT para {selected_object} gerada e pronta para download.")
                                except Exception as e:
                                    logger.exception("Erro ao gerar o arquivo TXT em memória.")
                                    st.session_state[f'txt_export_error_{selected_object}'] = f"Erro ao gerar TXT: {e}"
                        else: # Erro retornado por fetch_sample_data
                            st.session_state[f'txt_export_error_{selected_object}'] = f"Falha ao buscar dados para exportar TXT: {export_data_txt}"

                # Exibir botão de download TXT ou erro (fora do if do botão gerar)
                if st.session_state.get(f'txt_export_bytes_{selected_object}') and st.session_state.get(f'txt_export_filename_{selected_object}'):
                    with col_export_txt: # Coloca o botão de download na mesma coluna
                         st.download_button(
                              label="⬇️ Baixar Amostra TXT",
                              data=st.session_state[f'txt_export_bytes_{selected_object}'],
                              file_name=st.session_state[f'txt_export_filename_{selected_object}'],
                              mime="text/plain",
                              key=f"download_txt_{selected_object}",
                              use_container_width=True # Faz o botão ocupar a coluna
                         )
                    # Limpa estado após exibir o botão (para não reaparecer automaticamente)
                    # Considerar se realmente quer limpar aqui ou deixar baixar múltiplas vezes
                    # st.session_state[f'txt_export_bytes_{selected_object}'] = None
                    # st.session_state[f'txt_export_filename_{selected_object}'] = None
                elif st.session_state.get(f'txt_export_error_{selected_object}'):
                     with col_export_txt: # Exibe erro na coluna do botão TXT
                         st.error(st.session_state[f'txt_export_error_{selected_object}'])
                     # Limpa erro após exibir
                     # st.session_state[f'txt_export_error_{selected_object}'] = None

            # --- FIM: Seção de Pré-visualização de Dados ---

            # --- NOVO: Seção de Exportação Excel ---
            st.divider()
            st.subheader("📤 Exportar Amostra de Dados (Excel)")
            num_rows_export = st.number_input(
                "Número de linhas para exportar:",
                min_value=1,
                value=100, # Default maior para exportação
                step=10,
                key=f"num_rows_export_{selected_object}",
                help="Defina quantas linhas da amostra serão incluídas no arquivo Excel."
            )
            
            if st.button("Gerar Amostra para Exportar", key=f"generate_export_{selected_object}"):
                logger.info(f"Gerando amostra de {num_rows_export} linhas para exportar de {selected_object}...")
                # Usa os mesmos parâmetros de conexão da busca de timestamp
                export_data = fetch_sample_data(
                    db_path_for_ts,
                    db_user_for_ts,
                    db_password_for_ts,
                    db_charset_for_ts,
                    selected_object, 
                    num_rows_export
                )
                
                if isinstance(export_data, pd.DataFrame):
                    if export_data.empty:
                        st.warning(f"Nenhum dado retornado para '{selected_object}'. O arquivo Excel não será gerado.")
                        st.session_state['excel_export_data'] = None
                        st.session_state['excel_export_filename'] = None
                        st.session_state['excel_export_error'] = None
                    else:
                        try:
                            # --- INÍCIO: Tratamento de Tipos para Excel ---
                            df_to_export = export_data.copy() # Trabalhar com cópia
                            for col in df_to_export.columns:
                                # Verifica se a coluna é do tipo objeto e se contém bytes (indicativo de BLOB)
                                if df_to_export[col].dtype == 'object':
                                    # Checa o primeiro valor não nulo para ver se é bytes
                                    first_non_null = df_to_export[col].dropna().iloc[0] if not df_to_export[col].dropna().empty else None
                                    if isinstance(first_non_null, bytes):
                                        logger.info(f"Coluna '{col}' detectada como BLOB, substituindo por placeholder.")
                                        # Aplica a substituição para todos os valores bytes na coluna
                                        df_to_export[col] = df_to_export[col].apply(lambda x: "[BLOB Data]" if isinstance(x, bytes) else x)
                            # --- FIM: Tratamento de Tipos para Excel ---

                            # Criar buffer de bytes em memória
                            output = io.BytesIO()
                            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                                df_to_export.to_excel(writer, index=False, sheet_name=selected_object[:31]) # Usa o DataFrame modificado
                            # Salva os bytes e o nome do arquivo no estado
                            st.session_state['excel_export_data'] = output.getvalue()
                            st.session_state['excel_export_filename'] = f"amostra_{selected_object}.xlsx"
                            st.session_state['excel_export_error'] = None
                            logger.info(f"Amostra para {selected_object} gerada e pronta para download.")
                        except Exception as e:
                            logger.exception("Erro ao gerar o arquivo Excel em memória.")
                            st.session_state['excel_export_data'] = None
                            st.session_state['excel_export_filename'] = None
                            st.session_state['excel_export_error'] = f"Erro ao gerar Excel: {e}"
                else: # Erro retornado por fetch_sample_data
                    st.session_state['excel_export_data'] = None
                    st.session_state['excel_export_filename'] = None
                    st.session_state['excel_export_error'] = f"Falha ao buscar dados para exportar: {export_data}"
            
            # Exibir botão de download ou erro (fora do if do botão gerar)
            if st.session_state.get('excel_export_data') and st.session_state.get('excel_export_filename'):
                st.download_button(
                    label="⬇️ Baixar Arquivo Excel",
                    data=st.session_state['excel_export_data'],
                    file_name=st.session_state['excel_export_filename'],
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"download_excel_{selected_object}"
                )
                # Limpa estado após exibir o botão (para não reaparecer)
                # st.session_state['excel_export_data'] = None # Comentar para permitir múltiplos downloads? Não, melhor limpar.
                # st.session_state['excel_export_filename'] = None
                st.session_state['excel_export_data'] = None # Garante limpeza após tentativa de download
                st.session_state['excel_export_filename'] = None
            elif st.session_state.get('excel_export_error'):
                st.error(st.session_state['excel_export_error'])
                # Limpa erro após exibir
                # st.session_state['excel_export_error'] = None
                st.session_state['excel_export_error'] = None
            
            # --- FIM: Seção de Exportação Excel ---

            # --- Botão Salvar Edição --- 
            st.divider()
            if st.button("💾 Salvar Alterações nos Metadados", type="primary", key="save_edit_mode"):
                # NOVO: Comparar antes de salvar
                new_desc_count, new_notes_count = 0, 0
                if 'initial_metadata' in st.session_state:
                    new_desc_count, new_notes_count = compare_metadata_changes(
                        st.session_state.initial_metadata,
                        st.session_state.metadata
                    )
                else:
                    logger.warning("Estado inicial dos metadados não encontrado para comparação.")

                if save_metadata(st.session_state.metadata, METADATA_FILE):
                    # NOVO: Mensagem de sucesso com contadores
                    success_message = f"Metadados salvos com sucesso em `{METADATA_FILE}`!"
                    if new_desc_count > 0 or new_notes_count > 0:
                        success_message += f" ({new_desc_count} novas descrições, {new_notes_count} novas notas adicionadas nesta sessão)"
                    st.success(success_message, icon="✅")

                    try:
                        load_metadata.clear()
                        logger.info("Cache de metadados limpo após salvar.")
                        # NOVO: Atualizar estado inicial após salvar com sucesso
                        st.session_state.initial_metadata = copy.deepcopy(st.session_state.metadata)
                        logger.info("Estado inicial dos metadados atualizado após salvar.")
                        # ATUALIZA O TEMPO DO ÚLTIMO SAVE
                        st.session_state.last_save_time = time.time()
                        logger.info(f"Tempo do último salvamento atualizado para: {st.session_state.last_save_time}")
                    except Exception as e:
                        logger.warning(f"Erro ao limpar cache ou atualizar estado inicial: {e}")
                else:
                    st.error("Falha ao salvar metadados.")

    else:
        st.info("Selecione um objeto para editar seus metadados.")

# --- NOVO: Modo Análise ---
elif app_mode == "Análise":
    st.header("🔎 Análise Estrutural e de Referências do Schema")
    st.caption(f"Analisando informações de: `{TECHNICAL_SCHEMA_FILE}`")
    st.divider()

    # Recupera a análise estrutural do cache
    composite_pk_tables, junction_tables, composite_fk_details, column_roles = st.session_state.key_analysis

    # --- Seção: Colunas Mais Referenciadas (com Importância) ---
    st.subheader("Colunas Mais Referenciadas por FKs (com Prioridade)")
    if technical_schema_data and 'fk_reference_counts' in technical_schema_data:
        fk_counts = technical_schema_data['fk_reference_counts']
        if not fk_counts:
            st.info("Nenhuma contagem de referência de FK encontrada no schema técnico.")
        else:
            fk_list = []
            processed_columns = set()
            # Primeiro, processa colunas com contagem de referência
            for key, count in fk_counts.items():
                try:
                    table_name, column_name = key.split('.', 1)
                    if not table_name or not column_name: continue

                    role_info = column_roles.get((table_name, column_name), {'role': 'Normal', 'importance_level': 'Baixa'})
                    # Ajuste: Usar technical_schema_data que já temos acesso
                    metadata_info = technical_schema_data.get(table_name, {}).get('columns', [])
                    col_data = next((col for col in metadata_info if col.get('name') == column_name), None)
                    
                    # Correção: Verificar None antes de strip()
                    col_desc = col_data.get('business_description') if col_data else None
                    has_description = bool(col_desc.strip()) if col_desc else False
                    col_notes = col_data.get('value_mapping_notes') if col_data else None
                    has_notes = bool(col_notes.strip()) if col_notes else False

                    fk_list.append({
                        "Importância": role_info['importance_level'],
                        "Tabela": table_name,
                        "Coluna": column_name,
                        "Função Chave": role_info['role'],
                        "Nº Referências FK": count,
                        "Tem Descrição?": "✅" if has_description else "❌",
                        "Tem Notas?": "✅" if has_notes else "❌"
                    })
                    processed_columns.add((table_name, column_name))
                except ValueError:
                    logger.warning(f"Formato inválido na chave fk_reference_counts: {key}")

            # Adiciona outras colunas importantes (PK Comp, PK/FK) que não foram referenciadas
            for (table_name, column_name), role_info in column_roles.items():
                if (table_name, column_name) not in processed_columns and role_info['importance_level'] in ['Máxima', 'Alta']:
                    # Ajuste: Usar technical_schema_data
                    metadata_info = technical_schema_data.get(table_name, {}).get('columns', [])
                    col_data = next((col for col in metadata_info if col.get('name') == column_name), None)
                    
                    # Correção: Verificar None antes de strip()
                    col_desc = col_data.get('business_description') if col_data else None
                    has_description = bool(col_desc.strip()) if col_desc else False
                    col_notes = col_data.get('value_mapping_notes') if col_data else None
                    has_notes = bool(col_notes.strip()) if col_notes else False
                    
                    fk_list.append({
                        "Importância": role_info['importance_level'],
                        "Tabela": table_name,
                        "Coluna": column_name,
                        "Função Chave": role_info['role'],
                        "Nº Referências FK": 0, # Não foi referenciada diretamente
                        "Tem Descrição?": "✅" if has_description else "❌",
                        "Tem Notas?": "✅" if has_notes else "❌"
                    })

            if not fk_list:
                 st.warning("Não foi possível processar as colunas para análise.")
            else:
                # Ordenar primariamente por Importância (custom order), depois por Referências
                importance_order = {'Máxima': 0, 'Alta': 1, 'Média': 2, 'Baixa': 3}
                fk_list_sorted = sorted(fk_list,
                                        key=lambda x: (importance_order.get(x["Importância"], 99), -x["Nº Referências FK"]),
                                        reverse=False) # Ordem crescente de importância (Máxima primeiro)

                df_fk_analysis = pd.DataFrame(fk_list_sorted)
                cols_ordered_analysis = ["Importância", "Tabela", "Coluna", "Função Chave", "Nº Referências FK", "Tem Descrição?", "Tem Notas?"]
                df_fk_analysis = df_fk_analysis[[col for col in cols_ordered_analysis if col in df_fk_analysis.columns]]

                num_to_show_analysis = st.slider(
                    "Mostrar Top N colunas por importância/referência:",
                    min_value=5,
                    max_value=len(df_fk_analysis),
                    value=min(30, len(df_fk_analysis)), # Aumenta o padrão
                    step=5,
                    key="slider_analysis_importance"
                )
                st.dataframe(df_fk_analysis.head(num_to_show_analysis), use_container_width=True)
                with st.expander("Mostrar todas as colunas analisadas"):
                     st.dataframe(df_fk_analysis, use_container_width=True)

    else:
        st.error("Dados de contagem de referência de FK ('fk_reference_counts') não encontrados no arquivo de schema técnico.")
        st.info(f"Certifique-se de que o script `scripts/extract_schema.py` ou `scripts/merge_schema_data.py` foi executado e gerou o arquivo `{TECHNICAL_SCHEMA_FILE}` corretamente.")

    st.divider()

    # --- Seção: Tabelas com PK Composta ---
    st.subheader("Tabelas com Chave Primária Composta")
    if composite_pk_tables:
        pk_comp_list = []
        for table, cols in composite_pk_tables.items():
            pk_comp_list.append({"Tabela": table, "Colunas PK": ", ".join(cols)})
        df_pk_comp = pd.DataFrame(pk_comp_list).sort_values(by="Tabela")
        st.dataframe(df_pk_comp, use_container_width=True)
    else:
        st.info("Nenhuma tabela com chave primária composta identificada.")

    st.divider()

    # --- Seção: Tabelas de Junção ---
    st.subheader("Tabelas de Ligação (Junção)")
    if junction_tables:
        junction_list = []
        for table, details in junction_tables.items():
             junction_list.append({"Tabela": table, "Detalhes FKs na PK": "; ".join(details)})
        df_junction = pd.DataFrame(junction_list).sort_values(by="Tabela")
        st.dataframe(df_junction, use_container_width=True)
    else:
        st.info("Nenhuma tabela de junção identificada (PK composta totalmente por FKs).")

    st.divider()

    # --- Seção: Colunas em FK Composta ---
    st.subheader("Colunas em Chaves Estrangeiras Compostas")
    if composite_fk_details:
        fk_comp_list = []
        for (table, column), detail in composite_fk_details.items():
             fk_comp_list.append({"Tabela": table, "Coluna": column, "Referência (parte de FK Comp.)": detail})
        df_fk_comp = pd.DataFrame(fk_comp_list).sort_values(by=["Tabela", "Coluna"])
        st.dataframe(df_fk_comp, use_container_width=True)
    else:
        st.info("Nenhuma coluna identificada como parte de chave estrangeira composta.")

# --- NOVO: Modo Chat com Schema ---
elif app_mode == "Chat com Schema":
    st.header("💬 Chat com Schema")
    st.caption("Faça perguntas sobre o schema documentado. O assistente usará os metadados como contexto.")

    if not OLLAMA_AVAILABLE:
        st.error("Funcionalidade de Chat indisponível. Integração Ollama não carregada.")
    else:
        # Inicializa histórico de chat e feedback (carregando do arquivo)
        if "messages" not in st.session_state:
            st.session_state.messages = load_json(CHAT_HISTORY_FILE, [])
        if "feedback_log" not in st.session_state:
            st.session_state.feedback_log = load_json(CHAT_FEEDBACK_FILE, [])
            # Cria um set de IDs com feedback para busca rápida
            st.session_state.feedback_ids = {fb['message_id'] for fb in st.session_state.feedback_log}

        # Exibe mensagens do histórico
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
                # Adiciona botões de feedback para mensagens do assistente
                if message["role"] == "assistant":
                    message_id = message.get("message_id") # Pega o ID da mensagem
                    if message_id:
                        feedback_given = message_id in st.session_state.get('feedback_ids', set())
                        # Cria colunas para os botões
                        fb_cols = st.columns(3)
                        ratings = ["Bom", "Médio", "Ruim"]
                        icons = ["👍", "🤔", "👎"] # Ou use texto direto
                        for i, rating in enumerate(ratings):
                            with fb_cols[i]:
                                button_key = f"feedback_{message_id}_{rating}"
                                if st.button(icons[i], key=button_key, help=rating, disabled=feedback_given, use_container_width=True):
                                    if not feedback_given:
                                        new_feedback = {"message_id": message_id, "rating": rating, "timestamp": time.time()}
                                        st.session_state.feedback_log.append(new_feedback)
                                        st.session_state.feedback_ids.add(message_id) # Atualiza o set
                                        if save_json(st.session_state.feedback_log, CHAT_FEEDBACK_FILE):
                                            st.toast(f"Feedback '{rating}' registrado!", icon="✍️")
                                        else:
                                            st.toast("Erro ao salvar feedback!", icon="❌")
                                        st.rerun()

        # Input do usuário
        if prompt := st.chat_input("Qual sua dúvida sobre o schema?"):
            # Adiciona e exibe a mensagem do usuário
            user_message_id = str(uuid.uuid4()) # Gera ID único
            user_message = {"role": "user", "content": prompt, "message_id": user_message_id}
            st.session_state.messages.append(user_message)
            # Salva histórico APÓS adicionar a mensagem do usuário
            # save_json(st.session_state.messages, CHAT_HISTORY_FILE)
            
            with st.chat_message("user"):
                st.markdown(prompt)

            # Prepara para a resposta do assistente
            with st.chat_message("assistant"):
                message_placeholder = st.empty()
                message_placeholder.markdown("Pensando... 🧠")
                
                # --- Lógica de Coleta de Contexto ---
                context_parts = []
                max_context_tokens = 3000 # Limite aproximado (ajustar conforme necessário)
                current_context_tokens = 0
                context_limit_reached = False # Flag para parar de adicionar

                # REMOVIDA função interna add_to_context

                # 1. Contexto Global
                global_context = st.session_state.metadata.get("_GLOBAL_CONTEXT", "")
                if global_context and not context_limit_reached:
                    tokens = len(global_context.split())
                    if current_context_tokens + tokens < max_context_tokens:
                         context_parts.append(f"--- Contexto Geral ---\n{global_context}")
                         current_context_tokens += tokens
                    else:
                        context_limit_reached = True
                        logger.warning("Limite de contexto atingido ao adicionar Contexto Geral.")

                # 2. Busca por Palavra-chave (Nomes de Tabelas/Views/Colunas)
                found_tables = set()
                prompt_lower = prompt.lower()
                for obj_name, obj_data in technical_schema_data.items():
                    if context_limit_reached: break # Para loop externo se limite atingido
                    
                    obj_type = obj_data.get('object_type', 'OBJECT')
                    obj_meta = st.session_state.metadata.get(obj_type + "S", {}).get(obj_name, {})
                    table_context_to_add = []
                    table_tokens = 0

                    # Verifica nome da tabela/view E descrição da tabela
                    found_table_by_name = obj_name.lower() in prompt_lower
                    table_desc = obj_meta.get("description", "").strip()
                    
                    if found_table_by_name or table_desc: # Se o nome foi mencionado OU tem descrição
                        header = f"--- {obj_type.capitalize()}: {obj_name} ---"
                        table_context_to_add.append(header)
                        table_tokens += len(header.split())
                        if table_desc:
                             desc_text = f"Descrição: {table_desc}"
                             table_context_to_add.append(desc_text)
                             table_tokens += len(desc_text.split())
                        found_tables.add(obj_name) # Marca como encontrada

                    # Verifica nomes das colunas dentro desta tabela/view
                    column_context_parts = []
                    column_tokens = 0
                    for col_data in obj_data.get('columns', []):
                        if context_limit_reached: break # Para loop interno
                        
                        col_name = col_data.get('name')
                        if col_name and col_name.lower() in prompt_lower:
                             col_meta = obj_meta.get("COLUMNS", {}).get(col_name, {})
                             col_desc = col_meta.get("description", "").strip()
                             col_notes = col_meta.get("value_mapping_notes", "").strip()
                             
                             if col_desc or col_notes: # Só adiciona se tiver info útil
                                 col_str_parts = [f"  Coluna: {col_name}"]
                                 col_part_tokens = len(col_name.split()) + 2
                                 if col_desc:
                                     desc_text = f"    Descrição: {col_desc}"
                                     col_str_parts.append(desc_text)
                                     col_part_tokens += len(desc_text.split())
                                 if col_notes:
                                     notes_text = f"    Notas: {col_notes}"
                                     col_str_parts.append(notes_text)
                                     col_part_tokens += len(notes_text.split())
                                 
                                 # Verifica se esta coluna cabe
                                 if current_context_tokens + table_tokens + column_tokens + col_part_tokens < max_context_tokens:
                                      column_context_parts.extend(col_str_parts)
                                      column_tokens += col_part_tokens
                                 else:
                                      context_limit_reached = True
                                      logger.warning(f"Limite de contexto atingido ao adicionar Coluna '{col_name}'.")
                                      break # Para de processar colunas desta tabela
                    
                    # Adiciona contexto da tabela e suas colunas (se couber e houver algo)
                    if (table_context_to_add or column_context_parts) and not context_limit_reached:
                         if current_context_tokens + table_tokens + column_tokens < max_context_tokens:
                              context_parts.extend(table_context_to_add) 
                              context_parts.extend(column_context_parts)
                              current_context_tokens += table_tokens + column_tokens
                         else:
                              context_limit_reached = True
                              logger.warning(f"Limite de contexto atingido ao adicionar Bloco Tabela '{obj_name}'.")
                    elif context_limit_reached:
                        break # Sai do loop de tabelas

                # 3. Busca Semântica (FAISS - Se Habilitado)
                if st.session_state.get('use_embeddings', False) and st.session_state.get('faiss_index') and not context_limit_reached:
                    logger.info("Chat: Realizando busca FAISS para contexto adicional.")
                    try:
                        # Gerar embedding para a pergunta (precisa da função correta)
                        # Assumindo que temos uma função `get_embedding(text)` disponível
                        # query_embedding = get_embedding(prompt) # SUBSTITUIR PELA FUNÇÃO REAL
                        # Placeholder - Precisamos da função de embedding! Por enquanto, não executa.
                        query_embedding = None 
                        logger.warning("Função para gerar embedding da query não implementada/disponível. Busca FAISS pulada.")

                        if query_embedding is not None:
                            similar_cols = find_similar_columns(
                                st.session_state.faiss_index,
                                st.session_state.technical_schema, 
                                st.session_state.index_to_key_map,
                                query_embedding,
                                k=5 # Buscar 5 mais similares com descrição
                            )
                            if similar_cols:
                                faiss_context = "--- Contexto Similar (Busca Semântica) ---\n"
                                for sim_col in similar_cols:
                                    # Evitar adicionar tabelas já incluídas por palavra-chave?
                                    # Por ora, adiciona a coluna similar diretamente
                                    faiss_context += f"Tabela '{sim_col['table']}', Coluna '{sim_col['column']}': {sim_col['description']}\n"
                                # Adiciona o contexto FAISS (com verificação de limite)
                                tokens_faiss = len(faiss_context.split())
                                if current_context_tokens + tokens_faiss < max_context_tokens:
                                    context_parts.append(faiss_context)
                                    current_context_tokens += tokens_faiss
                                else:
                                    context_limit_reached = True
                                    logger.warning("Limite de contexto atingido ao adicionar contexto FAISS.")
                    except Exception as e:
                        logger.error(f"Erro durante busca FAISS para chat: {e}")

                # --- Monta o Prompt Final ---
                final_context = "\n".join(context_parts)
                if not final_context:
                    final_context = "Nenhum contexto relevante encontrado nos metadados."
                
                system_prompt = "Você é um assistente especialista em banco de dados. Responda à pergunta do usuário baseando-se *apenas* e *estritamente* no contexto fornecido sobre o schema. Não invente informações. Se a resposta não estiver no contexto, diga que não encontrou a informação no contexto fornecido."
                user_prompt_for_llm = f"**Contexto do Schema:**\n{final_context}\n\n**Pergunta:**\n{prompt}"
                
                logger.debug(f"Enviando para LLM:\nSystem: {system_prompt}\nUser: {user_prompt_for_llm}")
                
                try:
                    # Chamada ao LLM
                    full_response = chat_completion(
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt_for_llm}
                        ],
                        stream=False # Por enquanto, sem stream para simplificar
                    )

                    if full_response:
                        message_placeholder.markdown(full_response)
                        assistant_message_id = str(uuid.uuid4()) # Gera ID único para resposta
                        assistant_message = {"role": "assistant", "content": full_response, "message_id": assistant_message_id}
                        st.session_state.messages.append(assistant_message)
                    else:
                        fallback_msg = "Desculpe, não consegui obter uma resposta do modelo de IA."
                        message_placeholder.markdown(fallback_msg)
                        assistant_message_id = str(uuid.uuid4())
                        assistant_message = {"role": "assistant", "content": fallback_msg, "message_id": assistant_message_id}
                        st.session_state.messages.append(assistant_message)
                        
                    # Salva histórico APÓS adicionar a resposta do assistente
                    save_json(st.session_state.messages, CHAT_HISTORY_FILE)
                
                except Exception as e:
                    logger.exception("Erro ao chamar chat_completion no modo Chat com Schema:")
                    error_msg = f"Ocorreu um erro ao processar sua pergunta: {e}"
                    message_placeholder.markdown(error_msg)
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})


# --- Ações Globais na Sidebar --- 
st.sidebar.divider()
st.sidebar.header("Ações Globais")

# --- NOVO: Botão Salvar na Sidebar ---
if st.sidebar.button("💾 Salvar Alterações nos Metadados", type="primary", key="save_metadata_sidebar"): # Key atualizada para clareza
    logger.info("Tentativa de salvamento manual iniciada.")
    # Comparar antes de salvar
    new_desc_count, new_notes_count = 0, 0
    has_changes = False
    if 'initial_metadata' in st.session_state:
        new_desc_count, new_notes_count = compare_metadata_changes(
            st.session_state.initial_metadata,
            st.session_state.metadata
        )
        if new_desc_count > 0 or new_notes_count > 0:
            has_changes = True
    else:
        logger.warning("Estado inicial dos metadados não encontrado para comparação.")

    if save_metadata(st.session_state.metadata, METADATA_FILE):
        # Mensagem de sucesso com contadores
        success_message = f"Metadados salvos com sucesso em `{METADATA_FILE}`!"
        if new_desc_count > 0 or new_notes_count > 0:
            success_message += f" ({new_desc_count} novas descrições, {new_notes_count} novas notas adicionadas nesta sessão)"
        st.sidebar.success(success_message, icon="✅") # MUDANÇA: st.sidebar.success

        try:
            load_metadata.clear()
            logger.info("Cache de metadados limpo após salvar.")
            # Atualizar estado inicial após salvar com sucesso
            st.session_state.initial_metadata = copy.deepcopy(st.session_state.metadata)
            logger.info("Estado inicial dos metadados atualizado após salvar.")
            # ATUALIZA O TEMPO DO ÚLTIMO SAVE
            st.session_state.last_save_time = time.time()
            logger.info(f"Tempo do último salvamento atualizado para: {st.session_state.last_save_time}")
        except Exception as e:
            logger.warning(f"Erro ao limpar cache ou atualizar estado inicial: {e}")
    else:
        st.sidebar.error("Falha ao salvar metadados.") # MUDANÇA: st.sidebar.error
# --- FIM: Botão Salvar na Sidebar ---

if st.sidebar.button("Recarregar Metadados do Arquivo", key="reload_metadata_sidebar"):
    load_metadata.clear() # Limpa o cache antes de carregar
    st.session_state.metadata = load_metadata(METADATA_FILE)
    if st.session_state.metadata is not None:
        # NOVO: Atualiza também o estado inicial ao recarregar
        try:
            st.session_state.initial_metadata = copy.deepcopy(st.session_state.metadata)
            logger.info("Estado inicial dos metadados atualizado após recarregar.")
        except Exception as e:
            logger.error(f"Erro ao fazer deepcopy dos metadados iniciais após recarregar: {e}")
            st.session_state.initial_metadata = {} # Define como vazio em caso de erro
        st.success("Metadados recarregados do arquivo!")
        st.rerun()
    else:
        st.error("Falha ao recarregar metadados.")

st.sidebar.caption(f"Arquivo: {METADATA_FILE}")

# --- NOVO: Botão para Executar Merge ---
st.sidebar.divider()
st.sidebar.subheader("Processamento de Dados")

# --- Botão para Heurística Global ---
if st.sidebar.button("Aplicar Heurística Globalmente", key="apply_heuristics_button", help="Tenta preencher descrições de colunas vazias usando nomes/relações existentes."):
    with st.spinner("Aplicando heurística em todas as colunas vazias..."):
        upd_desc, upd_notes = apply_heuristics_globally(st.session_state.metadata, technical_schema_data)
        st.sidebar.success(f"Heurística Concluída!", icon="✅")
        # st.sidebar.info(f"- {updated} descrições preenchidas.
        #                - {already_filled} já tinham descrição.
        #                - {not_found} sem sugestão encontrada.")
        # NOVO: Feedback mais detalhado (CORRIGIDO)
        st.sidebar.info(f"- Descrições preenchidas: {upd_desc}\n- Notas preenchidas: {upd_notes}")
        st.sidebar.warning("As alterações estão em memória. Salve os metadados para persistir.")
# --- FIM Botão Heurística ---

# --- NOVO: Botão para Preencher via Chaves FK->PK ---
if st.sidebar.button("Preencher Descrições (Chaves FK->PK)", key="populate_keys_button", help="Preenche descrições vazias de colunas FK usando a descrição da PK referenciada."):
    with st.spinner("Analisando chaves FK -> PK e preenchendo descrições..."):
        updated_key_count = populate_descriptions_from_keys(st.session_state.metadata, technical_schema_data)
        if updated_key_count > 0:
            st.sidebar.success(f"{updated_key_count} descrições preenchidas via chaves FK->PK!", icon="🔑")
            st.sidebar.warning("As alterações estão em memória. Salve os metadados para persistir.")
        else:
            st.sidebar.info("Nenhuma descrição de FK vazia pôde ser preenchida via chaves.")
# --- FIM Botão Chaves --- 

if st.sidebar.button("Executar Merge de Dados", key="run_merge_script"):
    script_path = os.path.join("scripts", "merge_schema_data.py")
    if not os.path.exists(script_path):
        st.sidebar.error(f"Erro: Script de merge não encontrado em '{script_path}'")
    else:
        st.sidebar.info(f"Executando '{script_path}'...")
        try:
            python_executable = sys.executable 
            process = subprocess.Popen(
                [python_executable, script_path],
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                text=True, 
                encoding='utf-8',
                errors='replace'
            )
            
            # Ler stdout e stderr completos
            stdout, stderr = process.communicate()
            
            logger.info(f"Saída stdout do merge script:\n{stdout}")
            if stderr:
                logger.error(f"Saída stderr do merge script:\n{stderr}")

            if process.returncode == 0:
                st.sidebar.success(f"Merge concluído com sucesso! Arquivo '{OUTPUT_COMBINED_FILE}' atualizado.")
                # Limpar cache e recarregar app para refletir mudanças
                try:
                    load_technical_schema.clear()
                    logger.info("Cache do schema técnico limpo após merge.")
                    st.rerun()
                except Exception as e:
                    logger.warning(f"Erro ao limpar cache/rerun após merge: {e}")
                    st.sidebar.warning("Merge concluído, mas recarregue a página para ver as atualizações.")
            else:
                st.sidebar.error(f"Erro ao executar merge (Código: {process.returncode}). Verifique os logs.")
                if stderr:
                    st.sidebar.text_area("Erro Reportado:", stderr, height=100)
        except Exception as e:
            st.sidebar.error(f"Erro inesperado ao executar merge: {e}")
            logger.exception("Erro ao executar subprocesso de merge")
# --- FIM: Botão para Executar Merge ---

# --- NOVO: Constante e Toggle Auto-Save ---
st.sidebar.divider()
st.sidebar.subheader("Configurações Extras")
# Define a constante logo antes de usar
AUTO_SAVE_INTERVAL_SECONDS = 300 # 5 minutos 
st.sidebar.toggle(
    "Habilitar Auto-Save (Intervalo)", 
    key='auto_save_enabled', 
    value=st.session_state.get('auto_save_enabled', False),
    help=f"Salva automaticamente alterações pendentes a cada {AUTO_SAVE_INTERVAL_SECONDS // 60} minutos de interação."
)
# --- FIM: Toggle Auto-Save ---

# Informação sobre como rodar
st.sidebar.info("Para executar este app, use o comando: `streamlit run streamlit_app.py` no seu terminal.")


# --- LÓGICA DE AUTO-SAVE (Executa no final de cada rerun) ---
if st.session_state.get('auto_save_enabled', False):
    time_since_last_save = time.time() - st.session_state.get('last_save_time', 0)
    
    if time_since_last_save >= AUTO_SAVE_INTERVAL_SECONDS:
        logger.info(f"Verificando auto-save. Tempo desde último save: {time_since_last_save:.2f}s")
        # Verifica se há mudanças reais antes de salvar
        auto_save_desc_count, auto_save_notes_count = 0, 0
        auto_save_has_changes = False
        if 'initial_metadata' in st.session_state:
            auto_save_desc_count, auto_save_notes_count = compare_metadata_changes(
                st.session_state.initial_metadata,
                st.session_state.metadata
            )
            if auto_save_desc_count > 0 or auto_save_notes_count > 0:
                auto_save_has_changes = True
        
        if auto_save_has_changes:
            logger.info("Mudanças detectadas, iniciando auto-save...")
            if save_metadata(st.session_state.metadata, METADATA_FILE):
                try:
                    load_metadata.clear()
                    st.session_state.initial_metadata = copy.deepcopy(st.session_state.metadata)
                    st.session_state.last_save_time = time.time()
                    logger.info(f"Auto-save concluído com sucesso. Tempo atualizado: {st.session_state.last_save_time}")
                    st.toast("Metadados salvos automaticamente.", icon="⏱️")
                except Exception as e:
                    logger.error(f"Erro durante pós-processamento do auto-save: {e}")
                    # Não exibir toast de erro aqui para não ser muito intrusivo?
                    # Talvez logar seja suficiente.
            else:
                logger.error("Falha no auto-save.")
                # st.toast("Falha ao salvar automaticamente!", icon="❌") # Talvez intrusivo?
        else:
            logger.info("Auto-save verificado, mas sem alterações pendentes.")
# --- FIM: LÓGICA DE AUTO-SAVE ---