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

# Configuração básica de logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# NOVO: Tentar importar a função de chat (lidar com erro se não existir)
try:
    from src.ollama_integration.client import chat_completion
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
TECHNICAL_SCHEMA_FILE = 'data/combined_schema_details.json' # NOVO: Carregar dados técnicos combinados
OVERVIEW_COUNTS_FILE = 'data/overview_counts.json' # NOVO: Arquivo para contagens cacheadas
# NOVO: Definir o nome do arquivo de saída do merge para usar na mensagem
OUTPUT_COMBINED_FILE = 'data/combined_schema_details.json'

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

# --- Função find_existing_description (Adaptada de view_schema_app.py) ---
def find_existing_description(metadata, schema_data, current_object_name, target_col_name):
    """
    Procura por uma descrição existente para uma coluna:
    1. Busca por nome exato em outras tabelas/views.
    2. Se for FK, busca a descrição da PK referenciada.
    3. Se for PK, busca a descrição de uma coluna FK que a referencie.
    """
    if not metadata or not schema_data or not target_col_name or not current_object_name:
        return None, None # Retorna None para descrição e para a fonte

    # 1. Busca por nome exato (prioridade)
    for obj_type_key in ['TABLES', 'VIEWS', 'DESCONHECIDOS']:
        for obj_name, obj_meta in metadata.get(obj_type_key, {}).items():
            if obj_name == current_object_name: continue
            col_meta = obj_meta.get('COLUMNS', {}).get(target_col_name)
            if col_meta and col_meta.get('description', '').strip():
                desc = col_meta['description']
                source = f"nome exato em `{obj_name}`"
                logger.debug(f"Heurística: Descrição encontrada por {source} para {current_object_name}.{target_col_name}")
                return desc, source

    # Se não achou por nome exato, tenta via FKs (precisa do schema_data técnico)
    current_object_info = schema_data.get(current_object_name)
    if not current_object_info:
        logger.warning(f"Schema técnico não encontrado para {current_object_name} ao buscar heurística FK.")
        return None, None
    
    current_constraints = current_object_info.get('constraints', {})
    current_pk_cols = [col for pk in current_constraints.get('primary_key', []) for col in pk.get('columns', [])]

    # 2. Busca Direta (Se target_col é FK)
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
                    return desc, source
            except (IndexError, ValueError): continue

    # 3. Busca Inversa (Se target_col é PK)
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
                             return desc, source
                     except (IndexError, ValueError): continue

    return None, None # Nenhuma descrição encontrada

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

        # Formata contagem para exibição
        row_count_display = row_count_val
        if isinstance(row_count_val, int) and row_count_val >= 0:
             row_count_display = f"{row_count_val:,}".replace(",", ".") # Formato brasileiro
        elif isinstance(row_count_val, str) and row_count_val.startswith("Erro"):
            row_count_display = "Erro" # Simplifica exibição de erro
        
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
            '% c/ Notas': f"{notes_perc:.1f}%"
        })

    df_overview = pd.DataFrame(overview_data)
    if not df_overview.empty:
        # Ordenar colunas para melhor visualização
        cols_order = ['Objeto', 'Tipo', 'Descrição?', 'Total Colunas', 'Linhas (Cache)', 'Contagem Em',
                      'Col. Descritas', '% Descritas', 'Col. c/ Notas', '% c/ Notas']
        # Remove colunas que não existem mais ou ajusta a ordem
        cols_order = [col for col in cols_order if col in df_overview.columns]
        df_overview = df_overview[cols_order].sort_values(by=['Tipo', 'Objeto']).reset_index(drop=True)
    logger.info(f"Visão geral gerada. Shape: {df_overview.shape}")
    return df_overview

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

# --- Interface Streamlit --- MODIFICADA PARA MODOS
st.set_page_config(layout="wide")
st.title("📝 Editor/Visualizador de Metadados do Schema") # Título mais genérico

# --- Carregamento Inicial --- 
technical_schema_data = load_technical_schema(TECHNICAL_SCHEMA_FILE)
if technical_schema_data is None: st.stop()

if 'metadata' not in st.session_state:
    st.session_state.metadata = load_metadata(METADATA_FILE)
    if st.session_state.metadata is None: st.stop()

# NOVO: Carrega contagens cacheadas
if 'overview_counts' not in st.session_state:
    st.session_state.overview_counts = load_overview_counts(OVERVIEW_COUNTS_FILE)

# NOVO: Inicializa estado para timestamp sob demanda
if 'latest_db_timestamp' not in st.session_state:
    st.session_state.latest_db_timestamp = None # Inicializa como None

metadata_dict = st.session_state.metadata # Referência local
# NOVO: Inicializar estado para Ollama
if 'ollama_enabled' not in st.session_state:
    st.session_state.ollama_enabled = True

# --- Barra Lateral --- 
st.sidebar.title("Navegação e Ações")

# Seletor de Modo
app_mode = st.sidebar.radio(
    "Modo de Operação",
    ["Editar Metadados", "Visão Geral", "Análise"], # NOVO: Adicionado 'Análise'
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

# --- NOVO: Toggle para Habilitar/Desabilitar Ollama ---
st.sidebar.divider()
st.sidebar.subheader("Configurações")
if OLLAMA_AVAILABLE:
    st.sidebar.toggle("Habilitar Sugestões IA (Ollama)", key='ollama_enabled', help="Desabilitar pode melhorar a performance se não precisar das sugestões.")
else:
    st.sidebar.caption("Sugestões IA (Ollama) indisponíveis.")
# --- FIM: Toggle --- 

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
                process = subprocess.Popen(
                    [python_executable, script_path],
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

                            # Heurística e Área de Texto (Código existente, adaptado para usar estado da sessão)

                            # Heurística, Desc Area, Botões IA/Propagar
                            col_desc_key = f"desc_{selected_object_technical_type}_{selected_object}_{col_name}"
                            # --- INÍCIO: Código de Text Area para Descrição e Notas (Re-inserido) ---
                            current_col_desc_saved = col_meta_data.get('description', '').strip()
                            description_value_to_display = current_col_desc_saved
                            heuristic_source = None

                            if not current_col_desc_saved:
                                existing_desc, source = find_existing_description(metadata_dict, technical_schema_data, selected_object, col_name)
                                if existing_desc:
                                    description_value_to_display = existing_desc
                                    heuristic_source = source
                                    logger.info(f"Preenchendo '{selected_object}.{col_name}' com sugestão via {source}")

                            if heuristic_source:
                                st.caption(f"ℹ️ Sugestão preenchida ({heuristic_source}). Pode editar abaixo.")

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
                                elif heuristic_source and not current_col_desc_saved: # Se heuristica foi usada e campo estava vazio, salva heuristica
                                    col_meta_data["description"] = description_value_to_display
                                else:
                                    col_meta_data["description"] = current_col_desc_saved # Garante que o valor salvo seja mantido se não editado

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
                            col_notes_key = f"notes_{selected_object_technical_type}_{selected_object}_{col_name}"
                            new_col_notes = st.text_area(
                                f"Notas Mapeamento (`{col_name}`)",
                                value=col_meta_data.get("value_mapping_notes", ""),
                                key=col_notes_key,
                                height=75,
                                label_visibility="collapsed", # Esconde label repetido
                                help="Explique valores específicos (ex: 1=Ativo) ou formatos."
                            )
                            col_meta_data["value_mapping_notes"] = new_col_notes
                            # --- FIM: Código de Text Area para Descrição e Notas (Re-inserido) ---

            st.divider() # Separador antes da pré-visualização

            # --- NOVO: Seção de Pré-visualização de Dados ---
            with st.expander("👁️ Pré-Visualização de Dados", expanded=False):
                num_rows_to_fetch = st.number_input(
                    "Número de linhas para buscar:", 
                    min_value=1, 
                    # max_value=500, # REMOVIDO: Permitir valores maiores
                    value=10, 
                    step=1, # Mudar step para 1 para facilitar digitação de qualquer número
                    key=f"num_rows_{selected_object}",
                    help="Digite o número de linhas desejado. Valores muito altos podem impactar o desempenho."
                )
                st.caption("⚠️ Solicitar muitas linhas pode tornar a aplicação lenta ou consumir muita memória.")
                
                if st.button("Carregar Amostra", key=f"load_sample_{selected_object}"):
                    # Usa os mesmos parâmetros de conexão da busca de timestamp
                    sample_data = fetch_sample_data(
                        db_path_for_ts,
                        db_user_for_ts,
                        db_password_for_ts,
                        db_charset_for_ts,
                        selected_object, # Nome da tabela/view atual
                        num_rows_to_fetch
                    )
                    
                    if isinstance(sample_data, pd.DataFrame):
                        if sample_data.empty:
                            st.info(f"Nenhuma amostra de dados retornada para '{selected_object}'. A tabela pode estar vazia.")
                        else:
                            st.dataframe(sample_data, use_container_width=True)
                    else: # Se retornou uma string de erro
                        st.error(f"Falha ao carregar amostra: {sample_data}")
            # --- FIM: Seção de Pré-visualização de Dados ---

            # --- Botão Salvar Edição --- 
            st.divider()
            if st.button("💾 Salvar Alterações nos Metadados", type="primary", key="save_edit_mode"):
                if save_metadata(st.session_state.metadata, METADATA_FILE):
                    st.success(f"Metadados salvos com sucesso em `{METADATA_FILE}`!")
                    try: load_metadata.clear(); logger.info("Cache de metadados limpo após salvar.")
                    except Exception as e: logger.warning(f"Erro ao limpar cache: {e}")
                else: st.error("Falha ao salvar metadados.")

    else:
        st.info("Selecione um objeto para editar seus metadados.")

# --- NOVO: Modo Análise ---
elif app_mode == "Análise":
    st.header("🔎 Análise do Schema")
    st.caption(f"Analisando informações de: `{TECHNICAL_SCHEMA_FILE}`")
    st.divider()

    st.subheader("Colunas Mais Referenciadas por Chaves Estrangeiras (FKs)")
    
    if technical_schema_data and 'fk_reference_counts' in technical_schema_data:
        fk_counts = technical_schema_data['fk_reference_counts']
        
        if not fk_counts:
            st.info("Nenhuma contagem de referência de FK encontrada no schema técnico.")
        else:
            # Converter para lista de dicionários para o DataFrame
            fk_list = []
            for key, count in fk_counts.items():
                try:
                    table_name, column_name = key.split('.', 1)
                    
                    # --- NOVO: Verifica metadados da coluna --- 
                    has_description = False
                    has_notes = False
                    # Acessa o schema técnico combinado já carregado
                    obj_data = technical_schema_data.get(table_name)
                    if obj_data:
                        columns_list = obj_data.get('columns', [])
                        # Encontra os dados da coluna específica pelo nome
                        col_data = next((col for col in columns_list if col.get('name') == column_name), None)
                        if col_data:
                            # Verifica se a descrição de negócio (adicionada pelo merge) existe e não está vazia
                            desc_value = col_data.get('business_description')
                            has_description = bool(desc_value and desc_value.strip())
                            # Verifica se as notas de mapeamento (adicionadas pelo merge) existem e não estão vazias
                            notes_value = col_data.get('value_mapping_notes')
                            has_notes = bool(notes_value and notes_value.strip())
                    # --- FIM NOVO ---
                    
                    fk_list.append({
                        "Tabela": table_name, 
                        "Coluna": column_name, 
                        "Nº Referências FK": count,
                        "Tem Descrição?": "✅" if has_description else "❌", # NOVO
                        "Tem Notas?": "✅" if has_notes else "❌" # NOVO
                        })
                except ValueError:
                    logger.warning(f"Formato inválido na chave fk_reference_counts: {key}")
            
            if not fk_list:
                 st.warning("Não foi possível processar as contagens de FK.")
            else:
                # Ordenar pela contagem (descendente)
                fk_list_sorted = sorted(fk_list, key=lambda x: x["Nº Referências FK"], reverse=True)
                
                df_fk_counts = pd.DataFrame(fk_list_sorted)
                
                # Opção para mostrar N top colunas
                num_to_show = st.slider(
                    "Mostrar Top N colunas mais referenciadas:", 
                    min_value=5, 
                    max_value=len(df_fk_counts), 
                    value=min(20, len(df_fk_counts)), # Padrão 20 ou o total se menor
                    step=5
                )
                
                st.dataframe(df_fk_counts.head(num_to_show), use_container_width=True)
                
                # Expander para mostrar todas
                with st.expander("Mostrar todas as colunas referenciadas"):
                     st.dataframe(df_fk_counts, use_container_width=True)
            
    else:
        st.error("Dados de contagem de referência de FK ('fk_reference_counts') não encontrados no arquivo de schema técnico.")
        st.info(f"Certifique-se de que o script `scripts/extract_schema.py` ou `scripts/merge_schema_data.py` foi executado e gerou o arquivo `{TECHNICAL_SCHEMA_FILE}` corretamente.")

# --- Ações Globais na Sidebar --- 
st.sidebar.divider()
st.sidebar.header("Ações Globais")
if st.sidebar.button("Recarregar Metadados do Arquivo", key="reload_metadata_sidebar"):
    load_metadata.clear() # Limpa o cache antes de carregar
    st.session_state.metadata = load_metadata(METADATA_FILE)
    if st.session_state.metadata is not None:
        st.success("Metadados recarregados do arquivo!")
        st.rerun()
    else:
        st.error("Falha ao recarregar metadados.")

st.sidebar.caption(f"Arquivo: {METADATA_FILE}")

# --- NOVO: Botão para Executar Merge ---
st.sidebar.divider()
st.sidebar.subheader("Processamento de Dados")
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

# Informação sobre como rodar
st.sidebar.info("Para executar este app, use o comando: `streamlit run streamlit_app.py` no seu terminal.") 