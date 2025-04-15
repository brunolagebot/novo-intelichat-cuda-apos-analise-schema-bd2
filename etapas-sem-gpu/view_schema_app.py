import streamlit as st
import json
import pandas as pd
import fdb
import logging
import os
from collections import defaultdict
import re # Necessário para limpar o nome do tipo
import datetime # NOVO: Para timestamps
import time # GARANTIR QUE ESTE IMPORT ESTEJA PRESENTE
# Importar a função de chat do nosso cliente Ollama
from src.ollama_integration.client import chat_completion

# Configuração do Logging (opcional para Streamlit, mas útil para depuração)
# Nível DEBUG para ver dados brutos da amostra
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s')
logger = logging.getLogger(__name__)

SCHEMA_FILE = "firebird_schema.json"
METADATA_FILE = "schema_metadata.json" # Novo arquivo para metadados

# --- Configurações Padrão --- (Podem ser sobrescritas na interface)
DEFAULT_DB_PATH = r"C:\Projetos\DADOS.FDB"
DEFAULT_DB_USER = "SYSDBA"
DEFAULT_DB_CHARSET = "WIN1252"
DEFAULT_SAMPLE_SIZE = 10

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
    # Adicionar outros tipos comuns se necessário
}

def get_type_explanation(type_string):
    """Tenta encontrar uma explicação para o tipo SQL base."""
    if not type_string:
        return ""
    # Extrai o nome base do tipo (ex: VARCHAR de VARCHAR(100))
    base_type = re.match(r"^([A-Z\s_]+)", type_string.upper())
    if base_type:
        explanation = TYPE_EXPLANATIONS.get(base_type.group(1).strip())
        return f"*{explanation}*" if explanation else ""
    return ""

# --- Constante para arquivo de timestamps ---
RUN_TIMES_FILE = "run_times.json"
OVERVIEW_COUNTS_FILE = "overview_counts.json" # NOVO: Arquivo para contagens

# --- Funções Auxiliares --- 

# REMOVIDO/COMENTADO: @st.cache_data # Cache para estrutura técnica (não muda na sessão)
def load_schema(file_path):
    """Carrega o esquema técnico do banco de dados do arquivo JSON."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"Erro: Arquivo de esquema '{file_path}' não encontrado. Execute 'extract_firebird_schema.py' primeiro.")
        return None
    except json.JSONDecodeError:
        st.error(f"Erro: Arquivo de esquema '{file_path}' não é um JSON válido.")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao carregar o esquema: {e}")
        return None

def load_metadata(file_path):
    """Carrega os metadados (descrições) do arquivo JSON. Retorna dict vazio se não existir."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            st.warning(f"Aviso: Arquivo de metadados '{file_path}' inválido. Começando com metadados vazios.")
            return {}
        except Exception as e:
            st.error(f"Erro inesperado ao carregar metadados: {e}")
            return {}
    else:
        st.info(f"Arquivo de metadados '{file_path}' não encontrado. Será criado ao salvar.")
        return {}

def save_metadata_to_file(metadata, file_path):
    """Salva o dicionário de metadados no arquivo JSON."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=4, ensure_ascii=False)
        logger.info(f"Metadados salvos com sucesso em {file_path}")
        return True
    except IOError as e:
        logger.error(f"Erro de IO ao salvar metadados em {file_path}: {e}")
        st.error(f"Erro ao salvar arquivo de metadados: {e}")
        return False
    except Exception as e:
        logger.exception(f"Erro inesperado ao salvar metadados:")
        st.error(f"Erro inesperado ao salvar metadados: {e}")
        return False

def display_constraints(constraints):
    """Formata e exibe as constraints de uma tabela."""
    if not constraints:
        st.write("Nenhuma constraint definida para esta tabela.")
        return

    if constraints.get('primary_key'):
        st.subheader("Chave Primária")
        for pk in constraints['primary_key']:
            st.write(f"- Nome: `{pk['name']}`")
            st.write(f"  Colunas: `{', '.join(pk['columns'])}`")

    if constraints.get('foreign_keys'):
        st.subheader("Chaves Estrangeiras")
        for fk in constraints['foreign_keys']:
            st.write(f"- Nome: `{fk['name']}`")
            st.write(f"  Colunas: `{', '.join(fk['columns'])}`")
            st.write(f"  Referencia Tabela: `{fk['references_table']}`")
            # st.write(f"    Update Rule: {fk['update_rule']}") # Descomentar se precisar
            # st.write(f"    Delete Rule: {fk['delete_rule']}") # Descomentar se precisar

    if constraints.get('unique'):
        st.subheader("Constraints Únicas")
        for uq in constraints['unique']:
            st.write(f"- Nome: `{uq['name']}`")
            st.write(f"  Colunas: `{', '.join(uq['columns'])}`")

    if constraints.get('check'):
        st.subheader("Constraints Check")
        for ck in constraints['check']:
            st.write(f"- Nome: `{ck['name']}`")
            st.write(f"  Expressão: `{ck['expression']}`")

    # Adicionar outros tipos se necessário (ex: 'not_null', 'other')

def fetch_sample_data(db_path, user, password, charset, table_name, sample_size):
    """Busca uma amostra de dados de uma tabela ou view específica no Firebird."""
    conn = None
    try:
        logger.info(f"Tentando conectar a {db_path} para buscar amostra de {table_name}")
        conn = fdb.connect(
            dsn=db_path,
            user=user,
            password=password,
            charset=charset
        )
        cur = conn.cursor()
        sql = f'SELECT FIRST {sample_size} * FROM "{table_name}"'
        logger.info(f"Executando SQL: {sql}")
        cur.execute(sql)
        colnames = [desc[0] for desc in cur.description]
        logger.debug(f"Nomes das colunas da amostra: {colnames}")
        data = cur.fetchall()
        # Log DEBUG para ver os dados brutos antes de criar DataFrame
        logger.debug(f"Dados brutos recuperados ({len(data)} linhas): {data}")

        if not data:
            logger.info("Consulta de amostra não retornou linhas.")
            return pd.DataFrame(columns=colnames) # Retorna DF vazio com colunas

        # Tenta criar DataFrame
        df = pd.DataFrame(data, columns=colnames)
        logger.info(f"{len(df)} linhas de amostra convertidas para DataFrame.")
        return df

    except fdb.Error as e:
        logger.error(f"Erro do Firebird ao buscar amostra: {e}", exc_info=True)
        st.error(f"Erro de Conexão/Consulta Firebird: {e}")
        return None
    except Exception as e:
        logger.exception("Erro inesperado ao buscar/processar amostra de dados:")
        st.error(f"Erro inesperado ao processar dados: {e}")
        return None
    finally:
        if conn:
            conn.close()
            logger.info("Conexão para busca de amostra fechada.")

# Função para gerar descrição via IA (copiada e adaptada)
def generate_ai_description(prompt):
    """Chama a API Ollama para gerar uma descrição e limpa a resposta."""
    logger.debug(f"Enviando prompt para IA: {prompt}")
    messages = [{"role": "user", "content": prompt}]
    try:
        # Usando um spinner para feedback visual durante a chamada da IA
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

# --- Função find_existing_description (MODIFICADA para usar FKs) ---
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
            # Pula o próprio objeto atual
            if obj_name == current_object_name:
                continue
            col_meta = obj_meta.get('COLUMNS', {}).get(target_col_name)
            if col_meta and col_meta.get('description', '').strip():
                desc = col_meta['description']
                source = f"nome exato em '{obj_name}'"
                logger.debug(f"Heurística: Descrição encontrada por {source}")
                return desc, source

    # Se não achou por nome exato, tenta via FKs (precisa do schema_data)
    current_object_info = schema_data.get(current_object_name)
    if not current_object_info:
        return None, None
    
    current_constraints = current_object_info.get('constraints', {})
    current_pk_cols = [col for pk in current_constraints.get('primary_key', []) for col in pk.get('columns', [])]

    # 2. Busca Direta (Se target_col é FK)
    for fk in current_constraints.get('foreign_keys', []):
        fk_columns = fk.get('columns', [])
        ref_table = fk.get('references_table')
        ref_columns = fk.get('references_columns', [])
        if target_col_name in fk_columns and ref_table and ref_columns:
            # Assume FK simples (1 coluna local -> 1 coluna ref)
            try:
                idx = fk_columns.index(target_col_name)
                ref_col_name = ref_columns[idx]
                # Busca descrição da PK referenciada
                # Precisa determinar o tipo do objeto referenciado (TABLE/VIEW)
                ref_obj_type = schema_data.get(ref_table, {}).get('object_type', 'TABLE') # Assume TABLE se não achar
                ref_obj_type_key = ref_obj_type + "S"
                
                ref_col_meta = metadata.get(ref_obj_type_key, {}).get(ref_table, {}).get('COLUMNS', {}).get(ref_col_name)
                if ref_col_meta and ref_col_meta.get('description', '').strip():
                    desc = ref_col_meta['description']
                    source = f"chave estrangeira para '{ref_table}.{ref_col_name}'"
                    logger.debug(f"Heurística: Descrição encontrada por {source}")
                    return desc, source
            except (IndexError, ValueError):
                logger.warning(f"Inconsistência na FK {fk.get('name')} para coluna {target_col_name}")
                continue # Tenta próxima FK se houver erro

    # 3. Busca Inversa (Se target_col é PK)
    if target_col_name in current_pk_cols:
        # Varre todos os objetos no schema técnico buscando FKs que apontem para cá
        for other_obj_name, other_obj_info in schema_data.items():
            if other_obj_name == current_object_name: continue # Pula a própria tabela
            
            other_constraints = other_obj_info.get('constraints', {})
            for other_fk in other_constraints.get('foreign_keys', []):
                 if other_fk.get('references_table') == current_object_name and \
                    target_col_name in other_fk.get('references_columns', []):
                     # Achou uma FK em outra tabela apontando para nossa PK
                     referencing_columns = other_fk.get('columns', [])
                     ref_pk_columns = other_fk.get('references_columns', [])
                     try:
                         # Acha a coluna local (FK) correspondente à nossa PK
                         idx_pk = ref_pk_columns.index(target_col_name)
                         referencing_col_name = referencing_columns[idx_pk]
                         
                         # Busca a descrição desta coluna FK na outra tabela
                         other_obj_type = other_obj_info.get('object_type', 'TABLE')
                         other_obj_type_key = other_obj_type + "S"
                         other_col_meta = metadata.get(other_obj_type_key, {}).get(other_obj_name, {}).get('COLUMNS', {}).get(referencing_col_name)
                         
                         if other_col_meta and other_col_meta.get('description', '').strip():
                             desc = other_col_meta['description']
                             source = f"coluna '{referencing_col_name}' em '{other_obj_name}' (que referencia esta PK)"
                             logger.debug(f"Heurística: Descrição encontrada por {source}")
                             return desc, source
                     except (IndexError, ValueError):
                          logger.warning(f"Inconsistência na FK {other_fk.get('name')} em {other_obj_name} ao buscar descrição inversa.")
                          continue # Tenta próxima FK

    return None, None # Nenhuma descrição encontrada

# --- Função para Contar Linhas (MODIFICADA) ---
def fetch_row_count(db_path, user, password, charset, table_name):
    """Busca a contagem de linhas de uma tabela/view específica no Firebird."""
    conn = None
    cur = None # Inicializa cur fora do try
    try:
        logger.info(f"Buscando contagem de linhas para {table_name}")
        conn = fdb.connect(dsn=db_path, user=user, password=password, charset=charset)
        cur = conn.cursor()
        sql = f'SELECT COUNT(*) FROM "{table_name}"'
        cur.execute(sql)
        count = cur.fetchone()[0] 
        logger.info(f"Contagem para {table_name}: {count}")
        # Fechar cursor aqui após sucesso também é bom
        cur.close()
        cur = None 
        return count
    except fdb.Error as e:
        logger.error(f"Erro do Firebird ao buscar contagem para {table_name}: {e}", exc_info=True)
        return f"Erro DB: {e.args[0] if e.args else e}"
    except Exception as e:
        logger.exception(f"Erro inesperado ao buscar contagem para {table_name}:")
        return f"Erro inesperado: {e}"
    finally:
        # Garante que cursor e conexão sejam fechados
        if cur is not None:
            try:
                cur.close()
                logger.debug(f"Cursor para contagem ({table_name}) fechado no finally.")
            except fdb.Error as close_err:
                 # Logar erro ao fechar cursor, mas continuar para fechar conexão
                 logger.warning(f"Erro ao fechar cursor para {table_name} no finally: {close_err}")
        if conn is not None:
            try:
                conn.close()
                logger.debug(f"Conexão para busca de contagem ({table_name}) fechada no finally.")
            except fdb.Error as close_err:
                 # Logar erro ao fechar conexão
                 logger.warning(f"Erro ao fechar conexão para {table_name} no finally: {close_err}")

# --- NOVA FUNÇÃO: Gerar Visão Geral da Documentação ---
# (Adaptada para esta versão - sem cache, sem contar linhas reais)
def generate_documentation_overview(schema_data):
    """Gera DataFrame da visão geral, incluindo contagens/timestamps do cache."""
    logger.info("Iniciando generate_documentation_overview...")
    overview_data = []
    total_objects_processed = 0
    metadata = st.session_state.get('metadata', {}) # Metadados para desc/notas
    overview_counts = st.session_state.get("overview_row_counts", {}) # Contagens cacheadas

    for name, info in schema_data.items():
        object_type = info.get('object_type', 'DESCONHECIDO')
        if object_type not in ["TABLE", "VIEW"]:
             continue

        logger.debug(f"Processando objeto para visão geral: {name} (Tipo: {object_type})")
        total_objects_processed += 1
        columns = info.get('columns', [])
        total_cols = len(columns)
        
        key_type = object_type + "S"
        object_meta = metadata.get(key_type, {}).get(name, {})
        object_columns_meta = object_meta.get('COLUMNS', {})
        
        described_cols = 0
        noted_cols = 0
        if total_cols > 0:
            for col_def in columns:
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
             row_count_display = f"{row_count_val:,}"
        
        # Formata timestamp para exibição
        timestamp_display = "Nunca"
        if timestamp_val:
            try:
                dt_obj = datetime.datetime.fromisoformat(timestamp_val)
                timestamp_display = dt_obj.strftime("%d/%m/%y %H:%M") # Formato mais curto
            except ValueError:
                 timestamp_display = "Inválido" # Se o timestamp salvo for inválido

        overview_data.append({
            'Selecionar': False, # NOVA COLUNA BOOLEANA
            'Objeto': name,
            'Tipo': object_type,
            'Total Colunas': total_cols,
            'Total Linhas': row_count_display,
            'Última Contagem': timestamp_display, # NOVA COLUNA
            'Colunas Descritas': described_cols,
            'Colunas com Notas': noted_cols,
            '% Descritas': f"{desc_perc:.1f}%",
            '% Com Notas': f"{notes_perc:.1f}%"
        })

    df_overview = pd.DataFrame(overview_data)
    if not df_overview.empty:
        # Ordenar colunas para melhor visualização
        cols_order = ['Selecionar', 'Objeto', 'Tipo', 'Total Colunas', 'Total Linhas', 'Última Contagem',
                      'Colunas Descritas', '% Descritas', 'Colunas com Notas', '% Com Notas']
        df_overview = df_overview[cols_order].sort_values(by=['Tipo', 'Objeto']).reset_index(drop=True)
    logger.info(f"generate_documentation_overview concluído. Shape: {df_overview.shape}")
    return df_overview

# --- NOVAS Funções Auxiliares para Timestamps ---
def load_run_times(file_path):
    """Carrega os timestamps de execução do arquivo JSON."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.warning(f"Aviso: Arquivo de timestamps '{file_path}' inválido. Começando vazio.")
            return {}
        except Exception as e:
            logging.error(f"Erro inesperado ao carregar timestamps: {e}")
            return {}
    else:
        return {}

def save_run_times(run_times_data, file_path):
    """Salva o dicionário de timestamps no arquivo JSON."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(run_times_data, f, indent=4)
        logging.info(f"Timestamps salvos com sucesso em {file_path}")
        return True
    except IOError as e:
        logging.error(f"Erro de IO ao salvar timestamps em {file_path}: {e}")
        st.error(f"Erro ao salvar arquivo de timestamps: {e}")
        return False
    except Exception as e:
        logging.error(f"Erro inesperado ao salvar timestamps: {e}")
        st.error(f"Erro inesperado ao salvar timestamps: {e}")
        return False

# --- NOVAS Funções Auxiliares para Contagens ---
def load_overview_counts(file_path):
    """Carrega as contagens e timestamps da visão geral."""
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.warning(f"Aviso: Arquivo de contagens '{file_path}' inválido.")
            return {}
        except Exception as e:
            logging.error(f"Erro inesperado ao carregar contagens: {e}")
            return {}
    else:
        return {}

def save_overview_counts(counts_data, file_path):
    """Salva o dicionário de contagens e timestamps."""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(counts_data, f, indent=4)
        logging.info(f"Contagens da visão geral salvas em {file_path}")
        return True
    except Exception as e:
        logging.error(f"Erro ao salvar contagens da visão geral: {e}")
        return False

# --- NOVA FUNÇÃO: Calcular Contagem de Referências FK ---
# REMOVIDO/COMENTADO: @st.cache_data # Cache porque depende apenas do schema técnico
def calculate_fk_reference_counts(schema_data):
    """Calcula quantas vezes cada coluna é referenciada por uma FK."""
    reference_counts = defaultdict(int)
    if not schema_data:
        return []

    logger.info("[FK Count] Iniciando cálculo de referências...")
    # Itera sobre todos os objetos para encontrar FKs
    for obj_name, obj_info in schema_data.items():
        # LOG: Nome do objeto sendo processado
        logger.debug(f"[FK Count] Processando objeto: {obj_name}")
        constraints = obj_info.get('constraints', {})
        foreign_keys = constraints.get('foreign_keys', [])
        # LOG: Lista de FKs encontrada para este objeto
        logger.debug(f"[FK Count]   Lista foreign_keys encontrada para {obj_name}: {foreign_keys}")

        # Itera sobre cada FK definida neste objeto
        if not foreign_keys:
             logger.debug(f"[FK Count]   Nenhuma FK encontrada para {obj_name}, pulando.")
             continue # Pula para o próximo objeto se não houver FKs

        for i, fk in enumerate(foreign_keys):
            # LOG: Detalhes da FK sendo processada
            logger.debug(f"[FK Count]     Processando FK #{i+1} em {obj_name}: {fk}")
            ref_table = fk.get('references_table')
            ref_columns = fk.get('references_columns', [])
            # LOG: Tabela e colunas referenciadas extraídas da FK
            logger.debug(f"[FK Count]       -> references_table: {ref_table}, references_columns: {ref_columns}")

            # Incrementa a contagem para cada coluna referenciada
            if ref_table and ref_columns:
                for ref_col in ref_columns:
                    # Usa tupla (tabela, coluna) como chave
                    reference_counts[(ref_table, ref_col)] += 1
                    logger.debug(f"[FK Count]         -> Incrementando contagem para '{ref_table}.{ref_col}'")
            else:
                logger.debug(f"[FK Count]         -> FK ignorada (sem ref_table ou ref_columns).")
    
    # Converte para lista de tuplas e ordena pela contagem (descendente)
    sorted_references = sorted(
        [(table, col, count) for (table, col), count in reference_counts.items()],
        key=lambda item: item[2], # Ordena pelo terceiro elemento (contagem)
        reverse=True
    )
    logger.info(f"[FK Count] Cálculo de referências concluído. {len(sorted_references)} colunas referenciadas encontradas.")
    return sorted_references

# --- NOVA FUNÇÃO AUXILIAR: Obter Conceito da Coluna ---
def get_column_concept(schema_data, obj_name, col_name):
    """Determina o conceito raiz (PK referenciada ou a própria PK/coluna)."""
    if not schema_data or obj_name not in schema_data:
        return (obj_name, col_name) # Retorna ela mesma se não achar info
    
    obj_info = schema_data[obj_name]
    constraints = obj_info.get('constraints', {})
    pk_cols = [col for pk in constraints.get('primary_key', []) for col in pk.get('columns', [])]
    
    # É FK? Retorna a PK referenciada
    for fk in constraints.get('foreign_keys', []):
        if col_name in fk.get('columns', []):
            ref_table = fk.get('references_table')
            ref_columns = fk.get('references_columns', [])
            if ref_table and ref_columns:
                # Assume FK simples para conceito (primeira coluna referenciada)
                return (ref_table, ref_columns[0]) 
            else:
                return (obj_name, col_name) # FK malformada, retorna ela mesma
    
    # É PK? Retorna ela mesma
    if col_name in pk_cols:
        return (obj_name, col_name)
        
    # Não é PK nem FK? Retorna ela mesma
    return (obj_name, col_name)

# --- Função Principal da Aplicação --- 
def main():
    st.set_page_config(page_title="Anotador de Esquema Firebird", layout="wide")
    
    # Garante que metadados e chaves principais estão no estado da sessão
    if 'metadata' not in st.session_state:
        st.session_state.metadata = load_metadata(METADATA_FILE)
        logger.info("Metadados (re)carregados para session_state dentro de main.")
    # Garante as chaves de nível superior toda vez que main rodar
    st.session_state.metadata.setdefault('TABLES', {})
    st.session_state.metadata.setdefault('VIEWS', {})
    st.session_state.metadata.setdefault('_GLOBAL_CONTEXT', 'Digite aqui informações gerais sobre a empresa, o propósito do banco de dados, etc.')

    # Inicializa o estado do modo de visualização
    if 'app_mode' not in st.session_state:
        st.session_state.app_mode = "Explorar Esquema"

    # NOVO: Carrega timestamps e inicializa cache de contagem
    if 'run_times' not in st.session_state:
        st.session_state.run_times = load_run_times(RUN_TIMES_FILE)
    if 'overview_row_counts' not in st.session_state:
         st.session_state.overview_row_counts = load_overview_counts(OVERVIEW_COUNTS_FILE)

    # Carrega o schema técnico (estrutura real do DB)
    schema_data = load_schema(SCHEMA_FILE)
    if not schema_data:
        st.stop()

    # --- Barra Lateral --- 
    st.sidebar.title("Navegação e Configurações") # Título ajustado
    
    # Seletor de Modo
    st.session_state.app_mode = st.sidebar.radio(
        "Modo de Visualização",
        ["Explorar Esquema", "Visão Geral da Documentação"],
        key='app_mode_selector',
        index=["Explorar Esquema", "Visão Geral da Documentação"].index(st.session_state.app_mode)
    )
    st.sidebar.divider()

    # Configurações de Conexão (Sempre visível nesta versão, pode ser necessário ajustar)
    st.sidebar.header("Configurações de Conexão")
    db_path_input = st.sidebar.text_input("Caminho DB (.fdb)", value=DEFAULT_DB_PATH, key="db_path_sb") # Chave diferente para evitar conflito com st.session_state
    db_user_input = st.sidebar.text_input("Usuário DB", value=DEFAULT_DB_USER, key="db_user_sb")
    # Senha não fica na sidebar
    
    st.sidebar.divider()
    # Contexto Geral
    st.sidebar.subheader("Contexto Geral")
    context_key = "global_context_input"
    current_global_context = st.session_state.metadata.get('_GLOBAL_CONTEXT', '')
    new_global_context = st.sidebar.text_area(
        "Descreva o contexto geral da empresa/dados:",
        value=current_global_context,
        key=context_key,
        height=150
    )
    if new_global_context != current_global_context:
        st.session_state.metadata['_GLOBAL_CONTEXT'] = new_global_context
        st.toast("Contexto global atualizado (não salvo)", icon="✏️")

    st.sidebar.divider()
    # Botão Salvar
    if st.sidebar.button("💾 Salvar Metadados e Contexto", use_container_width=True):
        if save_metadata_to_file(st.session_state.metadata, METADATA_FILE):
            st.sidebar.success("Metadados e Contexto salvos!")
            st.session_state.metadata = load_metadata(METADATA_FILE) # Recarrega
            st.toast("Metadados salvos!", icon="💾")
        else:
            st.sidebar.error("Falha ao salvar.")
    st.sidebar.caption(f"Salvo em: {METADATA_FILE}")

    # --- Conteúdo Principal (Condicional ao Modo) ---

    if st.session_state.app_mode == "Visão Geral da Documentação":
        st.title("Visão Geral da Documentação e Contagem")
        st.markdown("Marque os objetos na coluna 'Selecionar' e clique no botão para contar as linhas.")
        
        if schema_data:
             try:
                 df_initial_overview = generate_documentation_overview(schema_data)
                 
                 # Usar st.data_editor em vez de st.dataframe
                 edited_df = st.data_editor(
                     df_initial_overview, 
                     key="overview_editor",
                     use_container_width=True,
                     # Desabilitar edição para outras colunas (opcional, mas recomendado)
                     disabled=['Objeto', 'Tipo', 'Total Colunas', 'Total Linhas', 'Última Contagem',
                               'Colunas Descritas', '% Descritas', 'Colunas com Notas', '% Com Notas']
                 )
                 
                 st.divider()
                 
                 # --- Ações de Contagem (MODIFICADO) --- 
                 col_act_all, col_act_selected = st.columns([1, 2])
                 
                 with col_act_all:
                    # --- Botão Contar Todos --- 
                    if st.button("Calcular Contagem (Todos)", key="btn_count_all", use_container_width=True):
                        password_to_use = "M@nagers2023" # Senha hardcoded ainda
                        objects_to_count = df_initial_overview['Objeto'].tolist()
                        total_obj = len(objects_to_count)
                        progress_bar = st.progress(0, text="Iniciando contagem...")
                        local_counts = {} # NOVO: Dicionário local para acumular
                        error_occurred = False

                        with st.spinner(f"Calculando contagem para {total_obj} objetos..."):
                            for i, obj_name in enumerate(objects_to_count):
                                progress_text = f"Contando {obj_name}... ({i+1}/{total_obj})"
                                progress_bar.progress((i + 1) / total_obj, text=progress_text)
                                logger.info(f"Executando fetch_row_count para: {obj_name}")
                                count_result = fetch_row_count(
                                    db_path=db_path_input, user=db_user_input, 
                                    password=password_to_use, charset=DEFAULT_DB_CHARSET, 
                                    table_name=obj_name
                                )
                                now_iso_obj = datetime.datetime.now().isoformat()
                                # Atualiza dicionário LOCAL
                                local_counts[obj_name] = {
                                     "count": count_result,
                                     "timestamp": now_iso_obj
                                }
                                if isinstance(count_result, str) and count_result.startswith("Erro"):
                                     error_occurred = True
                                     logger.warning(f"Erro ao contar {obj_name}: {count_result}")
                        
                        # Atualiza ESTADO DA SESSÃO com todos os resultados APÓS o loop
                        st.session_state.overview_row_counts = local_counts
                        # Salva as contagens do estado da sessão
                        save_overview_counts(st.session_state.overview_row_counts, OVERVIEW_COUNTS_FILE)
                        
                        # Atualiza timestamp geral apenas se não houve erro
                        if not error_occurred:
                             now_iso_all = datetime.datetime.now().isoformat()
                             st.session_state.run_times['last_row_count_all'] = now_iso_all
                             save_run_times(st.session_state.run_times, RUN_TIMES_FILE)
                             progress_bar.progress(1.0, text="Contagem concluída!")
                             st.toast("Contagem de linhas concluída e salva!", icon="✅")
                        else:
                             progress_bar.progress(1.0, text="Contagem concluída (com erros).")
                             st.warning("Contagem concluída, mas ocorreram erros (ver logs ou tabela). Timestamp geral não atualizado.")
                        
                        time.sleep(0.1)
                        st.rerun() 
                    
                    # Exibe Última Execução Completa Bem Sucedida
                    last_run_all = st.session_state.run_times.get('last_row_count_all')
                    if last_run_all:
                        try:
                            dt_obj_all = datetime.datetime.fromisoformat(last_run_all)
                            last_run_display_all = dt_obj_all.strftime("%d/%m/%Y %H:%M:%S")
                        except ValueError:
                            last_run_display_all = last_run_all
                        st.caption(f"Última contagem completa OK: {last_run_display_all}")
                    else:
                        st.caption("Contagem completa ainda não foi realizada com sucesso.")

                 with col_act_selected: 
                     # --- Botão Contar SELECIONADOS --- 
                     objects_selected = edited_df[edited_df["Selecionar"] == True]['Objeto'].tolist()
                     
                     if st.button(f"Contar Linhas de {len(objects_selected)} Objeto(s) Selecionado(s)", key="btn_count_selected", disabled=not objects_selected, use_container_width=True):
                          password_to_use_sel = "M@nagers2023"
                          total_sel = len(objects_selected)
                          progress_bar_sel = st.progress(0, text="Iniciando contagem dos selecionados...")
                          error_occurred_sel = False
                          
                          with st.spinner(f"Calculando contagem para {total_sel} objetos selecionados..."):
                              for i, obj_name_sel in enumerate(objects_selected):
                                  progress_text_sel = f"Contando {obj_name_sel}... ({i+1}/{total_sel})"
                                  progress_bar_sel.progress((i + 1) / total_sel, text=progress_text_sel)
                                  logger.info(f"[Contagem Selecionados] Chamando fetch_row_count para: {obj_name_sel}")
                                  count_result_sel = fetch_row_count(
                                      db_path=db_path_input, user=db_user_input, 
                                      password=password_to_use_sel, charset=DEFAULT_DB_CHARSET, 
                                      table_name=obj_name_sel
                                  )
                                  now_iso_sel = datetime.datetime.now().isoformat()
                                  st.session_state.overview_row_counts[obj_name_sel] = {
                                        "count": count_result_sel,
                                        "timestamp": now_iso_sel
                                  }
                                  if isinstance(count_result_sel, str) and count_result_sel.startswith("Erro"):
                                       error_occurred_sel = True
                                       logger.warning(f"[Contagem Selecionados] Erro ao contar {obj_name_sel}: {count_result_sel}")
                           
                          save_overview_counts(st.session_state.overview_row_counts, OVERVIEW_COUNTS_FILE)
                          progress_bar_sel.progress(1.0, text="Contagem dos selecionados concluída!")
                          if not error_occurred_sel:
                               st.toast(f"Contagem para {total_sel} objeto(s) selecionado(s) atualizada!", icon="✅")
                          else:
                               st.warning(f"Contagem concluída para selecionados, mas ocorreram erros.")
                          
                          time.sleep(0.1)
                          st.rerun()
                          
                     st.caption("Marque os objetos na tabela acima para habilitar este botão.")
                 
                 # Adiciona legenda
                 st.subheader("Legenda:")
                 st.markdown("""
                 *   **Objeto:** Nome da Tabela ou View.
                 *   **Tipo:** Indica se é TABLE ou VIEW.
                 *   **Total Colunas:** Número total de colunas no objeto (do schema técnico).
                 *   **Total Linhas:** Contagem real não implementada (N/A).
                 *   **Última Contagem:** Última vez que a contagem foi realizada.
                 *   **Colunas Descritas:** Número de colunas com `description` preenchido nos metadados.
                 *   **Colunas com Notas:** Número de colunas com `value_mapping_notes` preenchido nos metadados.
                 *   **% Descritas / % Com Notas:** Percentuais correspondentes.
                 """)

                 # --- NOVA SEÇÃO: Ranking de Colunas Mais Referenciadas ---
                 st.divider()
                 st.subheader("Colunas Mais Referenciadas (via Chaves Estrangeiras)")
                 st.caption("Ajuda a priorizar a documentação das colunas mais centrais do sistema.")
                 
                 try:
                     # LOG DE DIAGNÓSTICO ADICIONAL:
                     # Substitua 'SUA_TABELA_COM_FK' pelo nome de uma tabela que DEVE ter FKs no JSON
                     tabela_teste = 'TABELA_LOCAL_COBRANCA' # <-- COLOQUE O NOME DA SUA TABELA AQUI
                     if schema_data and tabela_teste in schema_data:
                         fks_teste = schema_data.get(tabela_teste, {}).get('constraints', {}).get('foreign_keys', 'CHAVE_NAO_ENCONTRADA')
                         logger.debug(f"[DIAGNÓSTICO main] Acesso direto a FKs para '{tabela_teste}': {fks_teste}")
                     else:
                         logger.debug(f"[DIAGNÓSTICO main] Tabela de teste '{tabela_teste}' não encontrada no schema_data.")
                     
                     # Chamada original da função
                     fk_counts = calculate_fk_reference_counts(schema_data)
                     if fk_counts:
                         df_fk_counts = pd.DataFrame(fk_counts, columns=['Tabela Referenciada', 'Coluna Referenciada', 'Nº de Referências'])
                         st.dataframe(df_fk_counts, use_container_width=True)
                     else:
                         st.info("Nenhuma referência de chave estrangeira encontrada no esquema técnico para análise.")
                 except Exception as e:
                     logger.error(f"Erro ao calcular ou exibir contagem de referências FK: {e}", exc_info=True)
                     st.error("Ocorreu um erro ao analisar as referências de chaves estrangeiras.")
             except Exception as e:
                 st.error(f"Erro ao gerar a visão geral da documentação: {e}")
                 logger.exception("Erro na seção Visão Geral:")
        else:
             st.warning("Esquema técnico não carregado.")

    elif st.session_state.app_mode == "Explorar Esquema":
        st.title("📝 Anotador de Esquema Firebird") # Título do modo original
        
        # --- Filtro e Seletor de Objetos (Lógica existente) ---
        st.subheader("Filtro de Objetos")
        filter_type = st.radio(
            "Mostrar:",
            ("Todos", "Tabelas", "Views"),
            horizontal=True,
            label_visibility="collapsed"
        )
        # Filtrar object_names com base na seleção do rádio (usando schema_data técnico)
        all_object_names = sorted(list(schema_data.keys()))
        object_names = []
        if filter_type == "Tabelas":
            object_names = [name for name in all_object_names if schema_data[name].get("object_type") == "TABLE"]
        elif filter_type == "Views":
            object_names = [name for name in all_object_names if schema_data[name].get("object_type") == "VIEW"]
        else: # "Todos"
            # Inclui apenas TABLE e VIEW (ignora outros tipos que possam existir no schema_data)
            object_names = [name for name in all_object_names if schema_data[name].get("object_type") in ["TABLE", "VIEW"]]

        if not object_names:
            st.warning(f"Nenhum objeto do tipo '{filter_type}' encontrado no arquivo de esquema.")
            st.stop()
        
        selected_object = st.selectbox("Selecione uma Tabela ou View para anotar:", object_names)
        
        # --- Lógica ao Selecionar Objeto (Lógica existente adaptada) ---
        if selected_object:
            if st.session_state.get('last_displayed_sample_object') != selected_object:
                st.session_state.sample_display_df = None
                st.session_state.last_displayed_sample_object = selected_object

            object_info = schema_data[selected_object] # Info técnica
            object_type = object_info.get("object_type", "TABLE")
            key_type = object_type + "S" # Para acessar metadados
            
            st.header(f"Anotando: `{selected_object}` ({object_type})")
            st.divider()
            
            # --- Seção de Amostra de Dados (com botões) ---
            with st.expander(f"Visualizar Amostra de Dados de '{selected_object}'", expanded=False):
                st.write("**Buscar Amostra:**")
                sample_sizes = [10, 50, 100, 500, 1000, 5000]
                cols_buttons = st.columns(len(sample_sizes))
                password_to_use = "M@nagers2023"
                for i, size in enumerate(sample_sizes):
                    with cols_buttons[i]:
                        if st.button(f"{size} linhas", key=f"btn_fetch_display_sample_{size}_{selected_object}", use_container_width=True):
                            with st.spinner(f"Buscando {size} linhas para visualização..."):
                                st.session_state.sample_display_df = fetch_sample_data(
                                    db_path=db_path_input, user=db_user_input, password=password_to_use,
                                    charset=DEFAULT_DB_CHARSET, table_name=selected_object,
                                    sample_size=size
                                )
                                st.rerun()
                st.divider()
                if isinstance(st.session_state.get('sample_display_df'), pd.DataFrame):
                    df_to_show = st.session_state.sample_display_df
                    st.info(f"Exibindo {len(df_to_show)} linhas.")
                    if not df_to_show.empty:
                        st.dataframe(df_to_show, use_container_width=True)
                    else:
                        st.info("A consulta da amostra não retornou linhas.")
                elif isinstance(st.session_state.get('sample_display_df'), str):
                    st.error(f"Erro ao buscar amostra: {st.session_state.sample_display_df}")
                else:
                    st.caption("Clique em um dos botões acima para buscar e exibir uma amostra dos dados.")
            st.divider()

            # --- Busca de Amostra para Exemplos (Lógica existente) ---
            sample_data_key = f"sample_data_{selected_object}"
            if sample_data_key not in st.session_state:
                 logger.info(f"Buscando amostra para {selected_object} pela primeira vez nesta sessão.")
                 password_to_use_ex = "M@nagers2023"
                 with st.spinner(f"Buscando amostra de dados para {selected_object}..."):
                     st.session_state[sample_data_key] = fetch_sample_data(
                        db_path=db_path_input, user=db_user_input, password=password_to_use_ex,
                        charset=DEFAULT_DB_CHARSET, table_name=selected_object, sample_size=DEFAULT_SAMPLE_SIZE
                     )
            sample_df = st.session_state.get(sample_data_key, None)
            # Removida exibição duplicada da amostra preview aqui

            # --- Anotação da Tabela/View (Lógica existente) ---
            st.subheader("Descrição Geral do Objeto")
            if key_type not in st.session_state.metadata: st.session_state.metadata[key_type] = {}
            if selected_object not in st.session_state.metadata[key_type]: st.session_state.metadata[key_type][selected_object] = {}
            if 'description' not in st.session_state.metadata[key_type][selected_object]: st.session_state.metadata[key_type][selected_object]['description'] = ''
            if 'COLUMNS' not in st.session_state.metadata[key_type][selected_object]: st.session_state.metadata[key_type][selected_object]['COLUMNS'] = {}
            col1, col2 = st.columns([4, 1])
            with col1:
                table_desc_key = f"desc_{object_type}_{selected_object}"
                st.session_state.metadata[key_type][selected_object]['description'] = st.text_area(
                    label="Descreva o propósito desta tabela/view:",
                    value=st.session_state.metadata[key_type][selected_object]['description'],
                    key=table_desc_key, height=100)
            with col2:
                 if st.button("Sugerir (IA)", key=f"btn_ai_desc_{selected_object}", use_container_width=True):
                    col_names_list = [col.get('name', '') for col in object_info.get('columns', [])]
                    prompt_object = (
                        f"Sugira uma descrição concisa em português brasileiro para um(a) {object_type} de banco de dados "
                        f"chamado(a) '{selected_object}'. As colunas são: {', '.join(col_names_list[:10])}... "
                        f"Foque no propósito provável do negócio. Responda apenas com a descrição sugerida.")
                    suggestion = generate_ai_description(prompt_object)
                    if suggestion:
                        st.session_state.metadata[key_type][selected_object]['description'] = suggestion
                        st.rerun()

            # --- Botão Sugerir Todas as Colunas (Lógica existente) ---
            st.markdown("--- Sugestão para Todas as Colunas ---")
            if st.button("Sugerir Todas as Colunas (IA)", key=f"btn_ai_all_cols_{selected_object}", help="Pede sugestões de descrição para todas as colunas listadas abaixo."):
                 columns_to_process = object_info.get('columns', [])
                 if not columns_to_process: st.toast("Nenhuma coluna encontrada para gerar sugestões.")
                 else:
                     progress_bar = st.progress(0, text="Gerando sugestões para colunas...")
                     total_cols = len(columns_to_process)
                     for i, col in enumerate(columns_to_process):
                         col_name = col.get('name'); col_type = col.get('type')
                         if not col_name or not col_type: continue
                         progress_text = f"Gerando sugestões para colunas... ({i+1}/{total_cols}: {col_name})"
                         progress_bar.progress((i + 1) / total_cols, text=progress_text)
                         prompt_column = (f"Sugira uma descrição concisa em português brasileiro para a coluna '{col_name}' ({col_type}) do objeto '{selected_object}' ({object_type}). Foque no significado. Responda só a descrição.")
                         suggestion = generate_ai_description(prompt_column)
                         if suggestion: st.session_state.metadata.setdefault(key_type, {}).setdefault(selected_object, {}).setdefault('COLUMNS', {}).setdefault(col_name, {})['description'] = suggestion
                         else: logger.warning(f"Não foi possível gerar sugestão para a coluna {col_name}")
                     progress_bar.progress(1.0, text="Sugestões concluídas!"); st.toast("Sugestões geradas!"); time.sleep(1); st.rerun()


            # --- Anotação das Colunas (Lógica existente) ---
            st.subheader("Colunas, Exemplos e Descrições")
            if object_info.get('columns'):
                object_columns_metadata = st.session_state.metadata[key_type][selected_object]['COLUMNS']
                for col in object_info['columns']:
                    col_name = col['name']; col_type = col['type']; col_nullable = col['nullable']
                    if col_name not in object_columns_metadata: object_columns_metadata[col_name] = {}
                    current_col_metadata = object_columns_metadata[col_name]
                    if 'description' not in current_col_metadata: current_col_metadata['description'] = ''
                    if 'value_mapping_notes' not in current_col_metadata: current_col_metadata['value_mapping_notes'] = ''
                    
                    # Cria uma chave única para estado dentro do expander
                    col_key_prefix = f"col_{selected_object}_{col_name}"

                    # --- Bloco ÚNICO para cada coluna --- 
                    with st.expander(f"**{col_name}** (`{col_type}`)", expanded=False):
                        # 1. Exibir tipo, nullability e explicação
                        type_explanation = get_type_explanation(col_type)
                        markdown_string = f"`{col_type}`{' - *NOT NULL*' if not col_nullable else ''}"
                        if type_explanation: markdown_string += f" - {type_explanation}"
                        st.markdown(markdown_string)
                        
                        # 2. Exibir exemplos da amostra
                        if sample_df is not None and not sample_df.empty and col_name in sample_df.columns:
                            try:
                                unique_values = sample_df[col_name].dropna().unique()
                                examples = [str(v) for v in unique_values[:5]] # Aumentado para 5 exemplos
                                if examples: st.caption(f"Exemplos: `{'`, `'.join(examples)}`")
                                else: st.caption("Exemplos: (Amostra sem valores não nulos)")
                            except Exception as e: logger.warning(f"Erro ao processar exemplos: {e}"); st.caption("Exemplos: (Erro)")
                        else: st.caption("Exemplos: (Amostra não disponível)")
                        st.markdown("--- Descrição --- ") # Separador

                        # 3. Lógica da Descrição (Heurística + Input + Botões)
                        col_desc_key = f"{col_key_prefix}_desc_input" # Chave única para o input text_area
                        current_col_desc_saved = current_col_metadata.get('description', '') # Descrição salva
                        description_value_to_display = current_col_desc_saved # Valor inicial é o salvo
                        heuristic_source = None # Fonte da heurística

                        # Aplica heurística SE a descrição salva estiver vazia
                        if not current_col_desc_saved:
                            existing_desc, source = find_existing_description(
                                st.session_state.metadata, schema_data, selected_object, col_name
                            )
                            if existing_desc:
                                description_value_to_display = existing_desc # Usa sugestão como valor inicial
                                heuristic_source = source
                                logger.info(f"Preenchendo '{selected_object}.{col_name}' com sugestão via {source}")
                        
                        # Exibe legenda SE a heurística foi aplicada (e descrição salva estava vazia)
                        if heuristic_source:
                            st.caption(f"ℹ️ Sugestão preenchida ({heuristic_source}). Você pode editar abaixo.")
                        
                        # Layout para Área de Texto da Descrição e Botões
                        col_desc_area, col_btn_area = st.columns([4, 1])
                        
                        with col_desc_area:
                             # Área de texto principal para descrição
                             # O valor inicial é `description_value_to_display`
                             # Usa on_change para atualizar o estado real (metadata)
                             st.text_area(
                                 label=f"Descrição para `{col_name}`:",
                                 value=description_value_to_display, 
                                 key=col_desc_key, # Chave única para este widget
                                 label_visibility="collapsed",
                                 height=75,
                                 on_change=lambda k, obj_type_key, obj, col: st.session_state.metadata[obj_type_key][obj]['COLUMNS'][col].update({'description': st.session_state[k]}),
                                 args=(col_desc_key, key_type, selected_object, col_name) # Passa args para on_change
                             )
                        
                        with col_btn_area:
                            # Botão Sugerir IA (lógica existente)
                            ai_btn_key = f"btn_ai_col_{col_key_prefix}"
                            if st.button("Sugerir IA", key=ai_btn_key, use_container_width=True):
                                prompt_column = (f"Sugira descrição concisa pt-br para coluna '{col_name}' ({col_type}) do objeto '{selected_object}' ({object_type}). Significado? Responda só descrição.")
                                suggestion = generate_ai_description(prompt_column)
                                if suggestion:
                                    # Atualiza o estado diretamente e força rerun para exibir
                                    st.session_state.metadata[key_type][selected_object]['COLUMNS'][col_name]['description'] = suggestion
                                    st.rerun()
                            
                            # Botão Propagar Descrição (agora no lugar certo)
                            # Condição usa o valor QUE ESTÁ no text_area (via sua chave)
                            # OU o valor inicial que seria exibido se não houvesse edição
                            effective_description = st.session_state.get(col_desc_key, description_value_to_display) 
                            if effective_description and effective_description.strip(): 
                                propagate_key = f"propagate_{col_key_prefix}"
                                if st.button("Propagar 🔁", key=propagate_key, help="Preenche esta descrição em colunas vazias equivalentes (mesmo nome ou mesma referência FK) em todo o schema.", use_container_width=True):
                                    # Pega a descrição ATUALMENTE SALVA no estado para propagar
                                    description_to_propagate = st.session_state.metadata[key_type][selected_object]['COLUMNS'][col_name].get('description', '')
                                    
                                    if not description_to_propagate.strip():
                                        st.toast("Nada para propagar. Descrição salva está vazia.", icon="⚠️")
                                    else:
                                        source_concept = get_column_concept(schema_data, selected_object, col_name)
                                        propagated_count = 0
                                        logger.info(f"[PROPAGATE] Iniciando propagação de '{selected_object}.{col_name}' (Conceito Fonte: {source_concept}): \"{description_to_propagate}\"")

                                        # Iterar sobre todos os objetos e colunas nos metadados
                                        for obj_type_prop in ['TABLES', 'VIEWS', 'DESCONHECIDOS']:
                                            if obj_type_prop not in st.session_state.metadata: continue # Pula se não existir
                                            for obj_name_prop, obj_meta_prop in st.session_state.metadata[obj_type_prop].items():
                                                if 'COLUMNS' not in obj_meta_prop: continue # Pula se não tiver colunas
                                                for col_name_prop, col_meta_prop in obj_meta_prop['COLUMNS'].items():
                                                    # Pular a coluna original
                                                    if obj_name_prop == selected_object and col_name_prop == col_name:
                                                        continue
                                                    
                                                    # LOG: Coluna alvo sendo checada
                                                    logger.debug(f"[PROPAGATE] Checando '{obj_name_prop}.{col_name_prop}'")
                                                    
                                                    # Verificar se a descrição está vazia
                                                    target_desc = col_meta_prop.get('description', '').strip()
                                                    is_target_empty = not target_desc
                                                    logger.debug(f"[PROPAGATE]   Descrição vazia? {is_target_empty} (Valor: '{target_desc}')")

                                                    if is_target_empty:
                                                        # Verificar se o conceito é o mesmo
                                                        target_concept = get_column_concept(schema_data, obj_name_prop, col_name_prop)
                                                        concepts_match = (target_concept == source_concept)
                                                        # LOG: Conceito alvo e resultado da comparação
                                                        logger.debug(f"[PROPAGATE]   Conceito Alvo: {target_concept}")
                                                        logger.debug(f"[PROPAGATE]   Comparando Conceitos: {target_concept} == {source_concept} -> {concepts_match}")

                                                        if concepts_match:
                                                            # Propagar a descrição
                                                            st.session_state.metadata[obj_type_prop][obj_name_prop]['COLUMNS'][col_name_prop]['description'] = description_to_propagate
                                                            propagated_count += 1
                                                            logger.info(f"[PROPAGATE]     --> DESCRIÇÃO PROPAGADA para '{obj_name_prop}.{col_name_prop}'") # Log INFO se propagou
                                                    else:
                                                         logger.debug(f"[PROPAGATE]   Descrição vazia, mas conceitos não batem.")
                                        
                                        # Mensagem final (existente)
                                        if propagated_count > 0: st.toast(f"Descrição propagada para {propagated_count} coluna(s) vazia(s).", icon="✅")
                                        else: st.toast("Nenhuma coluna vazia correspondente encontrada.", icon="ℹ️")

                        # 4. Notas de Mapeamento de Valores
                        st.markdown("--- Mapeamento de Valores --- ") # Separador
                        map_key = f"map_notes_{col_key_prefix}"
                        st.text_area(
                             label=f"Notas mapeamento `{col_name}`:", 
                             value=current_col_metadata.get('value_mapping_notes', ''), 
                             key=map_key, 
                             label_visibility="collapsed", 
                             height=75,
                             on_change=lambda k, obj_type_key, obj, col: st.session_state.metadata[obj_type_key][obj]['COLUMNS'][col].update({'value_mapping_notes': st.session_state[k]}),
                             args=(map_key, key_type, selected_object, col_name)
                        )
                        # Removido st.divider() dentro do loop, pode ficar muito carregado
                        # Adicionar um fora do expander se necessário

            else: st.write("Nenhuma coluna definida.")
            st.divider() # Separador após todas as colunas do objeto

            # --- Exibição de Constraints (Lógica existente) ---
            if object_info.get('constraints') or object_type == "TABLE":
                 st.subheader("Constraints" + (" (Info)" if object_type == "VIEW" else ""))
                 display_constraints(object_info.get('constraints'))


if __name__ == "__main__":
    main() 