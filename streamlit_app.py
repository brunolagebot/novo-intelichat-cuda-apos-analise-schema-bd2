import streamlit as st
import json
import os
import logging
import re # NOVO: Para limpar tipo
from collections import OrderedDict, defaultdict # NOVO: defaultdict

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

# --- Interface Streamlit --- MODIFICAÇÕES ABAIXO
st.set_page_config(layout="wide") # Usa layout mais largo
st.title("📝 Editor de Metadados do Schema (com Contexto Técnico)") # Título atualizado
st.caption(f"Editando o arquivo: `{METADATA_FILE}` | Contexto técnico de: `{TECHNICAL_SCHEMA_FILE}`")

# Carrega o schema técnico uma vez
technical_schema_data = load_technical_schema(TECHNICAL_SCHEMA_FILE)
if technical_schema_data is None:
    st.stop()

# Inicializar o estado da sessão se ainda não existir
if 'metadata' not in st.session_state:
    st.session_state.metadata = load_metadata(METADATA_FILE)
    if st.session_state.metadata is None:
        st.stop()

# Referência local para facilitar o acesso
metadata_dict = st.session_state.metadata

# --- Seleção do Objeto --- (MODIFICADO: Usa técnico como base)

# 1. Obter todos os nomes e tipos do schema técnico
all_technical_objects = {}
for name, data in technical_schema_data.items():
    obj_type = data.get('object_type')
    if obj_type in ["TABLE", "VIEW"]:
        all_technical_objects[name] = obj_type
    # Ignora outros tipos que possam existir

if not all_technical_objects:
    st.error("Nenhuma tabela ou view encontrada no arquivo de schema técnico.")
    st.stop()

# 2. Categorizar por tipo para os seletores
object_types_available = sorted(list(set(all_technical_objects.values())))
selected_type_display = st.radio(
    "Filtrar por Tipo:", 
    ["Todos"] + object_types_available, 
    horizontal=True, 
    index=0 # Começa mostrando Todos
)

# 3. Filtrar a lista de nomes baseada no tipo selecionado
if selected_type_display == "Todos":
    object_names = sorted(list(all_technical_objects.keys()))
elif selected_type_display in object_types_available:
    # object_types_available contém 'TABLE' ou 'VIEW'
    object_names = sorted([name for name, type in all_technical_objects.items() if type == selected_type_display])
else:
    object_names = [] # Caso inesperado

if not object_names:
    st.warning(f"Nenhum objeto do tipo '{selected_type_display}' encontrado no schema técnico.")
    selected_object = None
else:
    selected_object = st.selectbox("Selecione o Objeto", object_names)

st.divider()

# --- Edição dos Metadados --- MODIFICADO para garantir a criação da estrutura de metadados
if selected_object:
    # Determina o tipo técnico REAL do objeto selecionado
    selected_object_technical_type = all_technical_objects.get(selected_object)
    # Determina a chave a ser usada/criada no dicionário de metadados (ex: TABLES, VIEWS)
    metadata_key_type = selected_object_technical_type + "S" if selected_object_technical_type else None

    # Pega dados técnicos (já garantido que existe)
    tech_obj_data = technical_schema_data.get(selected_object)

    # Garante que a estrutura exista nos metadados ANTES de tentar acessá-la
    if metadata_key_type and metadata_key_type not in metadata_dict:
        metadata_dict[metadata_key_type] = OrderedDict()
        logger.info(f"Estrutura '{metadata_key_type}' criada nos metadados.")
    if metadata_key_type and selected_object not in metadata_dict[metadata_key_type]:
         metadata_dict[metadata_key_type][selected_object] = OrderedDict()
         metadata_dict[metadata_key_type][selected_object]['description'] = "" # Inicializa descrição
         metadata_dict[metadata_key_type][selected_object]['COLUMNS'] = OrderedDict() # Inicializa colunas
         logger.info(f"Entrada para '{selected_object}' criada em '{metadata_key_type}'.")

    # Dados de metadados para edição (agora garantido que existe a estrutura básica)
    obj_data = metadata_dict.get(metadata_key_type, {}).get(selected_object, {})
    
    if not tech_obj_data:
        st.error(f"Erro: Dados técnicos não encontrados para '{selected_object}' em '{TECHNICAL_SCHEMA_FILE}'. Pulando edição.")
    else:
        st.header(f"Editando: `{selected_object}` ({tech_obj_data.get('object_type', 'Desconhecido')})", divider='rainbow')

        col1, col2 = st.columns([1, 2])

        with col1:
            st.subheader("Descrição do Objeto")
            obj_desc_key = f"desc_{selected_object_technical_type}_{selected_object}"
            if "description" not in obj_data: obj_data["description"] = ""
            
            # Layout para descrição e botão IA
            desc_obj_area, btn_ai_obj_area = st.columns([4, 1])
            with desc_obj_area:
                new_obj_desc = st.text_area(
                    "Descrição Geral",
                    value=obj_data.get("description", ""),
                    key=obj_desc_key,
                    height=100,
                    help="Descreva o propósito geral desta tabela ou view."
                )
                obj_data["description"] = new_obj_desc # Atualiza estado imediatamente
            with btn_ai_obj_area:
                 if st.button("Sugerir IA", key=f"btn_ai_obj_{selected_object}", use_container_width=True, disabled=not OLLAMA_AVAILABLE):
                    tech_col_names = [c.get('name', '') for c in tech_obj_data.get('columns', [])]
                    prompt_object = (
                        f"Sugira descrição concisa pt-br para {tech_obj_data.get('object_type', 'objeto')} BD "
                        f"'{selected_object}'. Colunas: {', '.join(tech_col_names[:10])}... "
                        f"Propósito provável? Responda só a descrição.")
                    suggestion = generate_ai_description(prompt_object)
                    if suggestion:
                        st.session_state.metadata[metadata_key_type][selected_object]['description'] = suggestion
                        st.rerun()

        with col2:
            if "COLUMNS" not in obj_data or not isinstance(obj_data["COLUMNS"], dict):
                obj_data["COLUMNS"] = OrderedDict()
                st.warning("Estrutura 'COLUMNS' inicializada.")

            st.subheader("Descrição das Colunas")
            columns_dict_meta = obj_data["COLUMNS"] # Metadados das colunas para edição
            technical_columns = tech_obj_data.get("columns", []) # Lista de colunas técnicas

            if not technical_columns:
                st.write("*Nenhuma coluna definida neste objeto no schema técnico.*")
            else:
                # Iterar sobre as colunas TÉCNICAS
                technical_column_names = sorted([c['name'] for c in technical_columns if 'name' in c])
                column_tabs = st.tabs(technical_column_names)

                for i, col_name in enumerate(technical_column_names):
                    with column_tabs[i]:
                        # Encontrar dados técnicos da coluna atual
                        tech_col_data = next((c for c in technical_columns if c['name'] == col_name), None)
                        if not tech_col_data:
                            st.warning(f"Dados técnicos não encontrados para coluna {col_name}")
                            continue

                        # Garantir entrada no metadata para edição (já feito acima para o objeto, agora para a coluna)
                        columns_dict_meta = obj_data.setdefault('COLUMNS', OrderedDict())
                        if col_name not in columns_dict_meta:
                             columns_dict_meta[col_name] = OrderedDict()
                        col_meta_data = columns_dict_meta[col_name]
                        if "description" not in col_meta_data: col_meta_data["description"] = ""
                        if "value_mapping_notes" not in col_meta_data: col_meta_data["value_mapping_notes"] = ""
                        
                        # Exibir Informações Técnicas
                        col_type = tech_col_data.get('type', 'N/A')
                        col_nullable = tech_col_data.get('nullable', True) # Default to True if missing
                        type_explanation = get_type_explanation(col_type)
                        st.markdown(f"**Tipo Técnico:** `{col_type}` {type_explanation} | **Anulável:** {'Sim' if col_nullable else 'Não'}")
                        st.markdown("--- Descrição --- ")

                        # Descrição (Heurística + Edição + IA + Propagar)
                        col_desc_key = f"desc_{selected_object_technical_type}_{selected_object}_{col_name}"
                        col_notes_key = f"notes_{selected_object_technical_type}_{selected_object}_{col_name}"
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
                                f"Descrição da Coluna `{col_name}`",
                                value=description_value_to_display, # Valor inicial pode ser heurístico
                                key=col_desc_key,
                                height=75,
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
                            if st.button("Sugerir IA", key=f"btn_ai_col_{col_name}", use_container_width=True, disabled=not OLLAMA_AVAILABLE):
                                prompt_column = (f"Sugira descrição concisa pt-br para coluna '{col_name}' ({col_type}) do objeto '{selected_object}'. Significado? Responda só descrição.")
                                suggestion = generate_ai_description(prompt_column)
                                if suggestion:
                                    # USA metadata_key_type
                                    st.session_state.metadata[metadata_key_type][selected_object]['COLUMNS'][col_name]['description'] = suggestion
                                    st.rerun()
                            
                            # Botão Propagar
                            description_to_propagate = col_meta_data.get('description', '').strip()
                            if description_to_propagate:
                                if st.button("Propagar 🔁", key=f"propagate_{col_name}", help="Preenche esta descrição em colunas vazias equivalentes", use_container_width=True):
                                    source_concept = get_column_concept(technical_schema_data, selected_object, col_name)
                                    propagated_count = 0
                                    # Iterar sobre todos os objetos e colunas nos metadados para propagar
                                    for obj_type_prop in st.session_state.metadata:
                                        if obj_type_prop == "_GLOBAL_CONTEXT": continue
                                        for obj_name_prop, obj_meta_prop in st.session_state.metadata[obj_type_prop].items():
                                            # Garante que o objeto exista no schema tecnico para a heuristica
                                            if obj_name_prop not in technical_schema_data: continue
                                            if 'COLUMNS' not in obj_meta_prop: continue
                                            for col_name_prop, col_meta_prop in obj_meta_prop['COLUMNS'].items():
                                                if obj_name_prop == selected_object and col_name_prop == col_name: continue
                                                is_target_empty = not col_meta_prop.get('description', '').strip()
                                                if is_target_empty:
                                                    # Usa schema tecnico para a heuristica
                                                    target_concept = get_column_concept(technical_schema_data, obj_name_prop, col_name_prop)
                                                    if target_concept == source_concept:
                                                        # Mas escreve no metadata
                                                        st.session_state.metadata[obj_type_prop][obj_name_prop]['COLUMNS'][col_name_prop]['description'] = description_to_propagate
                                                        propagated_count += 1
                                    if propagated_count > 0: st.toast(f"Descrição propagada para {propagated_count} coluna(s) vazia(s).", icon="✅")
                                    else: st.toast("Nenhuma coluna vazia correspondente encontrada.", icon="ℹ️")

                        # Notas de Mapeamento (sem modificação)
                        st.markdown("--- Notas de Mapeamento de Valor --- ")
                        new_col_notes = st.text_area(
                            f"Notas Mapeamento (`{col_name}`)",
                            value=col_meta_data.get("value_mapping_notes", ""),
                            key=col_notes_key,
                            height=75,
                            help="Explique valores específicos (ex: 1=Ativo) ou formatos."
                        )
                        col_meta_data["value_mapping_notes"] = new_col_notes

        # --- Botão Salvar Geral --- (movido para fora do loop de colunas)
        st.divider()
        if st.button("💾 Salvar Alterações no Arquivo", type="primary"):
            if save_metadata(st.session_state.metadata, METADATA_FILE):
                st.success(f"Metadados salvos com sucesso em `{METADATA_FILE}`!")
                # Limpa cache dos metadados para forçar recarga na próxima interação
                # que precisar deles (como o próprio load_metadata) 
                # Mas não limpa o cache do schema técnico
                try: load_metadata.clear() 
                except Exception as e: logger.warning(f"Erro ao limpar cache de metadados: {e}")
            else:
                st.error("Falha ao salvar os metadados.")

else:
    st.info("Selecione um objeto para visualizar/editar seus metadados.")

# Botão Recarregar (sem modificação)
st.sidebar.header("Ações")
if st.sidebar.button("Recarregar Metadados do Arquivo"):
    load_metadata.clear() # Limpa o cache antes de carregar
    st.session_state.metadata = load_metadata(METADATA_FILE)
    if st.session_state.metadata is not None:
        st.success("Metadados recarregados do arquivo!")
        st.rerun()
    else:
        st.error("Falha ao recarregar metadados.")

# Informação sobre como rodar (sem modificação)
st.sidebar.info("Para executar este app, use o comando: `streamlit run streamlit_app.py` no seu terminal.") 