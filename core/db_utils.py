# Funções de interação com o banco de dados Firebird 

import fdb
import datetime
import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)

# REMOVIDO CACHE - Buscar sob demanda
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