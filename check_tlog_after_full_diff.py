from datetime import datetime, timedelta
import pyodbc
from user_whitelist import load_whitelist
from config import TLOG_HOURS_THRESHOLD

def get_recovery_model(cursor, database):
    try:
        cursor.execute("SELECT recovery_model_desc FROM sys.databases WHERE name = ?", database)
        row = cursor.fetchone()
        return row[0] if row else None
    except Exception:
        return None

# Função auxiliar para buscar TLOGs das últimas 24 horas via SQL
def get_tlogs_after_nhours(cursor, database, hours=TLOG_HOURS_THRESHOLD):
    lower_bound = datetime.now() - timedelta(hours=hours)
    query = """
    SELECT
        bs.database_name,
        bs.backup_start_date,
        bs.backup_finish_date,
        bs.is_copy_only,
        CASE bs.type
            WHEN 'L' THEN 'TLog'
            ELSE bs.type
        END as backup_type,
        bs.user_name,
        bmf.physical_device_name,
        @@SERVERNAME AS instance_name
    FROM msdb.dbo.backupset bs
    INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
    WHERE bs.database_name = ?
      AND bs.type = 'L'
      AND bs.is_copy_only = 0
      AND bs.backup_finish_date >= ?
    ORDER BY bs.backup_finish_date DESC
    """
    cursor.execute(query, database, lower_bound)
    return cursor.fetchall()

# Função auxiliar para buscar TLOGs feitos após um determinado momento
def get_tlogs_after(cursor, database, after_time):
    query = """
    SELECT
        bs.database_name,
        bs.backup_start_date,
        bs.backup_finish_date,
        bs.is_copy_only,
        CASE bs.type
            WHEN 'L' THEN 'TLog'
            ELSE bs.type
        END as backup_type,
        bs.user_name,
        bmf.physical_device_name,
        @@SERVERNAME AS instance_name
    FROM msdb.dbo.backupset bs
    INNER JOIN msdb.dbo.backupmediafamily bmf ON bs.media_set_id = bmf.media_set_id
    WHERE bs.database_name = ?
      AND bs.type = 'L'
      AND bs.is_copy_only = 0
      AND bs.backup_finish_date > ?
    ORDER BY bs.backup_finish_date ASC
    """
    cursor.execute(query, database, after_time)
    return cursor.fetchall()

def check_tlog_after_full_diff(database, backups, cursor):
    """
    Funcionalidade 3:
    - Verifica, via SQL, se há backup TLOG nas últimas 24 horas.
    - Obtém o último backup full ou differential (não copy_only).
    - Se o último backup full/differential tiver sido realizado por um utilizador não autorizado (conforme whitelist),
      gera uma anomalia.
    - Se houver TLOG após esse backup, indica que o TLOG necessita de um backup autorizado.
    - Se o backup full/differential for autorizado, verifica se existe pelo menos um TLOG posterior.
    - Apenas realiza a verificação se a base estiver em modo FULL.
    """
    anomalies = []
    now = datetime.now()
    allowed_prefixes = load_whitelist()

    # Usa o instance_name presente nos backups ou "Desconhecido"
    current_instance = (backups[0].instance_name 
                        if backups and hasattr(backups[0], 'instance_name') and backups[0].instance_name 
                        else "Desconhecido")

    recovery_model = get_recovery_model(cursor, database)
    if recovery_model is None or recovery_model.upper() != 'FULL':
        anomalies.append({
            'database': database,
            'device': '',
            'type': 'General',
            'user': '',
            'instance': current_instance,
            'issues': [f"A base está em modo {recovery_model or 'desconhecido'}. Tem que ser FULL"],
            'timestamp': now
        })
        return anomalies

    # Utiliza SQL para obter TLOGs das últimas TLOG_HOURS_THRESHOLD horas
    tlogs_last_n = get_tlogs_after_nhours(cursor, database, hours=TLOG_HOURS_THRESHOLD)
    if not tlogs_last_n:
        anomalies.append({
            'database': database,
            'device': '',
            'type': 'TLog',
            'user': '',
            'instance': current_instance,
            'issues': [f"Nenhum TLOG feito nas últimas {TLOG_HOURS_THRESHOLD} horas"],
            'timestamp': now
        })

    # Filtra backups (não copy_only) referentes a full/differential
    filtered = [b for b in backups if not b.is_copy_only]
    full_diff = [b for b in filtered if b.backup_type in ['Full backup', 'Differential']]
    if not full_diff:
        return anomalies

    latest_full_diff = max(full_diff, key=lambda b: b.backup_finish_date)

    # Se o último backup full/differential foi feito por um utilizador não autorizado, cria anomalia
    if not any(latest_full_diff.user_name.lower().startswith(prefix.lower()) for prefix in allowed_prefixes):
        anomalies.append({
            'database': database,
            'device': latest_full_diff.physical_device_name,
            'type': latest_full_diff.backup_type,
            'user': latest_full_diff.user_name,
            'instance': latest_full_diff.instance_name,
            'issues': ["Último backup full/differential feito por utilizador não autorizado"],
            'timestamp': latest_full_diff.backup_finish_date
        })
        # Usa SQL para obter TLOGs feitos após o último backup full/differential
        tlogs_after = get_tlogs_after(cursor, database, latest_full_diff.backup_finish_date)
        if tlogs_after:
            first_tlog_after = tlogs_after[0]
            anomalies.append({
                'database': database,
                'device': first_tlog_after.physical_device_name,
                'type': 'TLog',
                'user': first_tlog_after.user_name,
                'instance': first_tlog_after.instance_name,
                'issues': ["TLOG feito depois de um backup feito por utilizador não autorizado; precisa de backup autorizado"],
                'timestamp': first_tlog_after.backup_finish_date
            })
    else:
        # Se o backup full/differential for autorizado, mas não houver TLOG depois, utiliza SQL para confirmar
        tlogs_after = get_tlogs_after(cursor, database, latest_full_diff.backup_finish_date)
        if not tlogs_after:
            anomalies.append({
                'database': database,
                'device': latest_full_diff.physical_device_name,
                'type': latest_full_diff.backup_type,
                'user': latest_full_diff.user_name,
                'instance': latest_full_diff.instance_name,
                'issues': ["Nenhum TLOG encontrado depois do backup full/differential autorizado"],
                'timestamp': latest_full_diff.backup_finish_date
            })

    return anomalies
