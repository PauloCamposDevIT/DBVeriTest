from datetime import datetime, timedelta
from config import BACKUP_DATE_THRESHOLD_WEEKS   # importa a variável de configuração

def check_file_size(database, backups):
    # Funcionalidade 4:
    # - Verifica se o tamanho do ficheiro do backup é 0.
    # - Só considera backups (full, differential ou TLog) que não são copy_only.
    # - Só vê entradas das últimas BACKUP_DATE_THRESHOLD_WEEKS semanas.

    anomalies = []
    now = datetime.now()
    threshold_date = now - timedelta(weeks=BACKUP_DATE_THRESHOLD_WEEKS)

    for backup in backups:
        # Ignora backups copy_only e backups feitos há mais de BACKUP_DATE_THRESHOLD_WEEKS semanas
        if backup.is_copy_only or backup.backup_finish_date < threshold_date:
            continue

        if backup.backup_size == 0:
            anomalies.append({
                'database': database,
                'device': backup.physical_device_name,
                "user": backup.user_name,
                "instance": backup.instance_name,
                'type': backup.backup_type,
                'issues': ["Tamanho do ficheiro é 0"],
                'timestamp': backup.backup_finish_date
            })

    return anomalies
