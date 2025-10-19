from datetime import datetime, timedelta
from user_whitelist import load_whitelist
import pyodbc
from config import BACKUP_DATE_THRESHOLD_WEEKS, BACKUP_LAST_HOURS, BACKUP_INTERVAL_HOURS

def check_backup_frequency(database, backups):
    """
    Funcionalidade 2:
    - Verifica se foi feito ao menos um backup autorizado nas últimas {BACKUP_LAST_N_HOURS} horas.
    - Verifica se o espaço de tempo entre os backups é superior a BACKUPS_INTERVAL_HOURS.
    - Só tem em conta backups feitos por utilizadores autorizados (conforme whitelist).
    """
    anomalies = []
    now = datetime.now()
    allowed_prefixes = load_whitelist()

    # Uso o instance_name dos backups
    current_instance = (backups[0].instance_name
                        if backups and hasattr(backups[0], 'instance_name') and backups[0].instance_name
                        else "Desconhecido")

    # Filtra os backups: não copy_only e com utilizadores autorizados (case insensitive)
    filtered = [
        b for b in backups
        if (not b.is_copy_only)
           and any(b.user_name.lower().startswith(prefix.lower()) for prefix in allowed_prefixes)
    ]

    if not filtered:
        anomalies.append({
            'database': database,
            'device': '',
            'user': '',
            'instance': current_instance,
            'type': 'General',
            # Usa o BACKUP_DATE_THRESHOLD_WEEKS vindo do config (para exibição de semanas, se aplicável)
            'issues': [f"Nenhum backup autorizado feito nas últimas {BACKUP_DATE_THRESHOLD_WEEKS} semana(s)"],
            'timestamp': now
        })
        return anomalies

    # Se não houver backup nas últimas BACKUP_LAST_HOURS horas, adiciona uma anomalia
    if all(b.backup_finish_date < now - timedelta(hours=BACKUP_LAST_HOURS) for b in filtered):
        anomalies.append({
            'database': database,
            'device': '',
            'user': '',
            'instance': current_instance,
            'type': 'General',
            'issues': [f"Nenhum backup autorizado feito nas últimas {BACKUP_LAST_HOURS} horas"],
            'timestamp': now
        })

    # Ordena os backups por backup_finish_date ascendente
    sorted_backups = sorted(filtered, key=lambda b: b.backup_finish_date)

    # Verifica o intervalo entre backups consecutivos (usando BACKUPS_INTERVAL_HOURS)
    for i in range(1, len(sorted_backups)):
        prev = sorted_backups[i - 1]
        curr = sorted_backups[i]
        gap = curr.backup_finish_date - prev.backup_finish_date
        if gap > timedelta(hours=BACKUP_INTERVAL_HOURS):
            anomalies.append({
                'database': database,
                'device': curr.physical_device_name,
                'user': '',
                'instance': current_instance,
                'type': curr.backup_type,
                'issues': [f'Intervalo demasiado longo: {str(gap)}'],
                'timestamp': curr.backup_finish_date
            })

    return anomalies
