from datetime import datetime
from user_whitelist import load_whitelist
from revoke_backup_permissions import load_sysadmin_whitelist, remove_from_sysadmin_whitelist

def check_authorized_users(database, backups):
    """
    Funcionalidade 1:
    - Verifica se os backups foram feitos por users autorizados (os que começam com os prefixos na whitelist).
    - Ignora backups copy_only.
    - Se o backup for feito por um utilizador não autorizado (anomalia crítica),
      e esse utilizador estiver presente na sysadminwhitelist, remove-o automaticamente da lista
      para que seja posteriormente sujeito à revogação.
    Nota: A filtragem por data agora é feita na query SQL.
    """
    anomalies = []
    # Pré-processa as listas para evitar conversões repetitivas
    allowed_prefixes = [prefix.lower() for prefix in load_whitelist()]
    sysadmin_whitelist = [user.lower() for user in load_sysadmin_whitelist()]

    for backup in backups:
        if backup.is_copy_only:
            continue

        user_lower = backup.user_name.lower()
        # Verifica se o nome do utilizador começa com algum prefixo autorizado
        if any(user_lower.startswith(prefix) for prefix in allowed_prefixes):
            anomaly_level = 1
            issues = []
        else:
            anomaly_level = 0
            issues = ["Utilizador não autorizado"]
            # Se o utilizador estiver na sysadmin_whitelist, remove-o automaticamente.
            if user_lower in sysadmin_whitelist:
                remove_from_sysadmin_whitelist(backup.user_name)

        if issues:
            anomalies.append({
                'database': database,
                'device': backup.physical_device_name,
                "user": backup.user_name,
                "instance": backup.instance_name,
                'type': backup.backup_type,
                'issues': issues,
                'timestamp': backup.backup_finish_date,
                'level': anomaly_level
            })
    return anomalies
