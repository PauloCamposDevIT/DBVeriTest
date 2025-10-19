import pyodbc
from datetime import datetime

def check_db_status(database, cursor, instance=None):
    """
    Verifica se a base de dados está offline ou em modo emergência/recuperação.
    Se o estado (state_desc) não for 'ONLINE', cria uma anomalia, indicando a instância.
    O parâmetro 'instance' pode ser passado para evitar nova conexão.
    """
    anomalies = []
    now = datetime.now()
    
    # Se não foi informado o nome da instância, obtém pela query
    if instance is None:
        cursor.execute("SELECT @@SERVERNAME")
        instance = cursor.fetchone()[0]
        
    # Obtém o estado e o recovery model da base
    cursor.execute("SELECT state_desc, recovery_model_desc FROM sys.databases WHERE name = ?", database)
    row = cursor.fetchone()
    if row:
        state, recovery_model = row[0], row[1]
        if state.upper() != "ONLINE":
            issues = [f"A base está em estado: '{state}'",
                      f"Recovery model: '{recovery_model}'"]
            anomalies.append({
                'database': database,
                'device': '',
                'user': '',
                'type': 'DB Status',
                'instance': instance,
                'issues': issues,
                'timestamp': now
            })
    else:
        issues = [f"Base de dados '{database}' não encontrada em sys.databases"]
        anomalies.append({
            'database': database,
            'device': '',
            'user': '',
            'type': 'DB Status',
            'instance': instance,
            'issues': issues,
            'timestamp': now
        })
    return anomalies