import pyodbc
import datetime
from config import SYSTEM_DATABASES, BACKUP_DATE_THRESHOLD_WEEKS, TIMEOUT_SECONDS

# Função descontinuada: usar connect_to_db_server em vez desta.
# def connect_to_db():
#     return pyodbc.connect(CONN_STR, autocommit=True)

def connect_to_db_server(server, username, password):
    """
    Conecta a um servidor específico com base nos parâmetros fornecidos.
    Se 'username' estiver vazio, utiliza Trusted_Connection.
    Utiliza TIMEOUT_SECONDS definido em config.py.
    """
    timeout = f"Connection Timeout={TIMEOUT_SECONDS};"
    if username:
        conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE=master;UID={username};PWD={password};{timeout}"
    else:
        conn_str = f"DRIVER={{SQL Server}};SERVER={server};DATABASE=master;Trusted_Connection=yes;{timeout}"
    return pyodbc.connect(conn_str, autocommit=True)

def get_user_databases(cursor):
    # dá fetch a todas as DBs, exceto as system databases definidas
    cursor.execute("""
        SELECT name FROM sys.databases
        WHERE name NOT IN (?, ?, ?, ?)
    """, SYSTEM_DATABASES)
    return [row.name for row in cursor.fetchall()]

def get_backups(cursor, database):
    # Calcula a data mínima a ser considerada com base na configuração
    min_date = datetime.datetime.now() - datetime.timedelta(weeks=BACKUP_DATE_THRESHOLD_WEEKS)
    # Recupera os backups para a base de dados especificada, realizando a filtração pela data mínima
    cursor.execute("""
        SELECT
            bs.database_name,
            CASE bs.type
                WHEN 'D' THEN 'Full backup'
                WHEN 'I' THEN 'Differential'
                WHEN 'L' THEN 'TLog'
            END AS backup_type,
            bs.backup_start_date,
            bs.backup_finish_date,
            bs.is_copy_only,
            bmf.physical_device_name,
            bs.user_name,
            bs.backup_size,
            @@SERVERNAME AS instance_name
        FROM msdb.dbo.backupset bs
        INNER JOIN msdb.dbo.backupmediafamily bmf
            ON bs.media_set_id = bmf.media_set_id
        WHERE bs.database_name = ?
          AND bs.backup_finish_date >= ?
        ORDER BY bs.backup_finish_date DESC
    """, (database, min_date))
    return cursor.fetchall()
