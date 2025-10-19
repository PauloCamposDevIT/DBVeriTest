# CONN_STR foi descontinuado; use connect_to_db_server para ligar aos servidores especificados em servers.txt
# CONN_STR = (
#     "DRIVER={SQL Server};"
#    # "SERVER=VanSora-Hybrid\\SQLEXPRESS02;"
#     "DATABASE=master;"
#    # "Trusted_Connection=yes;"
# )
 
# Constantes para verificação de backups
#BACKUP_PATH_PREFIX = 'C:\\Backups\\'
SYSTEM_DATABASES = ['master', 'model', 'msdb', 'tempdb']

# Configurações de SMTP para alertas por email
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "sqlanomalyv@gmail.com"
SMTP_PASSWORD = "jsga zphi ikku ujvo "
ALERT_RECIPIENT = "sqlanomalyv+person1@gmail.com"

# Notas:
# - No Gmail é utilizar uma App Password se a verificação em duas etapas estiver ativada.
# - Para criar uma App Password, é preciso ir à Conta Google -> Segurança -> Passwords de Aplicativos.
# - Se encontrares problemas de login, verifica as configurações de segurança da conta.

# Variável para definir quantas semanas atrás serão incluídas na query de backup
# Configurações para os intervalos de backup
BACKUP_DATE_THRESHOLD_WEEKS = 2            # Exemplo
BACKUP_LAST_HOURS = 24                   # Tempo para verificação de backups recentes
BACKUP_INTERVAL_HOURS = 48                # Intervalo máximo entre backups consecutivos
TLOG_HOURS_THRESHOLD = 24         # Tempo máximo sem TLog para considerar anomalia

# Pasta onde os logs críticos vão ser arquivados
CRITICAL_ARCHIVE_FOLDER = "critical anomaly history log"

# Configurações de conexão
TIMEOUT_SECONDS = 30



