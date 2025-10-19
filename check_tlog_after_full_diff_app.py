import datetime
from db_utils import connect_to_db_server, get_user_databases, get_backups
from check_tlog_after_full_diff import check_tlog_after_full_diff
from server_manager import read_server_list
from user_whitelist import create_whitelist_file_if_not_exists
from anomaly_log import log_anomalies, log_critical_anomalies
from concurrent.futures import ThreadPoolExecutor
import threading

PRINT_LOCK = threading.Lock()

def process_server(srv):
    server = srv["server"]
    username = srv["username"]
    password = srv["password"]
    local_anomalias = []
    with PRINT_LOCK:
        print(f"\n=== A ligar ao servidor: {server} ===")
    conn = None
    try:
        conn = connect_to_db_server(server, username, password)
        cursor = conn.cursor()
        # Inicializa instance com o IP e tenta obter o nome da instância via query
        instance = server  # Valor padrão
        try:
            cursor.execute("SELECT @@SERVERNAME")
            result = cursor.fetchone()
            if result and result[0]:
                instance = result[0]
        except Exception as e:
            with PRINT_LOCK:
                print(f"Não foi possível obter o nome da instância no servidor {server}: {e}")
        
        databases = get_user_databases(cursor)
        if not databases:
            with PRINT_LOCK:
                print(f"Nenhuma base de dados encontrada no servidor {instance}.")
        else:
            with PRINT_LOCK:
                print(f"Servidor {instance}: {len(databases)} bases de dados encontradas. A executar funcionalidades...")
            # Para cada base, verifica as anomalias de TLOG
            for db in databases:
                backups = get_backups(cursor, db)
                if not backups:
                    continue
                anomalies = check_tlog_after_full_diff(db, backups, cursor)
                if anomalies:
                    # Atualiza o campo 'instance' caso não esteja definido, usando o nome obtido
                    for anomaly in anomalies:
                        if not anomaly.get("instance"):
                            anomaly["instance"] = instance.upper()
                    local_anomalias.extend(anomalies)
        conn.close()
    except Exception as e:
        with PRINT_LOCK:
            print(f"Erro ao ligar ao servidor {server}: {e}")
        connection_anomaly = {
            'database': 'N/A',
            'type': 'Connection Error',
            'device': 'N/A',
            'user': 'N/A',
            'instance': server.upper(),
            'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'issues': [f"Erro ao ligar ao servidor {server}: {e}"],
            'level': 0
        }
        local_anomalias.append(connection_anomaly)
    return local_anomalias

def main():
    # Assegura que o ficheiro whitelist.txt existe
    create_whitelist_file_if_not_exists()
    
    # Lê a lista de servidores
    servers = read_server_list()
    if not servers:
        with PRINT_LOCK:
            print("Nenhuma instância definido no ficheiro 'servers.txt'.")
        return
    
    overall_anomalias = []  # Agrega todas as anomalias de todos os servidores
    
    # Processa os servidores em paralelo
    with ThreadPoolExecutor() as executor:
        results = executor.map(process_server, servers)
        for res in results:
            overall_anomalias.extend(res)
    
    # Exibe os relatórios agrupados por instância
    for srv in servers:
        server_upper = srv["server"].upper()
        server_anomalias = [anomalia for anomalia in overall_anomalias if anomalia.get("instance") == server_upper]
        if server_anomalias:
            with PRINT_LOCK:
                print("\n" + "=" * 40)
                print(f"Relatório de Anomalias - Instância: {server_upper}")
                print("=" * 40 + "\n")
                for idx, anomaly in enumerate(server_anomalias, 1):
                    print(f"{idx}. Base de Dados: {anomaly.get('database', 'N/A')}")
                    print(f"   Tipo de Backup: {anomaly.get('type', 'N/A')}")
                    print(f"   Dispositivo: {anomaly.get('device', 'N/A')}")
                    print(f"   Utilizador: {anomaly.get('user', 'N/A')}")
                    print(f"   Instância: {anomaly.get('instance', 'N/A')}")
                    print(f"   Timestamp: {anomaly.get('timestamp', 'N/A')}")
                    print("   Problemas detectados:")
                    for issue in sorted(set(anomaly.get('issues', []))):
                        print(f"    - {issue}")
                    print()
            with PRINT_LOCK:
                print(f"Total de anomalias para a instância {server_upper}: {len(server_anomalias)}\n")
        else:
            with PRINT_LOCK:
                print(f"\nNenhuma anomalia detectada para a instância {server_upper}.\n")
    
    # Exibe o resumo geral de anomalias
    with PRINT_LOCK:
        print("\n========================================")
        print(f"Total de anomalias detetadas: {len(overall_anomalias)}")
        print("========================================\n")
    
    # Registra os logs das anomalias
    log_anomalies(overall_anomalias)
    # Se necessário, para anomalias críticas:
    critical_anomalias = [anomalia for anomalia in overall_anomalias if int(anomalia.get("level", 1)) == 0]
    log_critical_anomalies(critical_anomalias)

if __name__ == "__main__":
    main()