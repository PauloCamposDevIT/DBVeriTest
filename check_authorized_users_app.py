import datetime
from db_utils import connect_to_db_server, get_user_databases, get_backups
from check_authorized_users import check_authorized_users
from server_manager import read_server_list
from user_whitelist import create_whitelist_file_if_not_exists
from anomaly_log import log_anomalies, log_critical_anomalies
from notification import send_alert_email
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
    try:
        conn = connect_to_db_server(server, username, password)
        cursor = conn.cursor()
        databases = get_user_databases(cursor)
        if not databases:
            with PRINT_LOCK:
                print(f"Nenhuma base de dados encontrada no servidor {server}.")
        else:
            with PRINT_LOCK:
                print(f"Servidor {server}: {len(databases)} bases de dados encontradas. A executar funcionalidades...")
            # Para cada base, coleta as anomalias
            for db in databases:
                backups = get_backups(cursor, db)
                if not backups:
                    continue
                anomalies = check_authorized_users(db, backups)
                if anomalies:
                    # Atualiza o campo "instance" para os itens sem valor definido
                    for anomaly in anomalies:
                        if not anomaly.get("instance"):
                            anomaly["instance"] = server.upper()
                    local_anomalias.extend(anomalies)
        conn.close()
    except Exception as e:
        with PRINT_LOCK:
            print(f"Erro ao conectar no servidor {server}: {e}")
        local_anomalias.append({
            'database': 'N/A',
            'type': 'Connection Error',
            'device': 'N/A',
            'user': 'N/A',
            'instance': server.upper(),
            'timestamp': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'issues': [f"Erro ao conectar no servidor {server}: {e}"],
            'level': 0
        })
    return local_anomalias

def main():
    # Garante que o ficheiro whitelist.txt existe
    create_whitelist_file_if_not_exists()

    # Lê a lista de servidores
    servers = read_server_list()
    if not servers:
        print("Nenhum servidor definido no ficheiro 'servers.txt'.")
        return

    overall_anomalias = []  # Agrega todas as anomalias de todos os servidores

    # Processa os servidores em paralelo
    with ThreadPoolExecutor() as executor:
        results = executor.map(process_server, servers)
        for res in results:
            overall_anomalias.extend(res)

    # Exibe os relatórios agrupados por instância
    for srv in servers:
        server = srv["server"].upper()
        server_anomalias = [anomalia for anomalia in overall_anomalias if anomalia.get("instance") == server]
        if server_anomalias:
            with PRINT_LOCK:
                print("\n" + "=" * 40)
                print(f"Relatório de Anomalias - Instância: {server}")
                print("=" * 40 + "\n")
                for idx, anomaly in enumerate(server_anomalias, 1):
                    print(f"{idx}. Base de Dados: {anomaly.get('database', 'N/A')}")
                    print(f"   Tipo de Backup: {anomaly.get('type', 'N/A')}")
                    print(f"   Dispositivo: {anomaly.get('device', 'N/A')}")
                    print(f"   Utilizador: {anomaly.get('user', 'N/A')}")
                    print(f"   Instância: {anomaly.get('instance', 'N/A')}")
                    print(f"   Timestamp: {anomaly.get('timestamp', 'N/A')}")
                    print("   Problemas detectados:")
                    for issue in sorted(set(anomaly.get("issues", []))):
                        print(f"    - {issue}")
                    print()
        else:
            with PRINT_LOCK:
                print(f"\nNenhuma anomalia detectada para a instância {server}.\n")

    # Exibe o total de anomalias detetadas
    with PRINT_LOCK:
        print("\n========================================")
        print(f"Total de anomalias detetadas: {len(overall_anomalias)}")
        print("========================================\n")

    # Registra os logs
    log_anomalies(overall_anomalias)

    # Registra as anomalias críticas (está em comentário o envio automático por e-mail se necessário)
    critical_anomalias = [anomalia for anomalia in overall_anomalias if int(anomalia.get("level", 1)) == 0]
    log_critical_anomalies(critical_anomalias)
    # for anomaly in critical_anomalias:
    #     send_alert_email(anomaly)

if __name__ == "__main__":
    main()