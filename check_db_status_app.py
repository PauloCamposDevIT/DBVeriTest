import datetime
from db_utils import connect_to_db_server, get_user_databases
from check_db_status import check_db_status
from server_manager import read_server_list
from user_whitelist import create_whitelist_file_if_not_exists
from anomaly_log import log_anomalies
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
        databases = get_user_databases(cursor)
        if not databases:
            with PRINT_LOCK:
                print(f"Nenhuma base de dados encontrada no servidor {server}.")
        else:
            with PRINT_LOCK:
                print(f"Servidor {server}: {len(databases)} bases de dados encontradas.")
            # Para cada base, verifica o status (offline ou em modo de emergência/recuperação)
            for db in databases:
                anomalies = check_db_status(db, cursor, server)
                if anomalies:
                    # Atualiza o campo 'instance' para os itens sem valor definido
                    for anomaly in anomalies:
                        if not anomaly.get("instance"):
                            anomaly["instance"] = server.upper()
                    local_anomalias.extend(anomalies)
                else:
                    with PRINT_LOCK:
                        print(f"Base de Dados: {db} está ONLINE e operacional.")
    except Exception as e:
        with PRINT_LOCK:
            print(f"Erro ao conectar ao servidor {server}: {e}")
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
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
    return local_anomalias

def main():
    # Garante que o ficheiro whitelist.txt existe
    create_whitelist_file_if_not_exists()
    
    # Lê a lista de servidores
    servers = read_server_list()
    if not servers:
        print("Nenhum servidor definido no ficheiro 'servers.txt'.")
        return
    
    print("\n=== Verificação do Estado das Bases de Dados ===")
    
    overall_anomalias = []
    
    # Processa os servidores em paralelo
    with ThreadPoolExecutor() as executor:
        results = executor.map(process_server, servers)
        for res in results:
            overall_anomalias.extend(res)
    
    # Exibe os relatórios agrupados por instância
    for srv in servers:
        instance_upper = srv["server"].upper()
        server_anomalias = [anomalia for anomalia in overall_anomalias if anomalia.get("instance") == instance_upper]
        if server_anomalias:
            with PRINT_LOCK:
                print("\n" + "=" * 40)
                print(f"Relatório de Anomalias - Instância: {instance_upper}")
                print("=" * 40 + "\n")
                for idx, anomaly in enumerate(server_anomalias, 1):
                    print(f"{idx}. Base de Dados: {anomaly.get('database', 'N/A')}")
                    print(f"   Tipo de Verificação: {anomaly.get('type', 'N/A')}")
                    print(f"   Dispositivo: {anomaly.get('device', 'N/A')}")
                    print(f"   Utilizador: {anomaly.get('user', 'N/A')}")
                    print(f"   Instância: {anomaly.get('instance', 'N/A')}")
                    print(f"   Timestamp: {anomaly.get('timestamp', 'N/A')}")
                    print("   Problemas detectados:")
                    for issue in sorted(set(anomaly.get('issues', []))):
                        print(f"    - {issue}")
                    print()
            with PRINT_LOCK:
                print(f"Total de anomalias para a instância {instance_upper}: {len(server_anomalias)}\n")
        else:
            with PRINT_LOCK:
                print(f"\nNenhuma anomalia detectada para a instância {instance_upper}.\n")
    
    # Exibe o resumo geral e registra os logs
    if overall_anomalias:
        with PRINT_LOCK:
            print(f"\nTotal geral de anomalias: {len(overall_anomalias)}\n")
        log_anomalies(overall_anomalias)
    else:
        with PRINT_LOCK:
            print("\nTodas as bases de dados estão ONLINE e operacionais!")
    
if __name__ == "__main__":
    main()
