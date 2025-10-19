import datetime
from db_utils import connect_to_db_server, get_user_databases
from check_volumes import get_all_file_records, classify_volumes, check_file_volume_anomalies
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
    try:
        conn = connect_to_db_server(server, username, password)
        cursor = conn.cursor()
        # Tenta obter o nome da instância via query
        instance = server  # valor padrão (IP)
        try:
            cursor.execute("SELECT @@SERVERNAME")
            result = cursor.fetchone()
            if result and result[0]:
                instance = result[0]
        except Exception as e:
            with PRINT_LOCK:
                print(f"Não foi possível obter o nome da instância no servidor {server}: {e}")
        
        # Utiliza get_all_file_records em vez de get_all_file_paths
        file_records = get_all_file_records(cursor)
        conn.close()

        if not file_records:
            with PRINT_LOCK:
                print(f"Nenhum ficheiro encontrado no servidor {server}.")
            return local_anomalias

        volume_class = classify_volumes(file_records)
        anomalies = check_file_volume_anomalies(volume_class)

        # Transforma as anomalias para o esquema padrão
        transformed_anomalias = []
        for anomaly in anomalies:
            transformed = {
                "database": anomaly.get("database", "N/A"),
                "type": "Verificação de Volume",
                "device": anomaly.get("file_path", ""),
                "user": "",
                "instance": instance.upper(),
                "timestamp": datetime.datetime.now(),
                "issues": [anomaly.get("issue", "")]
            }
            transformed_anomalias.append(transformed)

        with PRINT_LOCK:
            print(f"Servidor {instance}: {len(file_records)} ficheiros encontrados. A executar funcionalidades...")
            if transformed_anomalias:
                print("\n" + "=" * 40)
                print(f"Relatório de Anomalias - Instância: {instance.upper()}")
                print("=" * 40 + "\n")
                for idx, anomalia in enumerate(transformed_anomalias, 1):
                    ts_str = anomalia["timestamp"].strftime("%Y-%m-%d %H:%M:%S.%f")
                    print(f"{idx}. Base de Dados: {anomalia.get('database', 'N/A')}")
                    print(f"   Tipo de Backup: {anomalia.get('type', 'N/A')}")
                    print(f"   Dispositivo: {anomalia.get('device', '')}")
                    print(f"   Utilizador: {anomalia.get('user', '')}")
                    print(f"   Instância: {anomalia.get('instance', 'N/A')}")
                    print(f"   Timestamp: {ts_str}")
                    print("   Problemas detectados:")
                    for issue in set(anomalia.get("issues", [])):
                        print(f"    - {issue}")
                    print()
                print(f"Total de anomalias: {len(transformed_anomalias)}")
            else:
                print(f"\nSem anomalias de volume no servidor {instance.upper()}.")
        
        local_anomalias.extend(transformed_anomalias)
    except Exception as e:
        with PRINT_LOCK:
            print(f"\nErro ao verificar a integridade dos volumes no servidor {server}: {e}")
    return local_anomalias

def main():
    create_whitelist_file_if_not_exists()

    servers = read_server_list()
    if not servers:
        print("Nenhum servidor definido no ficheiro 'servers.txt'.")
        return

    overall_anomalias = []
    print("\n=== Verificação de Integridade dos Volumes (Ficheiros de Dados vs TLOGs) ===")
    
    with ThreadPoolExecutor() as executor:
        results = executor.map(process_server, servers)
        for res in results:
            overall_anomalias.extend(res)

    if overall_anomalias:
        log_anomalies(overall_anomalias)
        print(f"\nTotal geral de anomalias detetadas: {len(overall_anomalias)}")
    else:
        print("\nNão foram encontradas anomalias de volume em nenhum servidor.")

if __name__ == "__main__":
    main()