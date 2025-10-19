import time
from collections import defaultdict
from db_utils import connect_to_db_server, get_user_databases, get_backups
from check_authorized_users import check_authorized_users
from check_backup_frequency import check_backup_frequency
from check_tlog_after_full_diff import check_tlog_after_full_diff
from check_file_size import check_file_size
from check_db_status import check_db_status  
from user_whitelist import create_whitelist_file_if_not_exists
from anomaly_log import log_anomalies, archive_critical_log, log_critical_anomalies
from notification import send_alert_email     
import os
import datetime
import threading
from check_volumes import check_volume_integrity  # Adiciona esta importação

# Ver se o ficheiro whitelist.txt existe (se não, cria-o)
create_whitelist_file_if_not_exists()

# Importa e chama logo a ensure_servers_file_exists() para criar o servers.txt se não existir
from server_manager import ensure_servers_file_exists, read_server_list
ensure_servers_file_exists()

#init_critical_log_file()

def check_daily_archive():
    """
    Verifica se o log de anomalias críticas já foi arquivado hoje.
    Se não, arquiva o log atual (chamando archive_critical_log()) e atualiza o ficheiro marcador.
    """
    marker_file = "last_critical_archive.txt"
    today_str = datetime.date.today().isoformat()
    
    if os.path.exists(marker_file):
        with open(marker_file, "r") as f:
            last_archived_date = f.read().strip()
    else:
        last_archived_date = ""
    
    if last_archived_date != today_str:
        archive_critical_log()
        with open(marker_file, "w") as f:
            f.write(today_str)

PRINT_LOCK = threading.Lock()

def run_multiserver_functionality(opcao):
    from server_manager import read_server_list
    servers = read_server_list()
    if not servers:
        with PRINT_LOCK:
            print("Nenhum servidor definido no ficheiro 'servers.txt'.")
        return
    from concurrent.futures import ThreadPoolExecutor

    def process_server(srv):
        server = srv["server"]
        username = srv["username"]
        password = srv["password"]
        with PRINT_LOCK:
            print(f"\n=== A ligar ao servidor: {server} ===")
        conn = None
        instance = server  # valor padrão caso a query falhe
        try:
            conn = connect_to_db_server(server, username, password)
            if conn is None:
                with PRINT_LOCK:
                    print(f"Falha ao conectar ao servidor {server}. A conexão retornou None.")
                return
            cursor = conn.cursor()
            # Obter o nome da instância via query SQL
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
                    print(f"Nenhuma base de dados encontrada na instância {instance}.")
            else:
                with PRINT_LOCK:
                    print(f"Instância {instance}: {len(databases)} bases de dados encontradas. A executar funcionalidades...")
                run_functionality(opcao, cursor, databases, instance)
        except Exception as e:
            with PRINT_LOCK:
                print(f"Erro ao ligar ao servidor {server}: {type(e).__name__} - {e}")
        finally:
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    with PRINT_LOCK:
                        print(f"Erro ao fechar a conexão no servidor {server}: {e}")

    with ThreadPoolExecutor() as executor:
        executor.map(process_server, servers)


def run_functionality(opcao, cursor, databases, instance):
    all_anomalias = []
    for db in databases:
        backups = get_backups(cursor, db)
        if not backups:
            continue

        if opcao == '1':
            anomalias = check_authorized_users(db, backups)
        elif opcao == '2':
            anomalias = check_backup_frequency(db, backups)
        elif opcao == '3':
            anomalias = check_tlog_after_full_diff(db, backups, cursor)
        elif opcao == '4':
            anomalias = check_file_size(db, backups)
        elif opcao == '5':
            anomalias = check_db_status(db, cursor, instance)
        else:
            with PRINT_LOCK:
                print("Opção inválida!")
            return

        # Atualiza o campo 'instance' para ser o nome da instância obtido via query
        for anomalia in anomalias:
            anomalia["instance"] = instance

        all_anomalias.extend(anomalias)

    with PRINT_LOCK:
        if all_anomalias:
            print(f"\n{'=' * 40}\nRelatório de Anomalias para a instância {instance}\n{'=' * 40}")
            for idx, anomalia in enumerate(all_anomalias, 1):
                print(f"\n{idx}. Base de Dados: {anomalia.get('database', 'N/A')}")
                print(f"   Tipo de Backup: {anomalia.get('type', 'N/A')}")
                print(f"   Dispositivo: {anomalia.get('device', 'N/A')}")
                print(f"   Utilizador: {anomalia.get('user', 'N/A')}")
                print(f"   Instância: {anomalia.get('instance', 'N/A')}")
                print(f"   Timestamp: {anomalia.get('timestamp', 'N/A')}")
                print("   Problemas detectados:")
                for issue in set(anomalia.get('issues', [])):
                    print(f"    - {issue}")
            print(f"\nTotal de anomalias: {len(all_anomalias)}")
        else:
            print(f"\nNenhuma anomalia encontrada na instância {instance}.")

    # Registra os logs
    log_anomalies(all_anomalias)

    # A funcionalidade de envio automático de e-mail para anomalias críticas foi colocada em comentário.
    # for anomalia in all_anomalias:
    #     if int(anomalia.get("level", 1)) == 0:
    #         send_alert_email(anomalia)

    critical_anomalias = [anomalia for anomalia in all_anomalias if int(anomalia.get("level", 1)) == 0]
    log_critical_anomalies(critical_anomalias)


def main():
    check_daily_archive()

    print("\nSeleciona a funcionalidade a executar:")
    print("1 - Verificar se users não autorizados fizeram backups")
    print("2 - Verificar frequência de backups")
    print("3 - Verificar anomalias na criação de TLOG")
    print("4 - Verificar backups com tamanho inválido")
    print("5 - Verificar se a base está offline ou em modo de emergência/recuperação")
    print("6 - Verificar integridade dos volumes (Ficheiros de Dados vs TLOGs)")
    print("7 - Enviar por email as anomalias críticas")
    print("8 - Revogar permissões de backup para utilizadores não autorizados")
    print("0 - Sair do programa")
    opcao = input("Escolhe (0-8): ").strip()

    if opcao == "0":
        print("Terminado...")
    elif opcao == "6":
        check_volume_integrity()
    elif opcao == "7":
        from notification import send_critical_anomalies_email
        send_critical_anomalies_email()
    elif opcao == "8":
        from revoke_backup_permissions import revoke_backup_permissions_all_servers
        actions = revoke_backup_permissions_all_servers()
        print("\n======================================")
        print("       Revogação de Permissões        ")
        print("======================================")
        for action in actions:
            print(f"  • {action}\n")
        print("======================================")
    else:
        run_multiserver_functionality(opcao)

if __name__ == "__main__":
    main()
