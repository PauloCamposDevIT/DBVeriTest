import os
import datetime
from db_utils import connect_to_db_server
from server_manager import read_server_list
from anomaly_log import log_anomalies
from concurrent.futures import ThreadPoolExecutor

def get_all_file_records(master_cursor):
    """
    Devolve uma lista de registos de ficheiros a partir de todas as bases de dados.
    Cada registo é uma tupla: (dbname, physical_name, type_desc)
    """
    file_records = []
    try:
        query = """
        SELECT d.name AS database_name, mf.physical_name, mf.type_desc
        FROM sys.master_files mf
        JOIN sys.databases d ON mf.database_id = d.database_id
        WHERE d.database_id > 4
        """
        master_cursor.execute(query)
        rows = master_cursor.fetchall()
        for row in rows:
            file_records.append((row[0], row[1], row[2]))
    except Exception as e:
        print(f"Erro ao obter registos de ficheiros: {e}")
    return file_records

def classify_volumes(file_records):
    """
    Classifica os ficheiros com base no type_desc e devolve um dicionário.
    Os ficheiros de dados (ROWS) e de log (LOG) serão separados.
    """
    volume_class = {"data": [], "log": []}
    for dbname, path, type_desc in file_records:
        if type_desc.upper() == "ROWS":
            volume_class["data"].append((dbname, path))
        elif type_desc.upper() == "LOG":
            volume_class["log"].append((dbname, path))
    return volume_class

def check_file_volume_anomalies(volume_class):
    """
    Verifica se existem ficheiros de dados em caminhos de TLOG ou ficheiros de TLOG em caminhos de dados.
    A heurística utilizada é: verificar se o nome do diretório (obtido com dirname) contém "data" ou "log".
    Cada anomalia incluirá também o database de origem.
    """
    anomalies = []
    # Anomalias em ficheiros de dados
    for dbname, path in volume_class.get("data", []):
        dir_name = os.path.basename(os.path.dirname(path)).lower()
        if "log" in dir_name:
            anomalies.append({
                "database": dbname,
                "file_path": path,
                "issue": "Ficheiro de dados encontrado em caminho de TLOG"
            })
    # Anomalias em ficheiros de log
    for dbname, path in volume_class.get("log", []):
        dir_name = os.path.basename(os.path.dirname(path)).lower()
        if "data" in dir_name:
            anomalies.append({
                "database": dbname,
                "file_path": path,
                "issue": "Ficheiro de TLOG encontrado em caminho de dados"
            })
    return anomalies

def check_volume_integrity():
    """
    Verifica se existem ficheiros fora do local esperado em todas as instâncias (para todos os databases).
    As informações são transformadas e registradas no histórico.
    """
    all_anomalias = []
    servers = read_server_list()
    if not servers:
        print("Nenhuma instância definido no ficheiro 'servers.txt'.")
        return

    print("\n" + "=" * 40)
    print("Relatório de Integridade dos Volumes (Verificação de local dos ficheiros)")
    print("=" * 40)

    def process_server(srv):
        server = srv["server"]
        username = srv["username"]
        password = srv["password"]
        server_anomalias = []
        print(f"\n=== Servidor: {server} ===")
        try:
            conn = connect_to_db_server(server, username, password)
            cursor = conn.cursor()
            
            # Obter o nome real da instância
            instance = server
            try:
                cursor.execute("SELECT @@SERVERNAME")
                result = cursor.fetchone()
                if result and result[0]:
                    instance = result[0]
            except Exception as e:
                print(f"Não foi possível obter o nome da instância no servidor {server}: {e}")
            
            # Chamada à função get_all_file_records logo após criar o cursor
            file_records = get_all_file_records(cursor)
            
            volume_class = classify_volumes(file_records)
            anomalies = check_file_volume_anomalies(volume_class)
            conn.close()
            
            for anomaly in anomalies:
                original_issue = anomaly.get("issue", "")
                transformed_issue = original_issue.replace("base de dados", "instância")
                transformed = {
                    "database": anomaly.get("database", "N/A"),
                    "type": "Verificação de Volume",
                    "device": anomaly.get("file_path", ""),
                    "user": "",
                    "instance": instance,
                    "timestamp": datetime.datetime.now(),
                    "issues": [transformed_issue]
                }
                server_anomalias.append(transformed)

            if server_anomalias:
                print(f"\nAnomalias encontradas na instância {instance}:")
                for idx, anot in enumerate(server_anomalias, 1):
                    print(f"\n{idx}. Ficheiro: {anot['device']}")
                    print(f"   Database: {anot['database']}")
                    print(f"   Tipo esperado: {anot['type'].upper()}")
                    print(f"   Problema: {anot['issues'][0]}")
                print(f"\nTotal de anomalias na instância {instance}: {len(server_anomalias)}")
            else:
                print(f"Sem anomalias de volume na instância {instance}.")
        except Exception as e:
            print(f"Erro ao verificar a integridade dos volumes na instância {server}: {e}")
        return server_anomalias

    with ThreadPoolExecutor() as executor:
        for result in executor.map(process_server, servers):
            all_anomalias.extend(result)

    if all_anomalias:
        print("\n" + "-" * 40)
        print(f"Total de anomalias de volume encontradas em todas as instâncias: {len(all_anomalias)}")
        print("-" * 40)
        log_anomalies(all_anomalias)
    else:
        print("\nNão foram encontradas anomalias de volume em nenhuma instância.")