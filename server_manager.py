import os
from encryption_utils import decrypt_password
from db_utils import connect_to_db_server, get_user_databases
from config import SYSTEM_DATABASES

SERVERS_FILE = "servers.txt"

def ensure_servers_file_exists():
    """
    Cria o ficheiro servers.txt se ele ainda não existir.
    """
    if not os.path.exists(SERVERS_FILE):
        with open(SERVERS_FILE, "w", encoding="utf-8") as f:
            f.write("# Insira cada servidor numa linha, no formato:\n")
            f.write("# server_name;username;encrypted_password\n")
            f.write("# Se o campo username estiver vazio, será usado Trusted_Connection.\n")
            f.write("VanSora-Hybrid\\SQLEXPRESS02;;\n")
            f.write("VanSora-Hybrid\\SQLEXPRESS01;;\n")

def read_server_list():
    """
    Lê e dá return a lista de servidores do ficheiro servers.txt.
    Cada linha válida deve estar no formato "server;username;encrypted_password".
    Linhas vazias ou que comecem com '#' são ignoradas.
    Se a password não estiver vazia, é decriptada.
    """
    ensure_servers_file_exists()
    servers = []
    try:
        with open(SERVERS_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(";")
            if len(parts) < 3:
                parts += [""] * (3 - len(parts))
            server, username, password = parts[0].strip(), parts[1].strip(), parts[2].strip()
            if password:
                try:
                    password = decrypt_password(password)
                except Exception as e:
                    print(f"Erro ao decriptar a password para o servidor {server}: {e}")
                    password = ""
            servers.append({
                "server": server,
                "username": username,
                "password": password
            })
        return servers
    except Exception as e:
        print(f"Erro a ler o ficheiro servers.txt: {e}")
        return []

def get_remote_databases():
    """
    Conecta a todos os servidores listados no ficheiro SERVERS_FILE e obtém as bases de dados non system de cada um.
    Devolve um dicionário que dá map os nomes dos servidores para listas de nomes de bases de dados.
    """
    databases_by_server = {}
    server_list = read_server_list()
    if not server_list:
        print(f"Nenhum servidor definido em '{SERVERS_FILE}'.")
        return databases_by_server

    for srv in server_list:
        server = srv.get("server", "")
        username = srv.get("username", "")
        password = srv.get("password", "")
        print(f"\nA conectar ao servidor {server}...")
        try:
            conn = connect_to_db_server(server, username, password)
            cursor = conn.cursor()
            dbs = get_user_databases(cursor)
            databases_by_server[server] = dbs
            conn.close()
            print(f"Servidor {server}: {len(dbs)} bases de dados encontradas.")
        except Exception as e:
            print(f"Erro ao conectar no servidor {server}: {e}")
    return databases_by_server

if __name__ == "__main__":
    # Testa a leitura dos servidores e recuperação de bases
    dbs_by_server = get_remote_databases()
    for srv, dbs in dbs_by_server.items():
        print(f"\nServidor: {srv}")
        for db in dbs:
            print(f" - {db}")