import csv
from db_utils import connect_to_db_server
from server_manager import read_server_list

def test_server_connections():
    servers = read_server_list()
    error_records = []
    
    if not servers:
        print("Nenhum servidor definido no ficheiro 'servers.txt'.")
        return
    
    for srv in servers:
        server = srv.get("server")
        username = srv.get("username")
        password = srv.get("password")
        print(f"Testando conexão com o servidor: {server} ...")
        try:
            conn = connect_to_db_server(server, username, password)
            conn.close()
        except Exception as e:
            print(f"Falha na conexão com o servidor {server}: {e}")
            error_records.append({"server": server, "error": str(e)})
    
    if error_records:
        with open("server_connection_report.csv", "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = ["server", "error"]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for record in error_records:
                writer.writerow(record)
        print("Relatório de erros gravado em 'server_connection_report.csv'")
    else:
        print("Todas as conexões foram bem sucedidas.")

if __name__ == "__main__":
    test_server_connections()