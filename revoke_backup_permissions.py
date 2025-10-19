import pyodbc
from user_whitelist import load_whitelist
from db_utils import connect_to_db_server

SYSADMIN_WHITELIST_FILE = "sysadminwhitelist.txt"
SYSADMIN_GROUP_WHITELIST_FILE = "sysadmingroupwhitelist.txt"

def load_sysadmin_whitelist():
    """
    Carrega a lista de logins da sysadminwhitelist.
    """
    try:
        with open(SYSADMIN_WHITELIST_FILE, "r") as f:
            lines = f.read().splitlines()
            return [line.strip() for line in lines if line.strip()]
    except FileNotFoundError:
        return []

def load_sysadmin_group_whitelist():
    """
    Carrega a lista de grupos da sysadmingroupwhitelist.
    """
    try:
        with open(SYSADMIN_GROUP_WHITELIST_FILE, "r") as f:
            lines = f.read().splitlines()
            return [line.strip() for line in lines if line.strip()]
    except FileNotFoundError:
        return []

def remove_from_sysadmin_whitelist(login):
    """
    Remove o login da sysadminwhitelist e regrava o arquivo.
    """
    whitelist = load_sysadmin_whitelist()
    if login in whitelist:
        whitelist.remove(login)
        with open(SYSADMIN_WHITELIST_FILE, "w") as f:
            for item in whitelist:
                f.write(item + "\n")
        print(f"Login '{login}' removido da sysadminwhitelist.")

def is_login_in_authorized_group(cursor, login, group_whitelist):
    """
    Verifica se o login pertence a algum grupo da group whitelist.
    """
    query = """
        SELECT rp.name
        FROM sys.server_role_members srm
        INNER JOIN sys.server_principals sp ON srm.member_principal_id = sp.principal_id
        INNER JOIN sys.server_principals rp ON srm.role_principal_id = rp.principal_id
        WHERE sp.name = ?
    """
    cursor.execute(query, login)
    groups = [row[0] for row in cursor.fetchall()]
    return any(g.lower() in (gw.lower() for gw in group_whitelist) for g in groups)

def get_nonwhitelisted_sysadmin_logins(cursor, whitelist, sysadmin_whitelist, sysadmin_group_whitelist):
    """
    Retorna uma lista de logins que possuem o role "sysadmin" e que NÃO estão na whitelist,
    nem na sysadminwhitelist, nem pertencem a um grupo da sysadmingroupwhitelist.
    """
    sysadmin_query = """
        SELECT sp.name
        FROM sys.server_principals sp
        INNER JOIN sys.server_role_members srm ON sp.principal_id = srm.member_principal_id
        INNER JOIN sys.server_principals rp ON rp.principal_id = srm.role_principal_id
        WHERE rp.name = 'sysadmin'
    """
    cursor.execute(sysadmin_query)
    rows = cursor.fetchall()
    logins = []
    for row in rows:
        user_name = row[0]
        # Se o login estiver na sysadmin_whitelist, ele deve ser mantido:
        if user_name.lower() in (user.lower() for user in sysadmin_whitelist):
            continue
        # Se o login pertence a um grupo autorizado, também deve ser mantido:
        elif is_login_in_authorized_group(cursor, user_name, sysadmin_group_whitelist):
            continue
        # Se o login não atender à condição da whitelist principal, ele será considerado para remoção.
        elif not any(user_name.lower().startswith(prefix.lower()) for prefix in whitelist):
            logins.append(user_name)
    return logins

def revoke_sysadmin_permissions(cursor, instance, whitelist, actions):
    """
    Remove o role 'sysadmin' dos logins (não autorizados) que foram confirmados pelo usuário.
    A mensagem utiliza o nome da instância obtido (em vez do IP) quando disponível.
    """
    sysadmin_whitelist = load_sysadmin_whitelist()
    sysadmin_group_whitelist = load_sysadmin_group_whitelist()
    logins_to_remove = get_nonwhitelisted_sysadmin_logins(cursor, whitelist, sysadmin_whitelist, sysadmin_group_whitelist)
    
    if not logins_to_remove:
        actions.append(f"[{instance}][master] Nenhum login para remover de 'sysadmin'.")
        return
    
    print(f"\nNo servidor {instance} os seguintes logins possuem o role 'sysadmin' e não estão na whitelist:")
    for login in logins_to_remove:
        print(f" - {login}")
    
    confirm = input("Tem certeza que deseja remover o role 'sysadmin' destes logins? (s/n): ").strip().lower()
    if confirm != 's':
        actions.append(f"[{instance}][master] Remoção cancelada pelo usuário.")
        return
    
    drop_srv_query = """
        IF EXISTS(
            SELECT 1 FROM sys.server_role_members srm 
            INNER JOIN sys.server_principals sp ON srm.member_principal_id = sp.principal_id
            WHERE sp.name = ?
        )
        BEGIN
            EXEC sp_dropsrvrolemember ?, 'sysadmin'
        END
    """
    for login in logins_to_remove:
        try:
            cursor.execute(drop_srv_query, login, login)
            actions.append(f"[{instance}][master] Utilizador '{login}': Removido do role 'sysadmin'.")
        except Exception as e_sysadmin:
            actions.append(f"[{instance}][master] Utilizador '{login}': Erro ao remover de 'sysadmin' (erro ignorado): {e_sysadmin}")

def revoke_backup_permissions_server(server, username, password):
    """
    Para o servidor especificado, remove o role 'sysadmin' dos logins que não estão na whitelist
    (após confirmação do usuário). Opera na base master e devolve uma lista com as ações efetuadas.
    Tenta obter o nome da instância via query; se obtido, utiliza-o nas mensagens.
    """
    whitelist = load_whitelist()
    actions = []
    try:
        conn = connect_to_db_server(server, username, password)
        cursor = conn.cursor()
        cursor.execute("USE [master]")
        # Tenta obter o nome da instância – se disponível, utiliza-o; caso contrário, mantém o IP
        instance = server
        try:
            cursor.execute("SELECT @@SERVERNAME")
            result = cursor.fetchone()
            if result and result[0]:
                instance = result[0]
        except Exception as e:
            print(f"Não foi possível obter o nome da instância no servidor {server}: {e}")
        revoke_sysadmin_permissions(cursor, instance.upper(), whitelist, actions)
        conn.commit()
    except Exception as e:
        actions.append(f"[{server}] Erro ao conectar: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass
    return actions

def revoke_backup_permissions_all_servers():
    """
    Aplica a funcionalidade a todos os servidores listados no ficheiro servers.txt.
    Remove o role "sysadmin" dos logins que não estão na whitelist (ou foram removidos da sysadminwhitelist),
    após confirmação do usuário.
    Devolve uma lista com todas as ações efetuadas.
    """
    from server_manager import read_server_list
    servers = read_server_list()
    all_actions = []
    if not servers:
        all_actions.append("Nenhum servidor definido no ficheiro 'servers.txt'.")
        return all_actions
    for srv in servers:
        server = srv["server"]
        username = srv["username"]
        password = srv["password"]
        actions = revoke_backup_permissions_server(server, username, password)
        all_actions.append(f"Servidor: {server}")
        all_actions.extend(actions)
    return all_actions

if __name__ == "__main__":
    actions = revoke_backup_permissions_all_servers()
    for action in actions:
        print(action)
