import os

WHITELIST_FILE = "whitelist.txt"
DEFAULT_PREFIX = "VanSora-Hybrid\\"

SYSADMIN_WHITELIST_FILE = "sysadminwhitelist.txt"
DEFAULT_SYSADMIN_PREFIX = "AdminUser"

SYSADMIN_GROUP_WHITELIST_FILE = "sysadmingroupwhitelist.txt"
DEFAULT_SYSADMIN_GROUP_PREFIX = "AdminGroup"

def create_whitelist_file_if_not_exists():
    """
    Cria o ficheiro whitelist.txt com um prefixo pré-criado, se este ficheiro já não existir.
    """
    if not os.path.exists(WHITELIST_FILE):
        with open(WHITELIST_FILE, "w") as f:
            f.write(DEFAULT_PREFIX + "\n")

def load_whitelist():
    """
    Lê o ficheiro whitelist.txt e dá return a uma lista de prefixos autorizados.
    Caso o ficheiro não exista, ele é criado com o prefixo padrão.
    """
    if not os.path.exists(WHITELIST_FILE):
        create_whitelist_file_if_not_exists()
    with open(WHITELIST_FILE, "r") as f:
        prefixes = [line.strip() for line in f if line.strip()]
    return prefixes

def create_sysadmin_whitelist_file_if_not_exists():
    """
    Cria o ficheiro sysadminwhitelist.txt com um valor padrão, se este ficheiro já não existir.
    """
    if not os.path.exists(SYSADMIN_WHITELIST_FILE):
        with open(SYSADMIN_WHITELIST_FILE, "w") as f:
            f.write(DEFAULT_SYSADMIN_PREFIX + "\n")

def load_sysadmin_whitelist():
    """
    Lê o ficheiro sysadminwhitelist.txt e retorna uma lista com os valores.
    Se o ficheiro não existir, cria-o.
    """
    if not os.path.exists(SYSADMIN_WHITELIST_FILE):
        create_sysadmin_whitelist_file_if_not_exists()
    with open(SYSADMIN_WHITELIST_FILE, "r") as f:
        entries = [line.strip() for line in f if line.strip()]
    return entries

def create_sysadmingroup_whitelist_file_if_not_exists():
    """
    Cria o ficheiro sysadmingroupwhitelist.txt com um valor padrão, se este ficheiro já não existir.
    """
    if not os.path.exists(SYSADMIN_GROUP_WHITELIST_FILE):
        with open(SYSADMIN_GROUP_WHITELIST_FILE, "w") as f:
            f.write(DEFAULT_SYSADMIN_GROUP_PREFIX + "\n")

def load_sysadmingroup_whitelist():
    """
    Lê o ficheiro sysadmingroupwhitelist.txt e retorna uma lista com os valores.
    Se o ficheiro não existir, cria-o.
    """
    if not os.path.exists(SYSADMIN_GROUP_WHITELIST_FILE):
        create_sysadmingroup_whitelist_file_if_not_exists()
    with open(SYSADMIN_GROUP_WHITELIST_FILE, "r") as f:
        entries = [line.strip() for line in f if line.strip()]
    return entries