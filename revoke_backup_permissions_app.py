from revoke_backup_permissions import revoke_backup_permissions_all_servers

def main():
    print("\n=== Revogação de Permissões de Backup ===")
    actions = revoke_backup_permissions_all_servers()
    print("\n======================================")
    print("       Revogação de Permissões        ")
    print("======================================")
    for action in actions:
        print(f"  • {action}\n")
    print("======================================")
    input("Carregue Enter para fechar o terminal...")

if __name__ == "__main__":
    main()