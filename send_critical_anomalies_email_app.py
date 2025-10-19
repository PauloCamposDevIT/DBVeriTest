from notification import send_critical_anomalies_email

def main():
    print("\n=== Envio de E-mail com as Anomalias Críticas ===")
    send_critical_anomalies_email()

if __name__ == "__main__":
    main()