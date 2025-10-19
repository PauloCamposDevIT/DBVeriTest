import os
import smtplib
from email.message import EmailMessage
from config import SMTP_SERVER, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, ALERT_RECIPIENT
import json
from anomaly_log import CRITICAL_LOG_FILE

def send_alert_email(anomaly):
    """
    Envia um e-mail de alerta para uma anomalia crítica única e mostra uma notificação no terminal.
    """
    msg = EmailMessage()
    msg['Subject'] = f"Anomalia crítica detetada - Base de Dados: {anomaly.get('database', 'N/A')}"
    msg['From'] = SMTP_USER
    msg['To'] = ALERT_RECIPIENT

    content = "Foi detetada uma anomalia crítica:\n\n"
    for key, value in anomaly.items():
        content += f"{key}: {value}\n"
    msg.set_content(content)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    


def send_critical_anomalies_email():
    """
    Lê o ficheiro JSON atual das anomalias críticas e envia-o por e-mail em anexo.
    Se o ficheiro não contiver anomalias, imprime "Nenhuma anomalia crítica encontrada!" e não envia o e-mail.
    """
    if not os.path.exists(CRITICAL_LOG_FILE):
        print("Nenhum ficheiro de anomalias críticas encontrado.")
        return

    try:
        with open(CRITICAL_LOG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Verifica se o objeto contém a chave "critical_anomalies" e se ela possui elementos
        critical_anomalies = data.get("critical_anomalies", [])
    except Exception as e:
        print("Erro ao ler o ficheiro de anomalias críticas:", e)
        return

    if not critical_anomalies:
        print("Nenhuma anomalia crítica encontrada!")
        return

    with open(CRITICAL_LOG_FILE, "r", encoding="utf-8") as f:
        critical_data = f.read()

    msg = EmailMessage()
    msg['Subject'] = "Histórico de Anomalias Críticas"
    msg['From'] = SMTP_USER
    msg['To'] = ALERT_RECIPIENT

    msg.set_content("Segue em anexo o ficheiro com o histórico das anomalias críticas.")

    msg.add_attachment(critical_data, subtype='json', filename="critical_anomaly_history.json")

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)

    print(f"\n{'*' * 40}\nE-mail com o registo de anomalias críticas de hoje enviado com sucesso.\n{'*' * 40}\n")