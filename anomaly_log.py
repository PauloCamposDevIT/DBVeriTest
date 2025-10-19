import os
import json
from datetime import datetime
import shutil
from config import CRITICAL_ARCHIVE_FOLDER  # Importa a pasta de arquivamento configurada

LOG_FILE = "anomaly_history.json"
CRITICAL_LOG_FILE = "critical_anomaly_history.json"
ARCHIVE_FOLDER = "archived_logs"
MAX_ENTRIES = 2000
CRITICAL_MAX_ENTRIES = 2000

def init_log_file():
    """Cria ou reinicializa o log geral em formato JSON."""
    data = {"anomalies": []}
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def init_critical_log_file():
    """Cria ou reinicializa o log crítico em formato JSON."""
    data = {"critical_anomalies": []}
    with open(CRITICAL_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def serialize_anomaly(anomaly):
    """Converte a anomalia para um formato serializável (ex.: datas em string)."""
    new_anom = anomaly.copy()
    ts = new_anom.get("timestamp")
    if isinstance(ts, datetime):
        new_anom["timestamp"] = ts.isoformat()
    else:
        new_anom["timestamp"] = str(ts)
    return new_anom

def add_anomaly_to_log(anomaly):
    """Adiciona uma única anomalia ao log geral JSON."""
    try:
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        init_log_file()
        with open(LOG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    anomalies = data.get("anomalies", [])
    while len(anomalies) >= MAX_ENTRIES:
        anomalies.pop(0)
    anomalies.append(serialize_anomaly(anomaly))
    data["anomalies"] = anomalies
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def log_anomalies(anomalies):
    """Registra cada anomalia no log geral JSON."""
    for anomaly in anomalies:
        add_anomaly_to_log(anomaly)

def add_critical_anomaly_to_log(anomaly):
    """Adiciona uma única anomalia crítica ao log crítico JSON."""
    try:
        with open(CRITICAL_LOG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        init_critical_log_file()
        with open(CRITICAL_LOG_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    crit_anomalies = data.get("critical_anomalies", [])
    while len(crit_anomalies) >= CRITICAL_MAX_ENTRIES:
        crit_anomalies.pop(0)
    crit_anomalies.append(serialize_anomaly(anomaly))
    data["critical_anomalies"] = crit_anomalies
    with open(CRITICAL_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def log_critical_anomalies(anomalies):
    """Registra as anomalias críticas (level==0) no log crítico JSON."""
    crit = [anom for anom in anomalies if int(anom.get("level", 1)) == 0]
    for anomaly in crit:
        add_critical_anomaly_to_log(anomaly)

def archive_critical_log():
    """
    Move o log crítico JSON atual para a pasta definida em CRITICAL_ARCHIVE_FOLDER 
    com o nome contendo a data no formato YYYY_MM_DD e reinicializa-o.
    """
    if not os.path.exists(CRITICAL_LOG_FILE):
        init_critical_log_file()
        return
    archive_folder = CRITICAL_ARCHIVE_FOLDER
    if not os.path.exists(archive_folder):
        os.makedirs(archive_folder)
    date_str = datetime.now().strftime("%Y_%m_%d")
    archive_file = os.path.join(archive_folder, f"critical_anomaly_history_{date_str}.json")
    shutil.move(CRITICAL_LOG_FILE, archive_file)
    init_critical_log_file()
