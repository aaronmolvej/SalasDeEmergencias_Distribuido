import socket
import json
import threading
import time
import os

from app.data_access.db_manager import execute_sql
from config.cluster_config import load_cluster_config


#   CONFIGURACIÓN GENERAL
REPLICATION_PORT = 9001      # Puerto donde los esclavos escuchan
REPLICATION_TIMEOUT = 3      # Tiempo máximo para esperar respuesta
REPLICATION_RETRIES = 2      # Reintentos si un nodo no responde
BUFFER_SIZE = 4096



#  ENVIAR JSON
def send_json(ip, port, data):
    """
    Envía un JSON a un nodo específico.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(REPLICATION_TIMEOUT)

    try:
        s.connect((ip, port))
        s.sendall(json.dumps(data).encode("utf-8"))
        response = s.recv(BUFFER_SIZE).decode("utf-8")
        return json.loads(response)
    except Exception as e:
        print(f"[REPLICATION] Error enviando a {ip}:{port} -> {e}")
        return None
    finally:
        s.close()



#   FUNCION 1: BROADCAST DESDE EL MAESTRO
def broadcast_to_slaves(operation_json):
    """
    Envía una operación SQL a todos los esclavos.
    
    operation_json debe seguir esta estructura:

    {
        "type": "REPLICATION",
        "action": "INSERT | UPDATE | DELETE",
        "table": "VISITAS",
        "sql": "INSERT INTO VISITAS (...) VALUES (...)",
        "params": [ ... ]
    }
    """

    print("\n[REPLICATION] Enviando operación a esclavos...")
    print("[REPLICATION] Operación:", operation_json)

    config = load_cluster_config()
    my_ip = config["self"]["ip"]
    nodes = config["nodes"]

    results = {}

    for node in nodes:
        if node["ip"] == my_ip:
            continue  # No replicar a sí mismo

        ip = node["ip"]
        print(f"[REPLICATION] -> Enviando a nodo {ip}:{REPLICATION_PORT}")

        attempts = 0
        success = False

        while attempts < REPLICATION_RETRIES:
            resp = send_json(ip, REPLICATION_PORT, operation_json)

            if resp and resp.get("status") == "OK":
                print(f"[REPLICATION] ✓ Nodo {ip} replicó correctamente.")
                results[ip] = True
                success = True
                break
            else:
                print(f"[REPLICATION] × Falló intento {attempts+1} con {ip}.")
                attempts += 1
                time.sleep(1)

        if not success:
            print(f"[REPLICATION] ❗ Nodo {ip} NO replicó.")
            results[ip] = False

    print("[REPLICATION] Resultados:", results)
    return results




#   FUNCION 2: LISTENER DEL ESCLAVO

def handle_replication(connection):
    """
    Maneja una operación de replicación recibida desde el maestro.
    """
    try:
        data = connection.recv(BUFFER_SIZE).decode("utf-8")
        operation = json.loads(data)

        print("\n[REPLICATION] Mensaje recibido:")
        print(operation)

        if operation["type"] != "REPLICATION":
            connection.sendall(json.dumps({"status": "ERROR"}).encode("utf-8"))
            return

        sql = operation["sql"]
        params = operation["params"]

        # Ejecutar SQL replicado en este nodo
        execute_sql(sql, params)

        connection.sendall(json.dumps({"status": "OK"}).encode("utf-8"))
    except Exception as e:
        print("[REPLICATION] Error procesando operación:", e)
        connection.sendall(json.dumps({"status": "ERROR"}).encode("utf-8"))
    finally:
        connection.close()


def replication_listener():
    """
    Inicia un servidor que escucha operaciones del maestro.
    Se ejecuta en los esclavos.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", REPLICATION_PORT))
    server.listen()

    print(f"[REPLICATION] Escuchando replicación en puerto {REPLICATION_PORT}...")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_replication, args=(conn,))
        thread.start()
