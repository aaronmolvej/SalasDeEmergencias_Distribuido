import socket
import json
import threading
import time
import os

from app.data_access.db_manager import execute_sql
from app.common.config_loader import load_cluster_config
from app.common.protocol import send_json as protocol_send_json, recv_json


#   CONFIGURACIÓN GENERAL
REPLICATION_PORT = 9001      
REPLICATION_TIMEOUT = 0.5  
REPLICATION_RETRIES = 1 
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



#   BROADCAST DESDE EL MAESTRO
def broadcast_to_slaves(operation_json, sender_id=None):
    """
    Envía una operación SQL a todos los nodos (excepto al propio maestro idealmente).
    """
    print(f"[REPLICATION] Difundiendo: {operation_json.get('sql')[:30]}...")

    config = load_cluster_config()
    nodes = config["nodes"]
    
    results = {}

    for node in nodes:
        if sender_id is not None and node['id'] == sender_id:
            continue
        target_ip = node["host"]
        target_port = node["port_db"] 
        
        node_id = f"{target_ip}:{target_port}"

        success = False
        for attempt in range(REPLICATION_RETRIES):
            try:
                with socket.create_connection((target_ip, target_port), timeout=REPLICATION_TIMEOUT) as sock:

                    protocol_send_json(sock, operation_json)
                    
                    response = recv_json(sock)
                    
                    if response and response.get("status") == "OK":
                        print(f" Réplica exitosa en Nodo {node['id']}")
                        success = True
                        break 
                    else:
                         print(f"Respuesta inesperada de Nodo {node['id']}: {response}")

            except ConnectionRefusedError:
                pass
            except Exception as e:
                print(f"Error replicando a {node_id}: {e}")
                
        results[node['id']] = success

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
