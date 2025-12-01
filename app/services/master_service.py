import socket
import json
import threading
from app.data_access.db_manager import execute_sql, fetch_one, fetch_all
from app.services.replication_service import broadcast_to_slaves

MASTER_PORT = 8000
BUFFER_SIZE = 4096


#   INICIO DEL SERVIDOR DEL MAESTRO

def start_master_listener():
    """
    Inicia el servidor del nodo maestro.
    Escucha solicitudes de nodos esclavos.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", MASTER_PORT))
    server.listen()

    print(f"[MASTER] Nodo maestro escuchando en puerto {MASTER_PORT}...")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_request, args=(conn,))
        thread.start()



#   PROCESAMIENTO DE SOLICITUDES

def handle_request(conn):
    try:
        data = conn.recv(BUFFER_SIZE).decode("utf-8")
        request = json.loads(data)
        print("\n[MASTER] Solicitud recibida:", request)

        req_type = request["type"]

        
        # Solicitud: Crear una visita
        
        if req_type == "NEW_VISIT":
            paciente_id = request["paciente_id"]
            trabajador_id = request["trabajador_id"]

            result = create_visit(paciente_id, trabajador_id)

            conn.sendall(json.dumps(result).encode("utf-8"))
            conn.close()
            return

        
        # Cerrar visita
        
        if req_type == "CLOSE_VISIT":
            visit_id = request["visit_id"]
            result = close_visit(visit_id)

            conn.sendall(json.dumps(result).encode("utf-8"))
            conn.close()
            return

        conn.sendall(json.dumps({"status": "ERROR", "msg": "Unknown request"}).encode("utf-8"))
    except Exception as e:
        print("[MASTER] Error:", e)
    finally:
        conn.close()


#   CREAR UNA VISITA 

def create_visit(paciente_id, trabajador_id):
    """
    Crea una visita de emergencia.
    Asigna doctor, cama y sala (este nodo).
    Replica a los demás nodos.
    """
    # 1) Elegir doctor y cama disponibles
    doctor = fetch_one("SELECT id FROM DOCTORES WHERE disponible = 1 LIMIT 1", [])
    cama = fetch_one("SELECT id FROM CAMAS WHERE disponible = 1 LIMIT 1", [])

    if doctor is None or cama is None:
        return {"status": "ERROR", "msg": "No hay doctor o cama disponible"}

    doctor_id = doctor[0]
    cama_id = cama[0]

    # 2) Generar folio
    folio = generate_folio(paciente_id, doctor_id, "SALA1")  # cambiar SALA dinámico

    # 3) Guardar visita en BD del maestro
    sql_insert = """
        INSERT INTO VISITAS (folio, paciente_id, doctor_id, cama_id, estado)
        VALUES (?, ?, ?, ?, 'ABIERTA')
    """

    params = [folio, paciente_id, doctor_id, cama_id]

    execute_sql(sql_insert, params)

    # 4) Marcar doctor y cama como ocupados
    execute_sql("UPDATE DOCTORES SET disponible = 0 WHERE id = ?", [doctor_id])
    execute_sql("UPDATE CAMAS SET disponible = 0 WHERE id = ?", [cama_id])

    # 5) REPLICAR la operación a los demás nodos
    broadcast_to_slaves({
        "type": "REPLICATION",
        "action": "INSERT",
        "table": "VISITAS",
        "sql": sql_insert,
        "params": params
    })

    # 6) Regresar al nodo solicitante
    return {
        "status": "OK",
        "folio": folio,
        "doctor": doctor_id,
        "cama": cama_id
    }



#   CERRAR UNA VISITA

def close_visit(visit_id):

    sql = "UPDATE VISITAS SET estado = 'CERRADA' WHERE id = ?"
    execute_sql(sql, [visit_id])

    broadcast_to_slaves({
        "type": "REPLICATION",
        "action": "UPDATE",
        "table": "VISITAS",
        "sql": sql,
        "params": [visit_id]
    })

    return {"status": "OK"}



#   GENERAR FOLIO

def generate_folio(paciente, doctor, sala):
    import random
    consecutivo = random.randint(1000, 9999)
    return f"{paciente}-{doctor}-{sala}-{consecutivo}"
