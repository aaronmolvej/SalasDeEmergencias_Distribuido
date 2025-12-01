import socket
import json
import threading
import time
from app.data_access.db_manager import DatabaseManager
from app.services.replication_service import broadcast_to_slaves
from app.common.constants import (
    MSG_OK, MSG_ERROR, MSG_NEW_VISIT, MSG_LOGIN, 
    DOC_DISPONIBLE, DOC_OCUPADO, CAMA_LIBRE, CAMA_OCUPADA
)

# Configuración por defecto
MASTER_PORT = 8000
BUFFER_SIZE = 4096
DB_PATH = "data/nodo_1.db" 
SCHEMA_PATH = "config/schema.sql"

# SEMÁFORO PARA EXCLUSIÓN MUTUA
mutex_asignacion = threading.Lock()

# Instancia global de la base de datos para este servicio
db = DatabaseManager(DB_PATH, SCHEMA_PATH)

def start_master_listener(port=MASTER_PORT):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server.bind(("0.0.0.0", port))
        server.listen(10)
        print(f"[MASTER] Nodo Maestro escuchando en puerto {port}...")

        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_request, args=(conn,))
            thread.start()
            
    except Exception as e:
        print(f"[MASTER Error] No se pudo iniciar el servidor: {e}")
    finally:
        server.close()

def handle_request(conn):
    """
    Despachador principal de mensajes recibidos.
    """
    try:
        data = conn.recv(BUFFER_SIZE).decode("utf-8")
        if not data:
            return
            
        request = json.loads(data)
        print(f"\n[MASTER] Solicitud recibida: {request.get('type')}")

        req_type = request.get("type")
        response = {"status": MSG_ERROR, "msg": "Petición no reconocida"}

        # REGISTRAR PACIENTE 
        if req_type == "REGISTER_PATIENT":
            sql = "INSERT INTO pacientes (nombre, seguro_social) VALUES (?, ?)"
            params = (request["nombre"], request["seguro"])
            
            # Escritura Local
            res_db = db.ejecutar_escritura(sql, params)
            
            if res_db["status"] == "OK":
                # Replicación
                broadcast_to_slaves({
                    "type": "WRITE",
                    "sql": sql,
                    "params": params
                })
                response = {"status": MSG_OK, "id": res_db["id"], "msg": "Paciente registrado"}
            else:
                response = {"status": MSG_ERROR, "msg": res_db.get("error")}

        # NUEVA VISITA 
        elif req_type == MSG_NEW_VISIT:
            seguro = request.get("seguro")
            paciente = db.ejecutar_lectura("SELECT id_paciente FROM pacientes WHERE seguro_social = ?", (seguro,))
            
            if paciente["status"] == "OK" and len(paciente["data"]) > 0:
                id_paciente = paciente["data"][0]["id_paciente"]
                response = create_visit_transaction(id_paciente)
            else:
                response = {"status": MSG_ERROR, "msg": "Paciente no encontrado"}

        # CONSULTAR DISPONIBILIDAD 
        elif req_type == "CHECK_AVAIL":
            docs = db.ejecutar_lectura(f"SELECT COUNT(*) as total FROM doctores WHERE estado = '{DOC_DISPONIBLE}'")
            camas = db.ejecutar_lectura(f"SELECT COUNT(*) as total FROM camas WHERE estado = '{CAMA_LIBRE}'")
            
            total_docs = docs["data"][0]["total"] if docs["status"] == "OK" else 0
            total_camas = camas["data"][0]["total"] if camas["status"] == "OK" else 0
            
            response = {
                "status": MSG_OK, 
                "doctores": total_docs, 
                "camas": total_camas
            }

        # CERRAR VISITA 
        elif req_type == "CLOSE_VISIT":
            # Aquí deberías implementar la lógica para liberar cama y doctor
            pass

        # Enviar respuesta final
        conn.sendall(json.dumps(response).encode("utf-8"))

    except Exception as e:
        print(f"[MASTER Error] Procesando petición: {e}")
        err_response = {"status": MSG_ERROR, "msg": str(e)}
        conn.sendall(json.dumps(err_response).encode("utf-8"))
    finally:
        conn.close()

def create_visit_transaction(id_paciente):
    """
    Maneja la transacción crítica de asignar recursos.
    Usa MUTEX para garantizar que no se asigne el mismo recurso dos veces.
    """
    with mutex_asignacion:
        print("[MASTER] Iniciando sección crítica (Asignación de Recursos)")
        
        # Buscar Doctor Disponible
        sql_doc = f"SELECT id_doctor FROM doctores WHERE estado = '{DOC_DISPONIBLE}' LIMIT 1"
        res_doc = db.ejecutar_lectura(sql_doc)
        
        # Buscar Cama Disponible
        sql_cama = f"SELECT id_cama FROM camas WHERE estado = '{CAMA_LIBRE}' LIMIT 1"
        res_cama = db.ejecutar_lectura(sql_cama)

        # Validación de Disponibilidad
        if (res_doc["status"] != "OK" or not res_doc["data"]) or \
           (res_cama["status"] != "OK" or not res_cama["data"]):
            print("[MASTER] Fin sección crítica (Sin recursos)")
            return {"status": MSG_ERROR, "msg": "No hay recursos disponibles (Cama o Doctor)"}

        id_doctor = res_doc["data"][0]["id_doctor"]
        id_cama = res_cama["data"][0]["id_cama"]
        id_sala_actual = 1 # ID del nodo maestro por defecto o lógica de balanceo

        # Generar Folio
        folio = generate_folio(id_paciente, id_doctor, id_sala_actual)

        try:
            # Actualizar estados (Ocupar recursos)
            db.ejecutar_escritura("UPDATE doctores SET estado = ? WHERE id_doctor = ?", (DOC_OCUPADO, id_doctor))
            db.ejecutar_escritura("UPDATE camas SET estado = ? WHERE id_cama = ?", (CAMA_OCUPADA, id_cama))

            # Insertar Visita
            sql_insert = """
                INSERT INTO visitas (folio, id_paciente, id_doctor, id_cama, id_sala, estado)
                VALUES (?, ?, ?, ?, ?, 'EN_PROCESO')
            """
            params_insert = (folio, id_paciente, id_doctor, id_cama, id_sala_actual)
            db.ejecutar_escritura(sql_insert, params_insert)

            #  REPLICAR (Broadcast)
            ops_replicacion = [
                {"sql": "UPDATE doctores SET estado = ? WHERE id_doctor = ?", "params": (DOC_OCUPADO, id_doctor)},
                {"sql": "UPDATE camas SET estado = ? WHERE id_cama = ?", "params": (CAMA_OCUPADA, id_cama)},
                {"sql": sql_insert, "params": params_insert}
            ]
            
            for op in ops_replicacion:
                broadcast_to_slaves({
                    "type": "WRITE",
                    "sql": op["sql"],
                    "params": op["params"]
                })

            print(f"[MASTER] Fin sección crítica (Éxito: {folio})")
            return {
                "status": MSG_OK,
                "folio": folio,
                "doctor_asignado": id_doctor,
                "cama_asignada": id_cama
            }

        except Exception as e:
            print(f"[MASTER] Fin sección crítica (Error: {e})")
            return {"status": MSG_ERROR, "msg": f"Error en transacción: {e}"}

def generate_folio(paciente, doctor, sala):
    import random
    consecutivo = random.randint(1000, 9999)
    return f"P{paciente}-D{doctor}-S{sala}-{consecutivo}"

if __name__ == "__main__":
    start_master_listener()