import socket
import json
import threading
import datetime
from app.data_access.db_manager import DatabaseManager
from app.services.replication_service import broadcast_to_slaves
from app.common.constants import (
    MSG_OK, MSG_ERROR, MSG_NEW_VISIT, 
    DOC_DISPONIBLE, DOC_OCUPADO, CAMA_LIBRE, CAMA_OCUPADA
)

MASTER_PORT = 8000
BUFFER_SIZE = 4096
DB_PATH = "data/nodo_1.db" 
SCHEMA_PATH = "config/schema.sql"

mutex_asignacion = threading.Lock()
db = DatabaseManager(DB_PATH, SCHEMA_PATH)
MY_NODE_ID = None 

def start_master_listener(port=MASTER_PORT, node_id=None):
    global MY_NODE_ID
    if node_id: MY_NODE_ID = node_id
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        print(f"[MASTER] Nodo Maestro (ID: {MY_NODE_ID}) escuchando en puerto {port}...")
        server.bind(("0.0.0.0", port))
        server.listen(10)
        while True:
            conn, addr = server.accept()
            threading.Thread(target=handle_request, args=(conn,)).start()
    except Exception as e: print(f"[MASTER Error] {e}")
    finally: server.close()

def handle_request(conn):
    try:
        data = conn.recv(BUFFER_SIZE).decode("utf-8")
        if not data: return
        request = json.loads(data)
        req_type = request.get("type")
        print(f"\n[MASTER] Solicitud: {req_type}")
        response = {"status": MSG_ERROR, "msg": "Petición no reconocida"}

        if req_type == "REGISTER_PATIENT":
            sql_local = "INSERT INTO pacientes (nombre, seguro_social) VALUES (?, ?)"
            params_local = (request["nombre"], request["seguro"])
            res_db = db.ejecutar_escritura(sql_local, params_local)
            if res_db["status"] == "OK":
                id_generado = res_db["id"]
                sql_rep = "INSERT INTO pacientes (id_paciente, nombre, seguro_social) VALUES (?, ?, ?)"
                broadcast_to_slaves({"type": "WRITE", "sql": sql_rep, "params": (id_generado, request["nombre"], request["seguro"])}, sender_id=MY_NODE_ID)
                response = {"status": MSG_OK, "id": id_generado, "msg": "Paciente registrado"}
            else:
                response = {"status": MSG_ERROR, "msg": res_db.get("error")}

        elif req_type == MSG_NEW_VISIT:
            seguro = request.get("seguro")
            paciente = db.ejecutar_lectura("SELECT id_paciente FROM pacientes WHERE seguro_social = ?", (seguro,))
            if paciente["status"] == "OK" and len(paciente["data"]) > 0:
                response = create_visit_transaction(paciente["data"][0]["id_paciente"])
            else:
                response = {"status": MSG_ERROR, "msg": "Paciente no encontrado"}

        elif req_type == "CHECK_AVAIL":
            sql_camas = """
                SELECT n.nombre as sala, 
                       SUM(CASE WHEN c.estado = 'LIBRE' THEN 1 ELSE 0 END) as libres,
                       SUM(CASE WHEN c.estado = 'OCUPADA' THEN 1 ELSE 0 END) as ocupadas
                FROM camas c JOIN nodos n ON c.id_sala = n.id_sala GROUP BY n.nombre
            """
            sql_docs = "SELECT SUM(capacidad_max - carga_actual) as cupos FROM doctores WHERE carga_actual < capacidad_max"
            
            res_camas = db.ejecutar_lectura(sql_camas, [])
            res_docs = db.ejecutar_lectura(sql_docs, [])
            total_cupos = res_docs["data"][0]["cupos"] if res_docs["data"][0]["cupos"] else 0
            
            response = {
                "status": MSG_OK,
                "desglose_camas": res_camas["data"],
                "doctores_libres": total_cupos 
            }

        elif req_type == "GET_ACTIVE_VISITS":
            sql = """
                SELECT v.folio, p.nombre as paciente, v.fecha_ingreso, n.nombre as sala, d.nombre as doctor
                FROM visitas v
                JOIN pacientes p ON v.id_paciente = p.id_paciente
                JOIN nodos n ON v.id_sala = n.id_sala
                JOIN doctores d ON v.id_doctor = d.id_doctor
                WHERE v.estado = 'EN_PROCESO'
            """
            res = db.ejecutar_lectura(sql, [])
            response = {"status": MSG_OK, "visitas": res["data"] if res["status"] == "OK" else []}

        elif req_type == "GET_ALL_PATIENTS":
             sql = "SELECT id_paciente, nombre, seguro_social, triage FROM pacientes"
             res = db.ejecutar_lectura(sql, [])
             response = {"status": MSG_OK, "pacientes": res["data"] if res["status"] == "OK" else []}

        elif req_type == "CLOSE_VISIT":
            folio = request.get("folio")
            response = close_visit_transaction(folio)

        conn.sendall(json.dumps(response).encode("utf-8"))
    except Exception as e:
        print(f"[MASTER Error] {e}")
        conn.sendall(json.dumps({"status": MSG_ERROR, "msg": str(e)}).encode("utf-8"))
    finally: conn.close()

def create_visit_transaction(id_paciente):
    with mutex_asignacion:
        print("[MASTER] Iniciando asignación")
        
        # BUSCAR DOCTOR
        sql_doc = """
            SELECT id_doctor, carga_actual, capacidad_max 
            FROM doctores 
            WHERE carga_actual < capacidad_max 
            ORDER BY carga_actual ASC LIMIT 1
        """
        res_doc = db.ejecutar_lectura(sql_doc, [])
        
        # BUSCAR CAMA Y SU SALA
        sql_cama = """
            SELECT id_cama, id_sala 
            FROM camas 
            WHERE estado = 'LIBRE' 
            LIMIT 1
        """
        res_cama = db.ejecutar_lectura(sql_cama, [])

        if not (res_doc["data"] and res_cama["data"]):
            return {"status": "ERROR", "msg": "No hay recursos (Cama o Doctor saturados)"}

        doctor = res_doc["data"][0]
        cama = res_cama["data"][0]  # Datos de la cama
        
        id_doctor = doctor["id_doctor"]
        id_cama = cama["id_cama"]
        
        id_sala_real = cama["id_sala"] 

        folio = generate_folio(id_paciente, id_doctor, id_sala_real)
        fecha_actual = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # ACTUALIZAR RECURSOS
            nueva_carga = doctor["carga_actual"] + 1
            nuevo_estado = "SATURADO" if nueva_carga >= doctor["capacidad_max"] else "DISPONIBLE"
            
            sql_update_doc = "UPDATE doctores SET carga_actual = ?, estado = ? WHERE id_doctor = ?"
            params_doc = (nueva_carga, nuevo_estado, id_doctor)
            
            sql_update_cama = "UPDATE camas SET estado = ? WHERE id_cama = ?"
            params_cama = (CAMA_OCUPADA, id_cama)

            # Insertar Visita usando id_sala_real
            sql_insert = """
                INSERT INTO visitas (folio, id_paciente, id_doctor, id_cama, id_sala, fecha_ingreso, estado)
                VALUES (?, ?, ?, ?, ?, ?, 'EN_PROCESO')
            """
            params_insert = (folio, id_paciente, id_doctor, id_cama, id_sala_real, fecha_actual)
            
            # Ejecutar local
            db.ejecutar_escritura(sql_update_doc, params_doc)
            db.ejecutar_escritura(sql_update_cama, params_cama)
            db.ejecutar_escritura(sql_insert, params_insert)

            # Replicar
            ops = [
                {"sql": sql_update_doc, "params": params_doc},
                {"sql": sql_update_cama, "params": params_cama},
                {"sql": sql_insert, "params": params_insert}
            ]
            for op in ops:
                broadcast_to_slaves({"type": "WRITE", "sql": op["sql"], "params": op["params"]}, sender_id=MY_NODE_ID)

            print(f"[MASTER] Visita creada: {folio} en Sala {id_sala_real}")
            return {"status": MSG_OK, "folio": folio, "fecha_ingreso": fecha_actual}

        except Exception as e:
            return {"status": "ERROR", "msg": str(e)}

def close_visit_transaction(folio):
    with mutex_asignacion:
        folio = folio.strip()
        print(f"[MASTER] Cerrando visita: '{folio}'")
        
        sql_get = "SELECT id_doctor, id_cama FROM visitas WHERE folio = ? AND estado = 'EN_PROCESO'"
        visita = db.ejecutar_lectura(sql_get, (folio,))
        
        if not visita or not visita["data"]:
            return {"status": "ERROR", "msg": "Folio no encontrado"}
            
        id_doctor = visita["data"][0]["id_doctor"]
        id_cama = visita["data"][0]["id_cama"]
        fecha_salida = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            # 1. Liberar Doctor 
            doc_data = db.ejecutar_lectura("SELECT carga_actual FROM doctores WHERE id_doctor=?", (id_doctor,))["data"][0]
            nueva_carga = max(0, doc_data["carga_actual"] - 1)
            
            sql_doc = "UPDATE doctores SET carga_actual = ?, estado = 'DISPONIBLE' WHERE id_doctor = ?"
            params_doc = (nueva_carga, id_doctor)
            
            # 2. Liberar Cama
            sql_cama = "UPDATE camas SET estado = ? WHERE id_cama = ?"
            params_cama = (CAMA_LIBRE, id_cama)
            
            # 3. Cerrar Visita
            sql_visita = "UPDATE visitas SET estado = 'CERRADA', fecha_salida = ? WHERE folio = ?"
            params_visita = (fecha_salida, folio)

            db.ejecutar_escritura(sql_doc, params_doc)
            db.ejecutar_escritura(sql_cama, params_cama)
            db.ejecutar_escritura(sql_visita, params_visita)
            
            ops = [
                {"sql": sql_doc, "params": params_doc},
                {"sql": sql_cama, "params": params_cama},
                {"sql": sql_visita, "params": params_visita}
            ]
            for op in ops:
                broadcast_to_slaves({"type": "WRITE", "sql": op["sql"], "params": op["params"]}, sender_id=MY_NODE_ID)

            return {"status": "OK", "msg": "Alta procesada"}
        except Exception as e:
            return {"status": "ERROR", "msg": str(e)}

def generate_folio(paciente, doctor, sala):
    import random
    return f"P{paciente}-D{doctor}-S{sala}-{random.randint(1000, 9999)}"

if __name__ == "__main__": start_master_listener()