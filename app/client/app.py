import json
import socket
import time

POSIBLES_NODOS = [
    ("127.0.0.1", 8001), ("127.0.0.1", 8002), 
    ("127.0.0.1", 8003), ("127.0.0.1", 8004)
]
CLIENT_TIMEOUT = 10

def send_to_master(data):
    for host, port in POSIBLES_NODOS:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(CLIENT_TIMEOUT)
                s.connect((host, port))
                s.sendall(json.dumps(data).encode())
                response_bytes = s.recv(4096)
                if not response_bytes: continue
                return json.loads(response_bytes.decode())
        except (ConnectionRefusedError, socket.timeout): continue
    print("ERROR CRÍTICO: El sistema está caído.")
    return None

def registrar_paciente():
    print("\n--- Registrar Paciente ---")
    nombre = input("Nombre: ")
    seguro = input("Seguro Social: ")
    resp = send_to_master({"type": "REGISTER_PATIENT", "nombre": nombre, "seguro": seguro})
    if resp and resp.get("status") == "OK":
        print(f"✔ Paciente registrado. ID: {resp['id']}")
    else:
        print(f"Error: {resp.get('msg') if resp else 'Error de conexión'}")

def ingresar_visita():
    print("\n--- Nueva Visita de Urgencia ---")
    seguro = input("Seguro Social del paciente: ")
    resp = send_to_master({"type": "NEW_VISIT", "seguro": seguro})
    
    if resp and resp.get("status") == "OK":
        print(f"Visita registrada!")
        print(f" Folio: {resp['folio']}")
        print(f" Fecha: {resp['fecha_ingreso']}")
    else:
        print(f"Error: {resp.get('msg') if resp else 'Error de conexión'}")

# --- VISTA DETALLADA DE CAMAS ---
def ver_disponibilidad():
    print("\nDisponibilidad del Hospital")
    resp = send_to_master({"type": "CHECK_AVAIL"})
    
    if resp and resp.get("status") == "OK":
        print(f"\nDoctores Disponibles (Total): {resp['doctores_libres']}")
        print("\nDesglose de Camas por Sala:")
        print(f"{'SALA':<20} | {'LIBRES':<8} | {'OCUPADAS':<8}")
        print("-" * 42)
        
        # Iteramos sobre la lista que nos manda el maestro
        for sala in resp["desglose_camas"]:
            # Manejo seguro por si algún campo viene vacío
            nombre = str(sala.get('sala', 'Desconocida'))
            libres = str(sala.get('libres', 0))
            ocupadas = str(sala.get('ocupadas', 0))
            print(f"{nombre:<20} | {libres:<8} | {ocupadas:<8}")
        print("-" * 42)
    else:
        print("Error obteniendo disponibilidad.")

# --- VISTA DE DOCTOR (LISTA + SELECCIÓN) ---
def cerrar_visita():
    print("\n--- 🩺 Alta Médica / Cerrar Visita ---")
    resp = send_to_master({"type": "GET_ACTIVE_VISITS"})
    
    if resp and resp.get("status") == "OK":
        visitas = resp["visitas"]
        if not visitas:
            print("No hay pacientes en atención actualmente.")
            return

        # MODIFICADO: Agregar columna DOCTOR
        print("\nPACIENTES EN ATENCIÓN:")
        print(f"{'FOLIO':<18} | {'PACIENTE':<15} | {'DOCTOR':<15} | {'SALA':<10} | {'INGRESO'}")
        print("-" * 80)
        for v in visitas:
            # .get() seguro
            fol = str(v.get('folio',''))
            pac = str(v.get('paciente',''))
            doc = str(v.get('doctor',''))
            sal = str(v.get('sala',''))
            fec = str(v.get('fecha_ingreso',''))
            print(f"{fol:<18} | {pac:<15} | {doc:<15} | {sal:<10} | {fec}")
        print("-" * 80)
        
        folio = input("\nIngrese el FOLIO a dar de alta (o '0' para cancelar): ")
        if folio == '0': return

        resp_close = send_to_master({"type": "CLOSE_VISIT", "folio": folio.strip()})
        if resp_close and resp_close.get("status") == "OK":
            print(f"Alta exitosa. Recursos liberados.")
        else:
            print(f"rror al cerrar: {resp_close.get('msg')}")
    else:
        print("Error al consultar lista.")

def ver_reportes():
    while True:
        print("\nREPORTES DEL SISTEMA")
        print("1. Ver Pacientes Registrados")
        print("2. Ver Visitas en Curso")
        print("3. Volver al Menú Principal")
        op = input("Selecciona: ")

        if op == '1':
            resp = send_to_master({"type": "GET_ALL_PATIENTS"})
            if resp and resp.get("status") == "OK":
                print(f"\n{'ID':<5} | {'NOMBRE':<20} | {'SEGURO SOCIAL'}")
                print("-" * 45)
                for p in resp['pacientes']:
                    # Manejo seguro de datos
                    pid = str(p.get('id_paciente', ''))
                    nom = str(p.get('nombre', ''))
                    ss = str(p.get('seguro_social', ''))
                    print(f"{pid:<5} | {nom:<20} | {ss}")
                print("-" * 45)
            else:
                print("Error al obtener pacientes.")

        elif op == '2':
            resp = send_to_master({"type": "GET_ACTIVE_VISITS"})
            if resp and resp.get("status") == "OK":
                if not resp['visitas']:
                    print("\nNo hay visitas activas.")
                else:
                    print(f"\n{'FOLIO':<20} | {'PACIENTE':<15} | {'SALA':<12} | {'INGRESO'}")
                    print("-" * 65)
                    for v in resp['visitas']:
                        print(f"{v['folio']:<20} | {v['paciente']:<15} | {v['sala']:<12} | {v['fecha_ingreso']}")
                    print("-" * 65)
            else:
                print("Error al obtener visitas.")

        elif op == '3':
            break

def main_menu():
    while True:
        print("\nSISTEMA DISTRIBUIDO DE EMERGENCIAS ===")
        print("1. Registrar Paciente")
        print("2. Ingresar Visita")
        print("3. Ver Disponibilidad (Detallada)")
        print("4. Ver Reportes (Pacientes/Visitas)") 
        print("5. Cerrar Visita (Médico)")
        print("6. Salir")
        op = input("Selecciona: ")
        if op == '1': registrar_paciente()
        elif op == '2': ingresar_visita()
        elif op == '3': ver_disponibilidad()
        elif op == '4': ver_reportes()       
        elif op == '5': cerrar_visita()
        elif op == '6': break

if __name__ == "__main__":
    main_menu()