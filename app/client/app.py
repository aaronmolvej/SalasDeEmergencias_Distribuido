import json
import socket

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 8001

def send_to_master(data):
    """Envía un mensaje JSON al maestro y devuelve la respuesta."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((MASTER_HOST, MASTER_PORT))
            s.sendall(json.dumps(data).encode())
            response = s.recv(4096).decode()
            return json.loads(response)
    except ConnectionRefusedError:
        print("No se pudo conectar al Maestro. ¿Está ejecutándose?")
        return None


def registrar_paciente():
    print("\n--- Registrar Paciente ---")
    nombre = input("Nombre del paciente: ")
    seguro = input("Número de seguro social: ")

    msg = {
        "type": "REGISTER_PATIENT",
        "nombre": nombre,
        "seguro": seguro
    }

    resp = send_to_master(msg)
    if resp:
        print("✔ Paciente registrado. ID:", resp.get("id"))


def ingresar_visita():
    print("\n--- Nueva Visita de Urgencia ---")
    seguro = input("Seguro social del paciente: ")

    msg = {
        "type": "NEW_VISIT",
        "seguro": seguro
    }

    resp = send_to_master(msg)
    if resp:
        print("Visita registrada. Folio:", resp.get("folio"))


def ver_disponibilidad():
    print("\nDisponibilidad de Salas")

    msg = {"type": "CHECK_AVAIL"}

    resp = send_to_master(msg)
    if resp:
        print("Camas disponibles:", resp.get("camas"))
        print("Doctores disponibles:", resp.get("doctores"))


def main_menu():
    while True:
        print("\n=== Sistema de Emergencias ===")
        print("1. Registrar Paciente")
        print("2. Ingresar Visita")
        print("3. Ver Disponibilidad")
        print("4. Salir")

        opcion = input("Selecciona una opción: ")

        if opcion == "1":
            registrar_paciente()
        elif opcion == "2":
            ingresar_visita()
        elif opcion == "3":
            ver_disponibilidad()
        elif opcion == "4":
            print("Saliendo…")
            break
        else:
            print("Opción inválida")


if __name__ == "__main__":
    main_menu()
