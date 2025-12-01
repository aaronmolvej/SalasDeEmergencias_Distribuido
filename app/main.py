import sys
import os
import json
import time
import threading

# Ajuste de path para que funcione desde la carpeta raíz
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.storage_service import StorageService
from app.services.master_service import start_master_listener

# from app.services.master_service import MasterService (Aún no creado)

CONFIG_PATH = "config/cluster_config.json"

def cargar_config():
    if not os.path.exists(CONFIG_PATH):
        raise Exception(f"No se encontró {CONFIG_PATH}")
    with open(CONFIG_PATH) as f:
        return json.load(f)

def obtener_info_nodo(node_id, config):
    """Busca la IP y puertos de este nodo en el JSON"""
    for node in config['nodes']:
        if node['id'] == node_id:
            return node
    raise Exception(f"Nodo con ID {node_id} no encontrado en configuración")

def main(node_id):
    print(f"--- INICIANDO NODO {node_id} ---")
    config = cargar_config()
    my_info = obtener_info_nodo(node_id, config)
    
    # Iniciar Servicio de Almacenamiento (Base de Datos)
    # Esto crea/conecta a data/nodo_1.db, nodo_2.db, etc.
    db_file = f"data/nodo_{node_id}.db"
    storage = StorageService(db_file, my_info['port_db'])
    
    # Corremos el storage en un hilo daemon para que no bloquee el main
    storage_thread = threading.Thread(target=storage.start, daemon=True)
    storage_thread.start()

    # Esperamos un poco para asegurar que el storage subió
    time.sleep(1)

    # Lógica de Roles (Maestro vs Esclavo)
    initial_master = config['initial_master_id']
    
    if node_id == initial_master:
    print(f"Soy el NODO MAESTRO (ID: {node_id})")

    print(f"[MASTER] Iniciando servicio maestro en puerto {my_info['port_manager']}...")

    # Iniciar el servidor maestro en un hilo separado
    master_thread = threading.Thread(
        target=start_master_listener,
        daemon=True
    )
    master_thread.start()

    else:
        print(f"Soy un NODO ESCLAVO (ID: {node_id})")
        print(f"   Esperando órdenes del Maestro {initial_master}...")

    # Mantener el programa vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nApagando nodo...")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python -m app.main <node_id>")
        sys.exit(1)
    
    # python -m app.main 1
    id_nodo = int(sys.argv[1])
    main(id_nodo)
