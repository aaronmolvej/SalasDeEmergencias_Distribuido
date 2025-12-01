import sys
import os
import time
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.storage_service import StorageService
from app.services.master_service import start_master_listener
from app.core.detector_failure import DetectorFallas
from app.data_access.db_manager import set_db_context, DatabaseManager 
from app.common.config_loader import load_cluster_config

DB_DIR = "data"
SCHEMA_PATH = "config/schema.sql"

def preparar_topologia_detector(config):
    mapa_nodos = {}
    for node in config["nodes"]:
        puerto_detector = node["port_manager"] + 100 
        mapa_nodos[str(node["id"])] = (node["host"], puerto_detector)
    return mapa_nodos

def callback_fallo_detectado(id_nodo):
    print(f"\n[ALERTA] Nodo {id_nodo} ha caído.")

def main(node_id):
    print(f"\n===INICIANDO NODO HOSPITALARIO {node_id} ===\n")
    
    config = load_cluster_config()
    
    # Buscar mi configuración
    mi_info = next((n for n in config["nodes"] if n["id"] == node_id), None)
    if not mi_info:
        print(f"Error: ID {node_id} no encontrado en config.")
        return

    master_id_actual = config["initial_master_id"]
    soy_maestro = (node_id == master_id_actual)
    ruta_db = os.path.join(DB_DIR, f"nodo_{node_id}.db")

    # CONFIGURAR CONTEXTO GLOBAL DE BD
    # permite que master_service y replication_service sepan dónde escribir
    set_db_context(ruta_db)
    
    # Inicializar esquema si es la primera vez
    if not os.path.exists(ruta_db):
        DatabaseManager(ruta_db, SCHEMA_PATH)

    # INICIAR SERVICIOS
    
    # A) Storage Service 
    servicio_storage = StorageService(ruta_db, mi_info["port_db"], mi_info["host"])
    t_storage = threading.Thread(target=servicio_storage.start, daemon=True)
    t_storage.start()
    
    # B) Detector failure
    mapa_nodos = preparar_topologia_detector(config)
    mi_puerto_detector = mi_info["port_manager"] + 100
    detector = DetectorFallas(
        id_nodo=str(node_id),
        host=mi_info["host"],
        puerto=mi_puerto_detector,
        nodos_cluster=mapa_nodos,
        es_maestro=soy_maestro,
        id_maestro=str(master_id_actual),
        al_detectar_fallo=callback_fallo_detectado
    )
    detector.iniciar()

    # 3. ROL DE MAESTRO
    if soy_maestro:
        print(f"\nNODO MAESTRO ACTIVO")
        t_maestro = threading.Thread(
            target=start_master_listener, 
            args=(mi_info['port_manager'],), 
            daemon=True
        )
        t_maestro.start()
    else:
        print(f"\nNODO ESCLAVO ACTIVO")

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("Apagando...")
        detector.detener()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python -m app.main <ID>")
        sys.exit(1)
    main(int(sys.argv[1]))