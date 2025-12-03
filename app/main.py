import sys
import os
import time
import threading

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.services.storage_service import StorageService
from app.services.master_service import start_master_listener
from app.services.election_service import ElectionService
from app.core.detector_failure import DetectorFallas 
from app.data_access.db_manager import set_db_context, DatabaseManager 
from app.common.config_loader import load_cluster_config

DB_DIR = "data"
SCHEMA_PATH = "config/schema.sql"

#VARIABLES GLOBALES DE ESTADO
current_master_id = None
soy_maestro = False
election_service = None
detector = None
my_node_id = None
my_node_config = None

# --- FUNCIONES AUXILIARES

def preparar_topologia_detector(config):
    """Calcula los puertos para el detector de fallas (offset +200)."""
    mapa_nodos = {}
    for node in config["nodes"]:
        puerto_detector = node["port_manager"] + 200 
        mapa_nodos[str(node["id"])] = (node["host"], puerto_detector)
    return mapa_nodos

# CALLBACKS DE EVENTOS (Ciclo de Vida)

def on_me_convierto_en_maestro():
    """Se ejecuta cuando GANO la elección."""
    global soy_maestro, current_master_id
    if soy_maestro: return 

    soy_maestro = True
    current_master_id = my_node_id
    print(f"\n [ROL] ¡He ganado la elección! Ascendiendo a MAESTRO (Nodo {my_node_id})...")
    
    # Iniciar servicio Maestro 
    t_maestro = threading.Thread(
        target=start_master_listener, 
        args=(my_node_config['port_manager'], my_node_id), 
        daemon=True
    )
    t_maestro.start()
    
    # Actualizar detector: Ahora yo vigilo a los esclavos
    if detector:
        detector.set_rol_maestro(True) 

def on_nuevo_maestro_electo(new_master_id):
    """Se ejecuta cuando OTRO gana la elección."""
    global current_master_id, soy_maestro
    print(f"\n [ROL] Reconociendo al Nodo {new_master_id} como nuevo Maestro.")
    
    current_master_id = new_master_id
    soy_maestro = False
    
    # Actualizar detector: Ahora vigilo al nuevo maestro
    if detector:
        detector.set_target_maestro(str(new_master_id))

def al_detectar_fallo_maestro(id_nodo_caido):
    """Callback que dispara el Detector de Fallas cuando alguien muere."""
    global current_master_id
    
    print(f"\n [DEBUG] Detector reporta caída del nodo: {id_nodo_caido}")
    
    # Convertimos a string para asegurar comparación correcta
    if str(id_nodo_caido) == str(current_master_id):
        print(f" [ALERTA CRÍTICA] ¡El Maestro (Nodo {id_nodo_caido}) ha muerto!")
        print(" Iniciando Algoritmo de Elección (Bully)...")
        
        if election_service:
            election_service.start_election()
    else:
        print(f" [INFO] El nodo caído ({id_nodo_caido}) no era el maestro actual ({current_master_id}).")

# MAIN

def main(node_id):
    global election_service, detector, my_node_id, my_node_config, current_master_id, soy_maestro
    
    my_node_id = node_id
    print(f"\n===  INICIANDO NODO {node_id} ===\n")
    
    # Cargar Configuración
    config = load_cluster_config()
    # Buscar mi configuración específica
    my_node_config = next((n for n in config["nodes"] if n["id"] == node_id), None)
    if not my_node_config:
        print(f" Error: ID {node_id} no encontrado en configuración.")
        return

    current_master_id = config["initial_master_id"]
    soy_maestro = False

    # Preparar Base de Datos
    if not os.path.exists(DB_DIR): os.makedirs(DB_DIR)
    ruta_db = os.path.join(DB_DIR, f"nodo_{node_id}.db")
    set_db_context(ruta_db)
    
    # Si no existe, la creamos
    if not os.path.exists(ruta_db): 
        DatabaseManager(ruta_db, SCHEMA_PATH)

    # SERVICIO DE ALMACENAMIENTO (Siempre activo, puerto 900X)
    servicio_storage = StorageService(ruta_db, my_node_config["port_db"], "0.0.0.0")
    threading.Thread(target=servicio_storage.start, daemon=True).start()
    
    # SERVICIO DE ELECCIÓN (Siempre activo, puerto 910X)
    election_service = ElectionService(node_id, on_me_convierto_en_maestro, on_nuevo_maestro_electo)
    election_service.start()

    # DETECTOR DE FALLAS (Siempre activo, puerto 820X)
    mapa_nodos = preparar_topologia_detector(config)
    mi_puerto_detector = my_node_config["port_manager"] + 200
    
    detector = DetectorFallas(
        id_nodo=str(node_id),
        host=my_node_config["host"],
        puerto=mi_puerto_detector,
        nodos_cluster=mapa_nodos,
        es_maestro=soy_maestro,
        id_maestro=str(current_master_id),
        al_detectar_fallo=al_detectar_fallo_maestro
    )
    detector.iniciar()

    # LÓGICA DE MAESTRO INICIAL
    print(f"Iniciando nodo. Estado: Indeterminado. Buscando líder...")
    if election_service:
        election_service.start_election()
    else:
        print(f" Iniciando como Esclavo (Monitor del Maestro {current_master_id})")

    # BUCLE PRINCIPAL
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\n Apagando nodo...")
        detector.detener()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python -m app.main <ID>")
    else:
        main(int(sys.argv[1]))