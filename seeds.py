import sys
import os
import time

# Ajuste de rutas para importaciones
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from app.data_access.db_manager import DatabaseManager, set_db_context
from app.services.replication_service import broadcast_to_slaves
from app.common.config_loader import load_cluster_config

# Configuración
DB_MASTER_PATH = "data/nodo_1.db" # Asumimos que el 1 es el maestro inicial
SCHEMA_PATH = "config/schema.sql"

def sembrar_datos():
    print("INICIANDO POBLADO DE DATOS (SEEDS)")
    
    # 1. Configurar contexto para el Maestro
    # Esto permite escribir en la BD local del nodo 1
    set_db_context(DB_MASTER_PATH)
    db = DatabaseManager(DB_MASTER_PATH, SCHEMA_PATH)

    # 2. Datos a insertar
    # (Tabla, SQL, Parámetros)
    datos_semilla = [
        # --- SALAS (NODOS) ---
        {
            "info": "Registrando Sala Norte",
            "sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)",
            "params": (1, "Sala Norte", "127.0.0.1", 9001)
        },
        {
            "info": "Registrando Sala Sur",
            "sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)",
            "params": (2, "Sala Sur", "127.0.0.1", 9002)
        },
        
        # --- DOCTORES ---
        {
            "info": "Contratando Dr. House",
            "sql": "INSERT INTO doctores (nombre, especialidad, estado) VALUES (?, ?, ?)",
            "params": ("Dr. Gregory House", "Diagnóstico", "DISPONIBLE")
        },
        {
            "info": "Contratando Dra. Grey",
            "sql": "INSERT INTO doctores (nombre, especialidad, estado) VALUES (?, ?, ?)",
            "params": ("Dra. Meredith Grey", "Cirugía", "DISPONIBLE")
        },

        # --- CAMAS ---
        {
            "info": "Instalando Cama A-100",
            "sql": "INSERT INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, ?)",
            "params": (1, "A-100", "LIBRE")
        },
        {
            "info": "Instalando Cama B-200",
            "sql": "INSERT INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, ?)",
            "params": (2, "B-200", "LIBRE")
        }
    ]

    # 3. Ejecución y Replicación
    for item in datos_semilla:
        print(f"{item['info']}...")
        
        # A) Escritura Local (Maestro)
        res = db.ejecutar_escritura(item['sql'], item['params'])
        
        if res['status'] == 'OK':
            print(" Guardado en Maestro (Nodo 1)")
            
            # B) Replicación a Esclavos
            # Construimos el paquete WRITE tal como lo espera el StorageService
            paquete_replicacion = {
                "type": "WRITE",
                "sql": item['sql'],
                "params": item['params']
            }
            
            broadcast_to_slaves(paquete_replicacion)
        else:
            print(f"Error en Maestro: {res.get('error')}")
        
        time.sleep(0.1) # Pequeña pausa para no saturar logs visuales

    print("\n POBLADO FINALIZADO")

if __name__ == "__main__":
    # Advertencia: Esto requiere que los Nodos Esclavos (2, 3, 4) estén encendidos
    # para recibir la replicación.
    sembrar_datos()