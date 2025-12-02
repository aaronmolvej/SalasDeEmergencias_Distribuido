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
        # REGISTRO DE SALAS 
        # Asegúrate que las IPs coincidan con tu cluster_config.json
        {
            "info": "Registrando Sala Norte (Nodo 1)",
            "sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)",
            "params": (1, "Sala Norte", "192.168.1.10", 9001)
        },
        {
            "info": "Registrando Sala Sur (Nodo 2)",
            "sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)",
            "params": (2, "Sala Sur", "192.168.1.11", 9001)
        },
        {
            "info": "Registrando Sala Este (Nodo 3)",
            "sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)",
            "params": (3, "Sala Este", "192.168.1.12", 9001)
        },
        {
            "info": "Registrando Sala Oeste (Nodo 4)",
            "sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)",
            "params": (4, "Sala Oeste", "192.168.1.13", 9001)
        },
        
        # DOCTORES 
        {
            "info": "Contratando Dr. House (Capacidad: 2)",
            "sql": "INSERT INTO doctores (nombre, especialidad, carga_actual, capacidad_max) VALUES (?, ?, 0, 2)",
            "params": ("Dr. Gregory House", "Diagnóstico")
        },
        {
            "info": "Contratando Dra. Grey (Capacidad: 3)",
            "sql": "INSERT INTO doctores (nombre, especialidad, carga_actual, capacidad_max) VALUES (?, ?, 0, 3)",
            "params": ("Dra. Meredith Grey", "Cirugía")
        },
        {
            "info": "Contratando Dr. Strange (Capacidad: 1)",
            "sql": "INSERT INTO doctores (nombre, especialidad, carga_actual, capacidad_max) VALUES (?, ?, 0, 1)",
            "params": ("Dr. Stephen Strange", "Neurocirugía")
        },

        # 3. CAMAS
        # Nodo 1
        {"info": "Cama A-100 (Norte)", "sql": "INSERT INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, ?)", "params": (1, "A-100", "LIBRE")},
        {"info": "Cama A-101 (Norte)", "sql": "INSERT INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, ?)", "params": (1, "A-101", "LIBRE")},
        # Nodo 2
        {"info": "Cama B-200 (Sur)", "sql": "INSERT INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, ?)", "params": (2, "B-200", "LIBRE")},
        {"info": "Cama B-201 (Sur)", "sql": "INSERT INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, ?)", "params": (2, "B-201", "LIBRE")},
        # Nodo 3 
        {"info": "Cama C-300 (Este)", "sql": "INSERT INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, ?)", "params": (3, "C-300", "LIBRE")},
        {"info": "Cama C-301 (Este)", "sql": "INSERT INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, ?)", "params": (3, "C-301", "LIBRE")},
        # Nodo 4 
        {"info": "Cama D-400 (Oeste)", "sql": "INSERT INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, ?)", "params": (4, "D-400", "LIBRE")},
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