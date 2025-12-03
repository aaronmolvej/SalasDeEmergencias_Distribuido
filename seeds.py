import sys
import os
from app.data_access.db_manager import DatabaseManager, set_db_context

# Ajuste de rutas
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

SCHEMA_PATH = "config/schema.sql"

def sembrar_datos_locales(node_id):
    # 1. Definir el nombre correcto de la BD para ESTA máquina
    db_path = f"data/nodo_{node_id}.db"
    
    print(f"--- SEMBRANDO DATOS EN: {db_path} ---")
    
    # Configurar contexto y asegurar que tablas existan
    set_db_context(db_path)
    # Al instanciar, se crea el esquema si no existe
    db = DatabaseManager(db_path, SCHEMA_PATH) 

    # 2. Datos Maestros (Idénticos para todos)
    datos = [
        # --- NODOS (Topología) ---
        # Asegúrate que estas IPs sean las REALES de tu cluster_config.json
        {"sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)", "params": (1, "Sala Norte", "192.168.89.131", 9001)},
        {"sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)", "params": (2, "Sala Sur",   "192.168.89.132", 9001)},
        {"sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)", "params": (3, "Sala Este",  "192.168.89.130", 9001)},
        {"sql": "INSERT OR IGNORE INTO nodos (id_sala, nombre, ip, puerto) VALUES (?, ?, ?, ?)", "params": (4, "Sala Oeste", "192.168.89.133", 9001)},

        # --- DOCTORES ---
        {"sql": "INSERT OR IGNORE INTO doctores (nombre, especialidad, carga_actual, capacidad_max) VALUES (?, ?, 0, 2)", "params": ("Dr. House", "Diagnóstico")},
        {"sql": "INSERT OR IGNORE INTO doctores (nombre, especialidad, carga_actual, capacidad_max) VALUES (?, ?, 0, 3)", "params": ("Dra. Grey", "Cirugía")},
        {"sql": "INSERT OR IGNORE INTO doctores (nombre, especialidad, carga_actual, capacidad_max) VALUES (?, ?, 0, 1)", "params": ("Dr. Strange", "Neurocirugía")},

        # --- CAMAS ---
        {"sql": "INSERT OR IGNORE INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, 'LIBRE')", "params": (1, "A-100")},
        {"sql": "INSERT OR IGNORE INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, 'LIBRE')", "params": (1, "A-101")},
        {"sql": "INSERT OR IGNORE INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, 'LIBRE')", "params": (2, "B-200")},
        {"sql": "INSERT OR IGNORE INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, 'LIBRE')", "params": (2, "B-201")},
        {"sql": "INSERT OR IGNORE INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, 'LIBRE')", "params": (3, "C-300")},
        {"sql": "INSERT OR IGNORE INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, 'LIBRE')", "params": (3, "C-301")},
        {"sql": "INSERT OR IGNORE INTO camas (id_sala, numero_cama, estado) VALUES (?, ?, 'LIBRE')", "params": (4, "D-400")},
    ]

    # 3. Inserción Directa (Sin Sockets, Sin Replicación)
    for item in datos:
        res = db.ejecutar_escritura(item['sql'], item['params'])
        if res['status'] != 'OK':
            print(f" Error en {item['params'][0]}: {res.get('error')}")
    
    print(f"Poblado local completado para Nodo {node_id}.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python seeds.py <ID_DE_ESTE_NODO>")
        sys.exit(1)
    
    sembrar_datos_locales(int(sys.argv[1]))