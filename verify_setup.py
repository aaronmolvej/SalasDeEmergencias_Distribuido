import os
import json
from app.data_access.db_manager import DatabaseManager
from app.common.constants import MSG_OK

def test_setup():
    print("Verificando Configuración...")
    try:
        with open('config/cluster_config.json') as f:
            config = json.load(f)
            print(f"   OK. Maestro inicial ID: {config['initial_master_id']}")
    except Exception as e:
        print(f"   FALLO: {e}")
        return

    print("Verificando Base de Datos...")
    db_path = "data/test_node.db"
    # Limpieza previa
    if os.path.exists(db_path): os.remove(db_path)
    
    try:
        db = DatabaseManager(db_path, "config/schema.sql")
        # Prueba de escritura
        res = db.ejecutar_escritura("INSERT INTO nodos (id_sala, nombre) VALUES (1, 'Test')")
        if res['status'] == 'OK':
            print("   OK. Tabla creada e inserción exitosa.")
        else:
            print(f"   FALLO en SQL: {res}")
    except Exception as e:
        print(f"   FALLO Crítico DB: {e}")

    print("3. Verificando Importación de Constantes...")
    if MSG_OK == "OK":
        print("   OK. Módulos visibles.")

if __name__ == "__main__":
    test_setup()