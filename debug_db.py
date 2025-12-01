import sqlite3
import os

# Ruta exacta donde esperamos que esté la BD del Nodo 2
DB_PATH = "data/nodo_2.db"

print(f"--- DIAGNÓSTICO DE BASE DE DATOS: {DB_PATH} ---")

# 1. Verificar existencia física del archivo
if not os.path.exists(DB_PATH):
    print(f"ERROR CRÍTICO: El archivo {DB_PATH} NO EXISTE.")
    print("   Verifica que la carpeta 'data' exista y que el Nodo 2 haya arrancado.")
    print("   Contenido actual de la carpeta data/:")
    if os.path.exists("data"):
        print(os.listdir("data"))
    else:
        print("   (La carpeta 'data' no existe)")
    exit()
else:
    size = os.path.getsize(DB_PATH)
    print(f"Archivo encontrado. Tamaño: {size} bytes.")

# 2. Inspeccionar contenido
try:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Listar tablas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablas = cursor.fetchall()
    print(f"\nTablas encontradas: {[t[0] for t in tablas]}")

    # Consultar Pacientes
    print("\n--- CONSULTANDO TABLA PACIENTES ---")
    cursor.execute("SELECT * FROM pacientes")
    filas = cursor.fetchall()
    
    if not filas:
        print("LA TABLA ESTÁ VACÍA.")
        print("   Conclusión: El Nodo 2 recibió la orden de replicación, pero el INSERT falló o no hizo commit.")
    else:
        print(f"Se encontraron {len(filas)} registros:")
        for fila in filas:
            print(f"   -> {fila}")

    conn.close()

except Exception as e:
    print(f"Error leyendo la BD: {e}")