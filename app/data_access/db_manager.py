import sqlite3
import os

CURRENT_DB_PATH = None

def set_db_context(db_path):
    global CURRENT_DB_PATH
    CURRENT_DB_PATH = db_path
    # Asegurar que el directorio existe
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

def get_connection():
    if not CURRENT_DB_PATH:
        raise Exception("La ruta de la BD no ha sido configurada. Llama a set_db_context() en main.py")
    conn = sqlite3.connect(CURRENT_DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn

def execute_sql(sql, params=()):
    """Ejecuta INSERT, UPDATE, DELETE"""
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        return {"status": "OK", "id": cursor.lastrowid, "rows": cursor.rowcount}
    except Exception as e:
        print(f"[DB Error] SQL: {sql} | Error: {e}")
        return {"status": "ERROR", "msg": str(e)}
    finally:
        conn.close()

def fetch_one(sql, params=()):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        row = cursor.fetchone()
        return row if row else None
    except Exception as e:
        print(f"[DB Error] {e}")
        return None
    finally:
        conn.close()

def fetch_all(sql, params=()):
    conn = get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    except Exception as e:
        print(f"[DB Error] {e}")
        return []
    finally:
        conn.close()

class DatabaseManager:
    def __init__(self, db_path, schema_path):
        self.db_path = db_path
        self.schema_path = schema_path
        # Inicializar esquema
        self._init_schema()

    def _init_schema(self):
        if not os.path.exists(self.schema_path): return
        conn = sqlite3.connect(self.db_path)
        with open(self.schema_path, 'r') as f:
            conn.executescript(f.read())
        conn.close()

    def ejecutar_escritura(self, sql, params):
        return execute_sql(sql, params) # Redirige a la función global

    def ejecutar_lectura(self, sql, params):
        res = fetch_all(sql, params)
        return {"status": "OK", "data": res}