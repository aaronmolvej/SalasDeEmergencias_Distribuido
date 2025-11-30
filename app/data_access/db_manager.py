import sqlite3
import os

class DatabaseManager:
    def __init__(self, db_path, schema_path):
        self.db_path = db_path
        self.schema_path = schema_path
        self._init_db()

    def _get_conn(self):
        # Conectar a la BD y habilitar Foreign Keys
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row # Para recibir diccionarios, no tuplas
        return conn

    def _init_db(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = self._get_conn()
        try:
            if os.path.exists(self.schema_path):
                with open(self.schema_path, 'r') as f:
                    conn.executescript(f.read())
                conn.commit()
                # print(f"[DB] Base de datos verificada en: {self.db_path}")
            else:
                print(f"[DB Error] No se encontró el esquema en {self.schema_path}")
        except Exception as e:
            print(f"[DB Error] {e}")
        finally:
            conn.close()

    def ejecutar_escritura(self, sql, params=()):
        conn = self._get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            conn.commit()
            return {"status": "OK", "id": cursor.lastrowid, "cambios": cursor.rowcount}
        except sqlite3.IntegrityError as e:
            return {"status": "ERROR", "error": f"Violación de integridad: {e}"}
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}
        finally:
            conn.close()

    def ejecutar_lectura(self, sql, params=()):
        conn = self._get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall()
            # Convertir objetos Row a diccionarios reales de Python
            data = [dict(row) for row in rows]
            return {"status": "OK", "data": data}
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}
        finally:
            conn.close()