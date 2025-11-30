import socket
import threading
from app.common.protocol import recv_json, send_json
from app.common.constants import MSG_ERROR, MSG_OK
from app.data_access.db_manager import DatabaseManager

class StorageService:
    def __init__(self, db_path, port, host='0.0.0.0'):
        self.host = host
        self.port = port
        self.db_path = db_path
        self.running = False
        
        # Instanciamos el gestor de BD (esto crea tablas si no existen)
        self.db = DatabaseManager(db_path, "config/schema.sql")

    def start(self):
        self.running = True
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR evita el error "Address already in use" al reiniciar rápido
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)
            print(f"[Storage] Nodo escuchando en {self.host}:{self.port} (BD: {self.db_path})")

            while self.running:
                try:
                    client_sock, addr = server_socket.accept()
                    # Cada petición se maneja en un hilo separado para no bloquear al nodo
                    client_handler = threading.Thread(
                        target=self._handle_client,
                        args=(client_sock,)
                    )
                    client_handler.start()
                except OSError:
                    break
                    
        except Exception as e:
            print(f"[Storage Error] No se pudo iniciar el servidor: {e}")
        finally:
            server_socket.close()

    def _handle_client(self, client_socket):
        try:
            request = recv_json(client_socket)
            if not request:
                return

            # print(f"[Storage] Petición recibida: {request}")
            response = self._process_request(request)
            send_json(client_socket, response)
            
        except Exception as e:
            print(f"[Storage Error] Procesando cliente: {e}")
            error_response = {"status": MSG_ERROR, "message": str(e)}
            send_json(client_socket, error_response)
        finally:
            client_socket.close()

    def _process_request(self, request):
        req_type = request.get("type")
        sql = request.get("sql")
        params = tuple(request.get("params", []))

        if req_type == "WRITE":
            # Usado para INSERT, UPDATE, DELETE
            # Esto será llamado tanto por operaciones locales como por REPLICACIÓN del maestro
            return self.db.ejecutar_escritura(sql, params)
        
        elif req_type == "READ":
            # Usado para SELECT
            return self.db.ejecutar_lectura(sql, params)
            
        return {"status": MSG_ERROR, "message": "Tipo de petición desconocido"}