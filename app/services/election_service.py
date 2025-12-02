import socket
import threading
import json
import time
from app.common.config_loader import load_cluster_config
from app.common.protocol import send_json, recv_json
from app.common.constants import MSG_ELECTION, MSG_ELECTION_OK, MSG_COORDINATOR

class ElectionService:
    def __init__(self, my_id, on_promotion_callback, on_new_master_callback):
        self.my_id = my_id
        self.config = load_cluster_config()
        self.my_info = next(n for n in self.config["nodes"] if n["id"] == my_id)
        self.port = self.my_info["port_db"] + 100 # Puerto 910X
        
        # Callbacks para avisar al main.py
        self.on_promotion = on_promotion_callback     # "Me convertí en Maestro"
        self.on_new_master = on_new_master_callback   # "Otro es el Maestro"
        
        self.running = True
        self.election_in_progress = False

    def start(self):
        threading.Thread(target=self._listen, daemon=True).start()

    def _listen(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(("0.0.0.0", self.port))
        server.listen(5)
        print(f"[Elección] Escuchando en puerto {self.port}")

        while self.running:
            try:
                conn, _ = server.accept()
                threading.Thread(target=self._handle_message, args=(conn,)).start()
            except:
                break

    def _handle_message(self, conn):
        try:
            msg = recv_json(conn)
            if not msg: return
            
            msg_type = msg.get("type")
            sender_id = msg.get("sender_id")

            if msg_type == MSG_ELECTION:
                print(f"[Elección] Recibido desafío de Nodo {sender_id}")
                if sender_id < self.my_id:
                    send_json(conn, {"type": MSG_ELECTION_OK, "sender_id": self.my_id})
                    self.start_election() # Yo tomo el relevo
            
            elif msg_type == MSG_COORDINATOR:
                print(f"[Elección] ¡Nuevo Líder! Es el Nodo {sender_id}")
                self.election_in_progress = False
                self.on_new_master(sender_id)

        except Exception as e:
            print(f"[Elección Error] {e}")
        finally:
            conn.close()

    def start_election(self):
        if self.election_in_progress: return
        self.election_in_progress = True
        print("[Elección] --- INICIANDO ELECCIÓN (Bully) ---")

        higher_nodes = [n for n in self.config["nodes"] if n["id"] > self.my_id]
        
        if not higher_nodes:
            self._declare_victory()
            return

        anyone_answered = False
        for node in higher_nodes:
            try:
                # Puerto de elección = port_db + 100
                target_port = node["port_db"] + 100
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.0) # Timeout corto para detectar caídas rápido
                sock.connect((node["host"], target_port))
                
                send_json(sock, {"type": MSG_ELECTION, "sender_id": self.my_id})
                resp = recv_json(sock)
                
                if resp and resp.get("type") == MSG_ELECTION_OK:
                    print(f"[Elección] Nodo {node['id']} respondió. Me retiro.")
                    anyone_answered = True
                sock.close()
            except (ConnectionRefusedError, socket.timeout):
                pass 
        
        if not anyone_answered:
            self._declare_victory()

    def _declare_victory(self):
        print("[Elección] Ahora soy el nodo maestro")
        self.election_in_progress = False
        
        # 1. Avisar a todos los nodos (menores)
        lower_nodes = [n for n in self.config["nodes"] if n["id"] != self.my_id]
        for node in lower_nodes:
            try:
                target_port = node["port_db"] + 100
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(0.5)
                sock.connect((node["host"], target_port))
                send_json(sock, {"type": MSG_COORDINATOR, "sender_id": self.my_id})
                sock.close()
            except:
                pass
        
        # Ejecutar lógica de promoción local
        self.on_promotion()