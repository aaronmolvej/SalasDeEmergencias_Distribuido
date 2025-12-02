import threading
import time
import socket
import json

# CONSTANTES DE TIEMPO
INTERVALO_PING = 2  
TIEMPO_LIMITE = 10     
TIMEOUT_SOCKET = 1.0  

class DetectorFallas:
    def __init__(self, id_nodo, host, puerto, nodos_cluster, es_maestro, id_maestro, al_detectar_fallo):
        self.id_nodo = str(id_nodo)
        self.host = host
        self.puerto = puerto
        self.nodos_cluster = nodos_cluster
        self.es_maestro = es_maestro
        self.id_maestro = str(id_maestro)
        self.callback_fallo = al_detectar_fallo
        
        self.running = False
        self.ultima_vez_visto = {}
        
        # Inicializamos a todos como "vistos ahora" para darles tiempo de arrancar
        now = time.time()
        for nid in self.nodos_cluster:
            if nid != self.id_nodo:
                self.ultima_vez_visto[nid] = now

    def iniciar(self):
        self.running = True
        
        # Hilo Servidor
        t_listen = threading.Thread(target=self._listen_heartbeats, daemon=True)
        t_listen.start()
        
        # Hilo Emisor
        t_send = threading.Thread(target=self._send_heartbeats, daemon=True)
        t_send.start()
        
        # Hilo Monitor 
        t_monitor = threading.Thread(target=self._check_timeouts, daemon=True)
        t_monitor.start()

    def detener(self):
        self.running = False

    def set_rol_maestro(self, es_maestro):
        self.es_maestro = es_maestro
    
    def set_target_maestro(self, nuevo_id):
        self.id_maestro = str(nuevo_id)
        self.ultima_vez_visto[self.id_maestro] = time.time()

    # LÓGICA INTERNA 

    def _listen_heartbeats(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            server.bind(("0.0.0.0", self.puerto))
            server.listen(5)
            # print(f"[Detector] Escuchando latidos en puerto {self.puerto}...")
            
            while self.running:
                try:
                    conn, addr = server.accept()
                    data = conn.recv(1024)
                    if data:
                        msg = json.loads(data.decode())
                        if msg['type'] == 'PING':
                            sender = str(msg['sender_id'])
                            self.ultima_vez_visto[sender] = time.time()
                    conn.close()
                except:
                    pass
        except Exception as e:
            print(f"[Detector Error] Bind falló: {e}")

    def _send_heartbeats(self):
        """Envía PING a todos los vecinos relevantes."""
        while self.running:
            
            for nid, (ip, port) in self.nodos_cluster.items():
                if nid == self.id_nodo: continue 
                
                self._send_ping(nid, ip, port)
            
            time.sleep(INTERVALO_PING)

    def _send_ping(self, target_id, ip, port):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(TIMEOUT_SOCKET)
            s.connect((ip, port))
            msg = {"type": "PING", "sender_id": self.id_nodo}
            s.sendall(json.dumps(msg).encode())
            s.close()
        except:
            pass

    def _check_timeouts(self):
        """Revisa periódicamente si alguien expiró."""
        while self.running:
            time.sleep(1)
            now = time.time()
            
            nodos_ids = list(self.ultima_vez_visto.keys())
            
            for nid in nodos_ids:
                last_seen = self.ultima_vez_visto[nid]
                delta = now - last_seen
                
                if delta > TIEMPO_LIMITE:
                    print(f"[Detector] Nodo {nid} ha muerto (Silencio por {int(delta)}s)")
                    
                    self.ultima_vez_visto[nid] = time.time() 
                    
                    # Avisar al Main
                    if self.callback_fallo:
                        self.callback_fallo(nid)