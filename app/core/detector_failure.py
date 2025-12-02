import threading
import time
import socket
import json

# CONSTANTES DE TIEMPO
INTERVALO_PING = 2      # Enviar 'estoy vivo' cada 2 seg
TIEMPO_LIMITE = 10      # Si en 10 seg no sé de ti, estás muerto
TIMEOUT_SOCKET = 1.0    # Tiempo máximo para esperar conexión

class DetectorFallas:
    def __init__(self, id_nodo, host, puerto, nodos_cluster, es_maestro, id_maestro, al_detectar_fallo):
        """
        :param nodos_cluster: Diccionario {'1': ('127.0.0.1', 8201), '2': ...}
        """
        self.id_nodo = str(id_nodo)
        self.host = host
        self.puerto = puerto
        self.nodos_cluster = nodos_cluster
        self.es_maestro = es_maestro
        self.id_maestro = str(id_maestro)
        self.callback_fallo = al_detectar_fallo
        
        self.running = False
        # Diccionario para guardar la última vez que vimos a cada nodo
        # Estructura: {'1': timestamp, '2': timestamp}
        self.ultima_vez_visto = {}
        
        # Inicializamos a todos como "vistos ahora" para darles tiempo de arrancar
        now = time.time()
        for nid in self.nodos_cluster:
            if nid != self.id_nodo:
                self.ultima_vez_visto[nid] = now

    def iniciar(self):
        self.running = True
        
        # 1. Hilo Servidor (Escuchar "ESTOY VIVO")
        t_listen = threading.Thread(target=self._listen_heartbeats, daemon=True)
        t_listen.start()
        
        # 2. Hilo Emisor (Enviar "ESTOY VIVO")
        t_send = threading.Thread(target=self._send_heartbeats, daemon=True)
        t_send.start()
        
        # 3. Hilo Monitor (Verificar quién murió)
        t_monitor = threading.Thread(target=self._check_timeouts, daemon=True)
        t_monitor.start()

    def detener(self):
        self.running = False

    def set_rol_maestro(self, es_maestro):
        self.es_maestro = es_maestro
    
    def set_target_maestro(self, nuevo_id):
        self.id_maestro = str(nuevo_id)
        # Reiniciamos el contador del nuevo maestro para no matarlo instantáneamente
        self.ultima_vez_visto[self.id_maestro] = time.time()

    # --- LÓGICA INTERNA ---

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
                            # Actualizamos su hora de vida
                            self.ultima_vez_visto[sender] = time.time()
                            # Opcional: Responder PONG (ACK)
                    conn.close()
                except:
                    pass
        except Exception as e:
            print(f"[Detector Error] Bind falló: {e}")

    def _send_heartbeats(self):
        """Envía PING a todos los vecinos relevantes."""
        while self.running:
            # Estrategia: "Todos contra Todos" (Simple para Bully)
            # Opcional: Si quieres reducir tráfico, solo ping al Maestro si eres esclavo.
            
            for nid, (ip, port) in self.nodos_cluster.items():
                if nid == self.id_nodo: continue # No me pingeo a mí mismo
                
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
            # print(f"   -> Ping a Nodo {target_id} OK") 
        except:
            # Si falla la conexión, NO actualizamos su timestamp.
            # El monitor detectará el timeout pronto.
            pass

    def _check_timeouts(self):
        """Revisa periódicamente si alguien expiró."""
        while self.running:
            time.sleep(1)
            now = time.time()
            
            # Hacemos una copia de las claves para poder modificar el dict si hace falta
            nodos_ids = list(self.ultima_vez_visto.keys())
            
            for nid in nodos_ids:
                last_seen = self.ultima_vez_visto[nid]
                delta = now - last_seen
                
                if delta > TIEMPO_LIMITE:
                    print(f"[Detector] Nodo {nid} ha muerto (Silencio por {int(delta)}s)")
                    
                    # Reseteamos el timer para no disparar la alerta infinitamente
                    # (o lo sacamos de la lista)
                    self.ultima_vez_visto[nid] = time.time() 
                    
                    # Avisar al Main
                    if self.callback_fallo:
                        self.callback_fallo(nid)