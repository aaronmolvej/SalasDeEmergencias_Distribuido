# Detector de fallas y algoritmo Dijkstra–Scholten para redistribución coordinada.

import threading
import time
import socket
import json
import logging
from typing import Dict, Callable, Optional, Set, Tuple


logging.basicConfig(level=logging.INFO)
bitacora = logging.getLogger("detector_fallas")

# Parámetros ajustables

INTERVALO_HEARTBEAT = 5.0         # Cada cuánto un nodo esclavo envía heartbeat
TIEMPO_FALLO = 15.0               # Tiempo sin heartbeat para marcar un nodo como caído
INTERVALO_REVISION = 2.0          # Frecuencia de revisión del nodo maestro
TIMEOUT_SOCKET = 3.0              # Timeout de sockets

# Tipos de mensajes

TIPO_HEARTBEAT = "HEARTBEAT"
TIPO_HEARTBEAT_RESP = "HEARTBEAT_RESP"

TIPO_DS_TAREA = "DS_TAREA"
TIPO_DS_TAREA_LISTO = "DS_TAREA_LISTO"
TIPO_DS_RESP = "DS_RESP"


# Función auxiliar para enviar JSON por socket

def enviar_json(ip: str, puerto: int, contenido: dict, timeout: float = TIMEOUT_SOCKET):
    """
    Envía un mensaje JSON por TCP.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            s.connect((ip, puerto))

            datos = json.dumps(contenido).encode("utf-8")
            s.sendall(len(datos).to_bytes(8, "big") + datos)

            # Leer respuesta (si existe)
            encabezado = s.recv(8)
            if not encabezado:
                return None

            longitud = int.from_bytes(encabezado, "big")
            cuerpo = b""
            while len(cuerpo) < longitud:
                parte = s.recv(min(4096, longitud - len(cuerpo)))
                if not parte:
                    break
                cuerpo += parte

            if cuerpo:
                return json.loads(cuerpo.decode("utf-8"))
            return None

    except Exception:
        return None


# Algoritmo de Dijkstra–Scholten

class AdministradorDS:
    """
    - Implementa el árbol de dependencia
    - Padre por operación
    - Hijos
    - Eventos de finalización
    - Ejecución de tareas
    - Maneja la terminación distribuida.
    """

    def __init__(self, detector):
        self.detector = detector
        self.padre: Dict[str, Optional[str]] = {}
        self.hijos: Dict[str, Set[str]] = {}
        self.eventos: Dict[str, threading.Event] = {}
        self.bloqueo = threading.Lock()

        # manejador de tarea que define la lógica a ejecutar
        self.manejador_tareas = None


    # Nodo maestro

    def iniciar_operacion(self, id_operacion: str):
        with self.bloqueo:
            self.padre[id_operacion] = None
            self.hijos[id_operacion] = set()
            self.eventos[id_operacion] = threading.Event()
        bitacora.info(f"[DS] Operación iniciada: {id_operacion}")

    def agregar_hijo(self, id_operacion: str, nodo: str):
        with self.bloqueo:
            self.hijos[id_operacion].add(nodo)

    def esperar_finalizacion(self, id_operacion: str, timeout=None):
        return self.eventos[id_operacion].wait(timeout=timeout)


    # Nodos esclavos

    def registrar_padre(self, id_operacion: str, padre: Optional[str]):
        with self.bloqueo:
            self.padre[id_operacion] = padre
            self.hijos.setdefault(id_operacion, set())
            self.eventos.setdefault(id_operacion, threading.Event())

    def registrar_manejador_tareas(self, funcion):
        """
        La función debe tener firma registrada:
        funcion(id_operacion, tarea, callback_finalizacion)
        """
        self.manejador_tareas = funcion

    def manejar_tarea_asincrona(self, id_operacion: str, tarea: dict, nodo_origen: str):
        """
        Ejecuta la tarea en un hilo y cuando termina notifica al algoritmo DS.
        """

        def ejecutar():
            try:
                if self.manejador_tareas:
                    callback = lambda: self._tarea_local_lista(id_operacion)
                    self.manejador_tareas(id_operacion, tarea, callback)
                else:
                    time.sleep(0.1)
                    self._tarea_local_lista(id_operacion)
            except:
                self._tarea_local_lista(id_operacion)

        threading.Thread(target=ejecutar, daemon=True).start()


    # Notificaciones de completado

    def _tarea_local_lista(self, id_operacion: str):
        """
        Nodo terminó su tarea.
        """
        padre = self.padre.get(id_operacion)
        if padre:
            # Informar al padre
            self.detector.informar_tarea_lista(id_operacion, self.detector.id_nodo)
        else:
            # Es nodo maestro
            self.hijo_lista(id_operacion, self.detector.id_nodo)

    def hijo_lista(self, id_operacion: str, nodo: str):
        """
        Se marca que un nodo hijo terminó.
        """
        with self.bloqueo:
            hijos = self.hijos.get(id_operacion, set())
            if nodo in hijos:
                hijos.remove(nodo)

            if len(hijos) == 0:
                padre = self.padre.get(id_operacion)
                if padre:
                    # avisar al padre
                    self.detector.informar_tarea_lista(id_operacion, self.detector.id_nodo)
                else:
                    # soy la raíz → toda la operación terminó
                    self.eventos[id_operacion].set()
                    bitacora.info(f"Operación completada: {id_operacion}")


# Detector de fallas

class DetectorFallas:
    """
    - Heartbeat
    - Detección de nodos muertos
    - Notificar a nodo su maestro
    - Soporte para Dijkstra–Scholten
    """

    def __init__(
        self,
        id_nodo: str,
        host: str,
        puerto: int,
        nodos_cluster: Dict[str, Tuple[str, int]],
        es_maestro: bool = False,
        id_maestro: Optional[str] = None,
        al_detectar_fallo: Optional[Callable[[str], None]] = None
    ):
        self.id_nodo = id_nodo
        self.host = host
        self.puerto = puerto

        # Diccionario: nodo -> (ip, puerto)
        self.nodos = dict(nodos_cluster)

        self.es_maestro = es_maestro
        self.id_maestro = id_maestro

        self.al_detectar_fallo = al_detectar_fallo or (lambda nodo: None)

        self.ultimo_heartbeat: Dict[str, float] = {nid: 0.0 for nid in self.nodos}
        self.nodos_fallados: Set[str] = set()

        self.bloqueo = threading.Lock()
        self.detener_evento = threading.Event()

        # Gestor Dijkstra–Scholten
        self.ds = AdministradorDS(self)

        # Hilos
        self.hilo_servidor = threading.Thread(target=self._ciclo_servidor, daemon=True)
        self.hilo_envio = None
        self.hilo_revision = None

    # Inicio/Detención del detector

    def iniciar(self):
        self.detener_evento.clear()
        self.hilo_servidor.start()

        if self.es_maestro:
            # hilo que revisa heartbeats
            self.hilo_revision = threading.Thread(target=self._ciclo_revision, daemon=True)
            self.hilo_revision.start()
        else:
            # hilo que envía heartbeats al nodo maestro
            self.hilo_envio = threading.Thread(target=self._ciclo_envio_heartbeat, daemon=True)
            self.hilo_envio.start()

    def detener(self):
        self.detener_evento.set()


    # Servidor TCP que recibe heartbeats y mensajes Dijkstra–Scholten

    def _ciclo_servidor(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind((self.host, self.puerto))
            srv.listen(15)
            srv.settimeout(1)

            bitacora.info(f"[{self.id_nodo}] Servidor activo en {self.host}:{self.puerto}")

            while not self.detener_evento.is_set():
                try:
                    conn, addr = srv.accept()
                    threading.Thread(target=self._manejar_conexion, args=(conn,), daemon=True).start()
                except socket.timeout:
                    pass

    def _manejar_conexion(self, conn: socket.socket):
        try:
            with conn:
                enc = conn.recv(8)
                if not enc:
                    return

                longitud = int.from_bytes(enc, "big")
                cuerpo = b""
                while len(cuerpo) < longitud:
                    parte = conn.recv(min(4096, longitud - len(cuerpo)))
                    if not parte:
                        break
                    cuerpo += parte

                msg = json.loads(cuerpo.decode("utf-8"))
                tipo = msg.get("tipo")
                desde = msg.get("desde")
                # HEARTBEAT
                if tipo == TIPO_HEARTBEAT:
                    with self.bloqueo:
                        self.ultimo_heartbeat[desde] = time.time()
                        if desde in self.nodos_fallados:
                            self.nodos_fallados.remove(desde)
                    self._responder(conn, {"tipo": TIPO_HEARTBEAT_RESP})
                    return

                # TAREA DE DIJKSTRA–SHOLTEN

                if tipo == TIPO_DS_TAREA:
                    id_op = msg.get("id_operacion")
                    tarea = msg.get("tarea")
                    padre = msg.get("padre")

                    self.ds.registrar_padre(id_op, padre)
                    self.ds.manejar_tarea_asincrona(id_op, tarea, desde)

                    self._responder(conn, {"tipo": TIPO_DS_RESP})
                    return

                # CONFIRMACIÓN DE TAREA TERMINADA EN HIJO

                if tipo == TIPO_DS_TAREA_LISTO:
                    id_op = msg.get("id_operacion")
                    self.ds.hijo_lista(id_op, desde)
                    self._responder(conn, {"tipo": "OK"})
                    return

                self._responder(conn, {"tipo": "NO_ENTENDIDO"})

        except Exception:
            pass

    def _responder(self, conn: socket.socket, respuesta: dict):
        datos = json.dumps(respuesta).encode("utf-8")
        conn.sendall(len(datos).to_bytes(8, "big") + datos)

    # Ciclo de envío de heartbeat (solo nodos que no son el nodo maestro)

    def _ciclo_envio_heartbeat(self):
        if not self.id_maestro:
            return
        ip, pr = self.nodos[self.id_maestro]
        while not self.detener_evento.is_set():
            enviar_json(ip, pr, {"tipo": TIPO_HEARTBEAT, "desde": self.id_nodo})
            time.sleep(INTERVALO_HEARTBEAT)

    # Nodo maestro revisa tiempos de heartbeat

    def _ciclo_revision(self):
        while not self.detener_evento.is_set():
            ahora = time.time()
            nuevos_caidos = []

            with self.bloqueo:
                for nodo, t in self.ultimo_heartbeat.items():
                    if nodo == self.id_nodo:
                        continue

                    if t != 0 and (ahora - t) > TIEMPO_FALLO and nodo not in self.nodos_fallados:
                        self.nodos_fallados.add(nodo)
                        nuevos_caidos.append(nodo)

            for fallado in nuevos_caidos:
                bitacora.warning(f"Nodo caído detectado: {fallado}")
                self.al_detectar_fallo(fallado)

            time.sleep(INTERVALO_REVISION)

    #Dijkstra–Scholten

    def iniciar_operacion_ds(self, id_operacion: str):
        self.ds.iniciar_operacion(id_operacion)

    def enviar_tarea_ds(self, id_operacion: str, nodo_destino: str, tarea: dict):
        if nodo_destino not in self.nodos:
            return False

        ip, pr = self.nodos[nodo_destino]

        contenido = {
            "tipo": TIPO_DS_TAREA,
            "id_operacion": id_operacion,
            "tarea": tarea,
            "padre": self.id_nodo,
            "desde": self.id_nodo
        }

        resp = enviar_json(ip, pr, contenido)
        if resp is not None:
            self.ds.agregar_hijo(id_operacion, nodo_destino)
            return True
        return False

    def informar_tarea_lista(self, id_operacion: str, nodo: str):
        padre = self.ds.padre.get(id_operacion)

        if not padre:
            # soy nodo raíz
            self.ds.hijo_lista(id_operacion, nodo)
            return

        if padre not in self.nodos:
            # si el padre no existe entonces tratar como completado localmente
            self.ds.hijo_lista(id_operacion, nodo)
            return

        ip, pr = self.nodos[padre]

        contenido = {
            "tipo": TIPO_DS_TAREA_LISTO,
            "id_operacion": id_operacion,
            "desde": nodo
        }

        enviar_json(ip, pr, contenido)

    def esperar_fin_ds(self, id_operacion: str, timeout=None):
        return self.ds.esperar_finalizacion(id_operacion, timeout)
