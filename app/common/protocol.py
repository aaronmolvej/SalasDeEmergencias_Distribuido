import json
import struct
import socket

# Encabezado de 4 bytes para indicar el tamaño del mensaje
HEADER_LENGTH = 4

def send_json(sock, data):
    try:
        # Convertir dict a bytes
        json_bytes = json.dumps(data).encode('utf-8')
        
        # Crear encabezado (Entero de 4 bytes, Big Endian)
        header = struct.pack('>I', len(json_bytes))
        
        # Enviar todo junto
        sock.sendall(header + json_bytes)
    except Exception as e:
        print(f"[Protocol Error] Fallo al enviar: {e}")
        raise

def recv_json(sock):
    try:
        # Leer el encabezado para saber cuánto medir el mensaje
        header_data = _recv_all(sock, HEADER_LENGTH)
        if not header_data:
            return None
        
        # Desempaquetar el tamaño
        msg_length = struct.unpack('>I', header_data)[0]
        
        # Leer el cuerpo del mensaje
        msg_data = _recv_all(sock, msg_length)
        if not msg_data:
            return None
            
        return json.loads(msg_data.decode('utf-8'))
    except Exception as e:
        print(f"[Protocol Error] Fallo al recibir: {e}")
        return None

#Función auxiliar para asegurar lectura completa de n bytes
def _recv_all(sock, n):
    data = b''
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data += packet
    return data