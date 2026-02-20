import socket
import struct

def send_message(sock , command , text):
    payload = text.encode('utf-8')
    header = struct.pack("!IB" , len(payload) , command)
    sock.sendall(header + payload)