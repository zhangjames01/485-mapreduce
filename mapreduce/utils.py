import socket
import json

"""Utils file.

This file is to house code common between the Manager and the Worker

"""
def makeSocket(sock, host, port):
    
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port)) # <- hope this is right
        sock.listen()

def sendMessage(port, host, dict):
    # create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

        # connect to the server
        sock.connect((host, port))

        # send a message
        message = json.dumps(dict)
        sock.sendall(message.encode('utf-8'))