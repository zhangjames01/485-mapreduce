from asyncio.log import logger
import socket
import json
import logging

"""Utils file.

This file is to house code common between the Manager and the Worker

"""
LOGGER = logging.getLogger(__name__)

def makeSocket(sock, host, port):
    
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port)) # <- hope this is right
        sock.listen()

# TCP send message
def sendMessage(port, host, dict):
    LOGGER.debug("sent message to port " + str(port) + "msg: " + str(dict))
    # create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

        # connect to the server
        sock.connect((host, port))

        # send a message
        message = json.dumps(dict)
        sock.sendall(message.encode('utf-8'))
