from asyncio.log import logger
import socket
import json
import logging
import pathlib

"""Utils file.

This file is to house code common between the Manager and the Worker

"""
LOGGER = logging.getLogger(__name__)

def makeSocket(sock, host, port):
    
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port)) # <- hope this is right
        sock.listen()

# TCP send message
def sendMessage(port, host, mess):
    LOGGER.debug("sent message to port " + str(port) + "msg: " + str(mess))
    # create an INET, STREAMing socket, this is TCP
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:

        # connect to the server
        sock.connect((host, port))

        # send a message
        message = json.dumps(mess)
        sock.sendall(message.encode('utf-8'))

class PathJSONEncoder(json.JSONEncoder):
    """Extended the Python JSON encoder to encode Pathlib objects.
    Docs: https://docs.python.org/3/library/json.html
    Usage:
    >>> json.dumps({
            "executable": TESTDATA_DIR/"exec/wc_map.sh",
        }, cls=PathJSONEncoder)
    """
    def default(self, o):
        """Override base class method to include Path object serialization."""
        if isinstance(o, pathlib.Path):
            return str(o)
        return super().default(o)
