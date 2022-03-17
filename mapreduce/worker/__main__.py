"""MapReduce framework Worker node."""
import sys
import os
import logging
import json
import time
import click
import socket
import mapreduce.utils
import threading

# Configure logging
LOGGER = logging.getLogger(__name__)


class Worker:
    """A class representing a worker node in a MapReduce cluster."""
    def __init__(self, host, port, manager_host, manager_port,
                 manager_hb_port):
        """Construct a Worker instance and start listening for messages."""
        threads = []

        signals = {"shutdown" : False}

        LOGGER.info(
            "Starting worker host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )
        LOGGER.info(
            "manager_host=%s manager_port=%s manager_hb_port=%s",
            manager_host, manager_port, manager_hb_port,
        )
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            mapreduce.utils.makeSocket(sock, host, port)
            register_mess = {
                "message_type": "register",
                "worker_host": host,
                "worker_port": port,
            }
            mapreduce.utils.sendMessage(manager_port, manager_host, register_mess)
            LOGGER.debug("TCP recv\n%s", json.dumps(register_mess, indent=2))
            while not signals["shutdown"]:
            # Wait for a connection for 1s.  The socket library avoids consuming
            # CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])

                # Receive data, one chunk at a time.  If recv() times out before we can
                # read a chunk, then go back to the top of the loop and try again.
                # When the client closes the connection, recv() returns empty data,
                # which breaks out of the loop.  We make a simplifying assumption that
                # the client will always cleanly close the connection.
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(message_dict)
                if message_dict['message_type'] == "register_ack":
                    LOGGER.debug("recieved register_ack")
                    newthread = threading.Thread(target=sendHeartBeat, args=(signals, manager_host, manager_hb_port, host, port))
                    threads.append(newthread)
                    newthread.start()
                if message_dict['message_type'] == "shutdown":
                    signals["shutdown"] = True
                    LOGGER.debug("%d", threading.active_count())
                # send register message to 
                time.sleep(.1)
        
               
                

def sendHeartBeat(signals, manager_host, manager_hb_port, host, port, timer=2):
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect((manager_host, manager_hb_port))
        hb_message = {
                     "message_type": "heartbeat",
                     "worker_host": host,
                     "worker_port": port
                    }
        while not signals["shutdown"]:
            LOGGER.debug("sending heartbeat")
            message = json.dumps(hb_message)
            sock.sendall(message.encode('utf-8'))
            time.sleep(timer)

@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6001)
@click.option("--manager-host", "manager_host", default="localhost")
@click.option("--manager-port", "manager_port", default=6000)
@click.option("--manager-hb-port", "manager_hb_port", default=5999)
def main(host, port, manager_host, manager_port, manager_hb_port):
    """Run Worker."""
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        f"Worker:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    Worker(host, port, manager_host, manager_port, manager_hb_port)


if __name__ == '__main__':
    main()
