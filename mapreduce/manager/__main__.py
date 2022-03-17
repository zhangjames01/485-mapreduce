"""MapReduce framework Manager node."""
import sys
import os
import logging
import json
import time
import click
import mapreduce.utils
from pathlib import Path
import threading
import socket
import json


# Configure logging
LOGGER = logging.getLogger(__name__)


class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port, hb_port):
        """Construct a Manager instance and start listening for messages."""
        # create new folder temp. if temp already exists, keep it
        tempDir = Path().resolve()
        tempDir = tempDir / "tmp"
        # delete any old mapreduce job folders in temp
        jobFiles = tempDir.glob("job-*")
        for job in jobFiles:
            os.remove(job)

        #create a new thread, which will listen to udp heartbeat messages from the workers
        manag_thread = threading.Thread(target=worker, args=())
        faut_tol = threading.Thread(target=worker, args=())
        LOGGER.info(
            "Starting manager host=%s port=%s hb_port=%s pwd=%s",
            host, port, hb_port, os.getcwd(),
        )

        # create a new tcp socket on given port and call the listen function
        # wait for incoming messages, ignore invalid messages (incl those that fail json endecoding)
        mapreduce.utils.makeSocket(port)

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # TODO: you should remove this. This is just so the program doesn't
        # exit immediately!
        LOGGER.debug("IMPLEMENT ME!")
        time.sleep(120)

        # TODO: exit all threads before moving on

# worker function from example_thread.py
def worker(worker_id):
    """Worker thread."""
    pid = os.getpid()
    print("Worker {}  pid={}".format(worker_id, pid))
    while True:
        # spin
        pass



@click.command()
@click.option("--host", "host", default="localhost") # host address to listen for addresses
@click.option("--port", "port", default=6000) # tcp port to listen for messages
@click.option("--hb-port", "hb_port", default=5999) #udp port to listen for heartbeats
def main(host, port, hb_port):
    """Run Manager."""
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter(
    f"Manager:{port} [%(levelname)s] %(message)s"
)
handler.setFormatter(formatter)
root_logger = logging.getLogger()
root_logger.addHandler(handler)
root_logger.setLevel(logging.INFO)
Manager(host, port, hb_port)


if __name__ == '__main__':
    main()
