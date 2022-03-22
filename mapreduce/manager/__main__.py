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
        threads = []
        workers = []
        jobs = []
        map_outputs = []
        num_tasks = 0
        completed_tasks = 0
        job_counter = 0
        working = False
        stage = ""
        signals = {"shutdown": False}
        tempDir = Path().resolve()
        tempDir = tempDir / "tmp"
        # delete any old mapreduce job folders in temp
        jobFiles = tempDir.glob("job-*")
        for job in jobFiles:
            os.remove(job)

        #create a new thread, which will listen to udp heartbeat messages from the workers
        manag_thread = threading.Thread(target=heartbeatListener, args=(signals, host, hb_port, workers))
        threads.append(manag_thread)
        manag_thread.start()
        #faut_tol = threading.Thread(target=worker, args=())
        LOGGER.info(
            "Starting manager host=%s port=%s hb_port=%s pwd=%s",
            host, port, hb_port, os.getcwd(),
        )

        # create a new tcp socket on given port and call the listen function
        # wait for incoming messages, ignore invalid messages (incl those that fail json endecoding)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            mapreduce.utils.makeSocket(sock, host, port)
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

                # This is a fake message to demonstrate pretty printing with logging
                if message_dict['message_type'] == "register":
                    tempWorker = {'host' : message_dict['worker_host'],
                                  'port' : message_dict['worker_port'],
                                  'status' : 'ready',
                                  'missedHB' : 0
                                 }
                    workers.append(tempWorker)
                    register_mess = {
                        "message_type": "register_ack",
                        "worker_host": tempWorker['host'],
                        "worker_port": tempWorker['port'],
                    }
                    mapreduce.utils.sendMessage(tempWorker['port'], tempWorker['host'], register_mess)
                    if len(workers) == 1:
                        if not working:
                            working = True
                if message_dict['message_type'] == "new_manager_job":
                    currJobPath = tempDir / ("job-"+str(job_counter))/"intermediate"
                    currJobPath.mkdir(parents=True)
                    job_counter += 1
                    currJob = {
                                "input_directory": message_dict['input_directory'],
                                "output_directory": message_dict['output_directory'],
                                "mapper_executable": message_dict['mapper_executable'],
                                "reducer_executable": message_dict['reducer_executable'],
                                "num_mappers" : message_dict['num_mappers'],
                                "num_reducers" : message_dict['num_reducers'],
                                }
                    jobs.append(currJob)
                    for worker in workers:
                        if worker['status'] == "ready":
                            if not working:
                                working = True
                                map_outputs = []
                                num_tasks = 0
                                jobThread = threading.Thread(target=doJob,args=(jobs, workers, signals, port))
                                threads.append(jobThread)
                                jobThread.start()
                if message_dict['message_type'] == "finished":
                    if stage == "mapping":
                        for worker in workers:
                            if worker['port'] == message_dict['worker_port'] and worker['host'] == message_dict['worker_host']:
                                worker['status'] = "ready"
                        map_outputs.appends(message_dict['output_paths'])
                        completed_tasks += 1
                        if completed_tasks == num_tasks:
                            LOGGER.info("Manager:%s end map stage", port)
                            stage = "reducing"
                            num_tasks = 0
                            completed_tasks = 0
                            #reduceThread = threading.thread
                if message_dict['message_type'] == "shutdown":
                    for worker in workers:
                        mapreduce.utils.sendMessage(worker['port'], worker['host'], {"message_type" : "shutdown"})
                    signals["shutdown"] = True
                LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

                time.sleep(.1)

        # TODO: exit all threads before moving on

def doJob(jobs, workers, signals, port):
    # stage = "mapping"
    currJob = jobs[0]
    currJob['input_directory'].sort()
    partitionedFiles = [[{}]]
    i = 0
    for task in currJob['input_directory']:
        partitionedFiles[i].append({'task' : task,
                                    'taskid': i})
        i += 1
        i % currJob['num_mappers']
    num_tasks = len(partitionedFiles)
    while not signals['shutdown'] and len(partitionedFiles > 0):
        for i in range(len(partitionedFiles)):
            for worker in workers:
                if worker['status'] == "ready":
                    message_dict = {
                                    "message_type": "new_map_task",
                                    "task_id": partitionedFiles[i]['taskid'],
                                    "input_paths": partitionedFiles[i]['task'],
                                    "executable": currJob['mapper_executable'],
                                    "output_directory": currJob['output_directory'],
                                    "num_partitions": currJob['num_reducers'],
                                    "worker_host": worker['host'],
                                    "worker_port": worker['port']
                                    }
                    mapreduce.utils.sendMessage(worker['port'], worker['host'], message_dict)
                    worker['status'] = "busy"
                    partitionedFiles.pop(i)
        time.sleep(.1)
    LOGGER.info("Manager:%s begin map stage", port)
    stage = "mapping"
def heartbeatListener(signals, host, hb_port, workers):
       # Create an INET, DGRAM socket, this is UDP
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        # Bind the UDP socket to the server 
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, hb_port))
        sock.settimeout(1)
        # No sock.listen() since UDP doesn't establish connections like TCP
        # Receive incoming UDP messages
        while not signals["shutdown"]:
            for worker in workers:
                if worker['missedHB'] >= 5:
                    worker['status'] = "dead"
                worker['missedHB'] += 1
                time.sleep(2)
        while not signals["shutdown"]:
            try:
                message_bytes = sock.recv(4096)
            except socket.timeout:
                continue
            message_str = message_bytes.decode("utf-8")
            message_dict = json.loads(message_str)
            LOGGER.debug("%s",message_dict)
            if message_dict["message_type"] == "heartbeat":
                LOGGER.debug("recieving heartbeat")
                for worker in workers:
                    if worker['port'] == message_dict['worker_port']:
                        worker['missedHB'] = 0
            time.sleep(.1)

    
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
