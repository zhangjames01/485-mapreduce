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
import shutil

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
        reduce_outputs = []
        num_tasks = []
        completed_tasks = 0
        job_counter = 0
        working = False
        stage = ""
        signals = {"shutdown": False}
        tempDir = Path().resolve() / "tmp"
        # delete any old mapreduce job folders in temp
        jobFiles = tempDir.glob("job-*")
        for job in jobFiles:
            shutil.rmtree(job)

        #create a new thread, which will listen to udp heartbeat messages from the workers
        manag_thread = threading.Thread(target=heartbeatListener, args=(signals, host, hb_port, workers))
        threads.append(manag_thread)
        manag_thread.start()
        check_thread = threading.Thread(target=check_heartbeats, args=(signals, workers))
        threads.append(check_thread)
        check_thread.start()
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
                    # if len(workers) == 1:
                    #     if not working:
                    #         working = True
                if message_dict['message_type'] == "new_manager_job":
                   
                    currJobPath = Path("tmp")/ ("job-"+str(job_counter))/"intermediate"
                    currJobPath.mkdir(parents=True)
                    job_counter += 1
                    currJob = {
                                "input_directory": message_dict['input_directory'],
                                "output_directory_map": currJobPath,
                                "output_directory_reduce": message_dict['output_directory'],
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
                                reduce_outputs = []
                                num_tasks = []
                                #partitionedFiles = doPartition(jobs)
                                jobThread = threading.Thread(target=doJob,args=(jobs, workers, signals, port, stage, num_tasks))
                                threads.append(jobThread)
                                jobThread.start()
                                stage = "mapping"
                if message_dict['message_type'] == "finished":
                    if stage == "mapping":
                        for worker in workers:
                            if worker['port'] == message_dict['worker_port'] and worker['host'] == message_dict['worker_host']:
                                worker['status'] = "ready"
                        for output in message_dict['output_paths']:
                            map_outputs.append(output)
                        
                        completed_tasks += 1
                        print("tasks needed: " + str(len(num_tasks)) + "tasks done: " + str(completed_tasks))
                        if completed_tasks == len(num_tasks):
                            LOGGER.info("Manager:%s end map stage", port)
                            stage = "reducing"
                            num_tasks = []
                            completed_tasks = 0
                            LOGGER.info("Manager:%s begin reduce stage", port)
                            reduceThread = threading.Thread(target=doReduce, args=(jobs, workers, signals, port, map_outputs, num_tasks))
                            threads.append(reduceThread)
                            reduceThread.start()
                    elif stage == "reducing":
                        for worker in workers:
                            if worker['port'] == message_dict['worker_port'] and worker['host'] == message_dict['worker_host']:
                                worker['status'] = "ready"
                        reduce_outputs.append(message_dict['output_paths'])
                        completed_tasks += 1
                        if completed_tasks == len(num_tasks):
                            LOGGER.info("Manager:%s end reducing stage", port)
                            stage = "reducing"
                            num_tasks = []
                            completed_tasks = 0
                            working = False
                            jobs.pop(0)

                if len(jobs) > 0:
                    for worker in workers:
                        if worker['status'] == "ready":
                            if not working:
                                working = True
                                map_outputs = []
                                reduce_outputs = []
                                num_tasks = []
                                #partitionedFiles = doPartition(jobs)
                                jobThread = threading.Thread(target=doJob,args=(jobs, workers, signals, port, stage, num_tasks))
                                threads.append(jobThread)
                                jobThread.start()
                                stage = "mapping"
                                break
                if message_dict['message_type'] == "shutdown":
                    for worker in workers:
                        if worker['status'] != "dead":
                            mapreduce.utils.sendMessage(worker['port'], worker['host'], {"message_type" : "shutdown"})
                    signals["shutdown"] = True
                LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))
                time.sleep(.1)

        # TODO: exit all threads before moving on

def doReduce(jobs, workers, signals, port, map_outputs, num_tasks):
    #make array for tasks
    partitionedFiles = []
    currJob = jobs[0]
    #sort output from map
    map_outputs.sort()
    #make an array inside each task to hold a list of files
    for i in range(currJob['num_reducers']):
        partitionedFiles.append([])
    #for each file in map_outputs, put it in the correct task's list
    i = 0
    for task in map_outputs:
        partitionedFiles[i].append({'task': task,
                                 'taskid': i})
        if i not in num_tasks:
            num_tasks.append(i)
        i += 1
        i = i % currJob['num_reducers']
    #send tasks to workers when ready
    while not signals['shutdown'] and len(partitionedFiles) > 0:
        for i in range(len(partitionedFiles)):
            for worker in workers:
                if worker['status'] == "ready":
                    # make a list of easily readible files from the old list
                    input_paths = []
                    for path in partitionedFiles[i]:
                        input_paths.append(path['task'])
                    message_dict = {
                                    "message_type": "new_reduce_task",
                                    "task_id": partitionedFiles[i][0]['taskid'],
                                    "executable": currJob['reducer_executable'],
                                    "input_paths": input_paths,
                                    "output_directory": str(currJob['output_directory_reduce']),
                                    "worker_host": worker['host'],
                                    "worker_port": worker['port']
                                    }
                    mapreduce.utils.sendMessage(worker['port'], worker['host'], message_dict)
                    worker['status'] = "busy"
                    partitionedFiles.pop(i)
        time.sleep(.1)

def doPartition(jobs):
    pass

def doJob(jobs, workers, signals, port, stage, num_tasks):
    currJob = jobs[0]
    #print("num of partitions: " + str(currJob['num_mappers']))
    tasks = []
    for file in Path(currJob['input_directory']).glob("*"):
        tasks.append(os.path.basename(file))
    tasks.sort()
    partitionedFiles = []
    for i in range(currJob['num_mappers']):
        partitionedFiles.append([])
    i = 0
    for task in tasks:
        partitionedFiles[i].append({'task': str(task),
                                 'taskid': i})
        if i not in num_tasks:
            num_tasks.append(i)
        i += 1
        i = i % currJob['num_mappers']
    while not signals['shutdown'] and len(partitionedFiles) > 0:
        i = 0
        while i < len(partitionedFiles):
            print("i: " + str(i))
            for worker in workers:
                if worker['status'] == "ready":
                    input_paths = []
                    for path in partitionedFiles[i]:
                        input_paths.append(currJob['input_directory']+"/"+path['task'])
                    message_dict = {
                                    "message_type": "new_map_task",
                                    "task_id": partitionedFiles[i][0]['taskid'],
                                    "input_paths": input_paths,
                                    "executable": currJob['mapper_executable'],
                                    "output_directory": str(currJob['output_directory_map']),
                                    "num_partitions": currJob['num_reducers'],
                                    "worker_host": worker['host'],
                                    "worker_port": worker['port']
                                    }
                    worker['message'] = message_dict
                    mapreduce.utils.sendMessage(worker['port'], worker['host'], message_dict)
                    worker['status'] = "busy"
                    print("pop index: " + str(i))
                    partitionedFiles.pop(i)
                    i -= 1
                    break
            i += 1
        time.sleep(.1)
    LOGGER.info("Manager:%s begin map stage", port)
    
def send_replacementTask(signals, message, workers):
    while not signals['shutdown']:
        for worker in workers:
            if worker['status'] == "ready":
                message['worker_port'] = worker['port']
                worker['message'] = message
                mapreduce.utils.sendMessage(worker['port'], worker['host'], message)
                worker['status'] = "busy"
                return
        time.sleep(.1)
            
def check_heartbeats(signals, workers):
    while not signals["shutdown"]:
        for worker in workers:
            if worker['missedHB'] >= 5:
                if worker['status'] != "dead":
                    LOGGER.debug("worker: " + str(worker['port']) + "is dead")
                    if worker['status'] == "busy":
                        worker['status'] = "dead"
                        newTaskThread = threading.Thread(target=send_replacementTask, args=(signals,worker['message'],workers))
                        newTaskThread.start()
                    worker['status'] = "dead"

            worker['missedHB'] += 1
        time.sleep(2)

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
