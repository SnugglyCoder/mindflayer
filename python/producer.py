import socket
import random
import sys
import signal
import threading
import time
from threading import Lock

def run_producer():
    print("Listening on port: " + str(LISTEN_PORT))
    ## CREATE CONNECTION TO MASTER
    # producer socket
    topicLabel = "random"
    consumerGroups = [] ## list of dicts ("Group Label": [consumer1, consumer2, ...])
    producer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## send MASTER the port we are listening on
    producer.connect((MASTER_IP, MASTER_PORT))
    msg = "producer\n" + str(LISTEN_PORT) + "\n" + topicLabel ##Message to send to MASTER 
    producer.send(msg.encode())
    master_response = producer.recv(1024)
    print("Master: " + master_response.decode())
    producer.close()
    
    producer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Listen on Port for consumer
    producer.bind((socket.gethostname(),LISTEN_PORT))
    print("    Waiting for consumers...")
    
    ## Daemon to send data to consumers
    newThread = threading.Thread(target=sendToGroups, args=())
    newThread.daemon = True
    newThread.start()
    
    producer.listen(5)

    while True:
        consumer, addr = producer.accept()
        newThread = threading.Thread(target=acceptConsumer, args=(consumer,))
        newThread.daemon = True
        newThread.start()
## gets free port to listen on
def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port

def sendToGroups():
    mutex = Lock()
    current = [] ## track current consumer of each group
    while True:
        if len(consumerGroups) > 0:
            print("\n")
        ## generate date
        msg = str(random.randint(0,100))
        
        # Init indexes of current consumer
        for group in consumerGroups:
            current.append(0)

        index = 0 ## index of current group in consumerGroups
        
        for group in consumerGroups:
            # send msg to consumers
            mutex.acquire()
            print("sending " + msg + " to consumer " + str(current[index]) + " in group " + group["groupID"] + " at address " + group["Consumers"][current[index]].getsockname()[0])
            group["Consumers"][current[index]].sendall(msg.encode()) ## send message
            current[index] = (current[index] + 1) % len(group["Consumers"])  ## increment current
            mutex.release()            
            index +=1 ## group

        time.sleep(2)


def acceptConsumer(consumer):
    mutex = Lock()
    ## GET GROUP ID
    groupID = consumer.recv(1024).decode()
    ## handle group ID
    ## PREVENT RACE CONDITION
    mutex.acquire()
    ## EXISTING CONSUMER EXITING
    if(groupID == ""):
        ## consumer is exiting, remove them from consumer groups
        group = next((group for group in consumerGroups if consumer in group["Consumers"] ), None)
        group["Consumers"].remove(consumer)
        print("    Consumer at " + consumer.getsockname()[0] + " removed from Consumer Group... ")
        consumer.close()
    ## GOT PORT
    else:
        group = next((group for group in consumerGroups if group["groupID"] == groupID), None)
        ## IF GROUP EXISTS
        if group:
            group["Consumers"].append(consumer)
            print("    Consumer added to group...")
        ## ELSE, CREATE GROUP
        else:
            consumerGroups.append({"groupID": groupID, "Consumers": [consumer]})
            print("    Group created, Consumer added...")
    mutex.release()
    ## END CRITICAL SECTION
    return

def exit_gracefully(signum, frame):
    ## SEND EXIT TO MASTER
    producer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## send MASTER the port we are listening on
    producer.connect((MASTER_IP, MASTER_PORT))
    msg = "producer\nexit\n" + topicLabel + "\n" + str(LISTEN_PORT) ##Message to send to MASTER 
    producer.send(msg.encode())
    producer.close()
    ## SEND EXIT TO ALL CONSUMERS
    for group in consumerGroups:
        for consumer in group["Consumers"]:
            consumer.send("".encode())

    # for consumer in consumerGroups:
    #     consumer.send("".encode())
    print("Exited Cleanly.\n")
    sys.exit(0)


## init?
# def __init__():
#     self.consumers = []
#     self.MASTER_IP = ?
#     MASTER_PORT = 8080
#     

if __name__ == '__main__':
    consumerGroups = []
    # FORMAT
    # [
    #     {"GroupID":"A", "Consumers": [consumer1, consumer2, ...]},
    #     {"GroupID":"B", "Consumers": [consumer1, consumer2, ...]}
    # ]
    
    LISTEN_PORT = get_free_tcp_port() ## get port number
    MASTER_IP = sys.argv[1] ## MASTER IP from cmd line
    topicLabel = sys.argv[2] ## topic from cmdln
    MASTER_PORT = 8080 ## MASTER PORT
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)
    run_producer()