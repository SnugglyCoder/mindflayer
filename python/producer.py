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
    msg = "producer \n" + str(LISTEN_PORT) ##Message to send to MASTER 
    producer.send(msg.encode())
    master_response = producer.recv(1024)
    print("Master: " + master_response.decode())
    producer.close()
    
    producer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Listen on Port for consumer
    producer.bind((socket.gethostname(),LISTEN_PORT))
    print("    Waiting for consumers...")
    newThread = threading.Thread(target=sendToGroups, args=(,))
    newThread.daemon = True
    newThread.start()
    
    producer.listen(5)
    # WHILE LOOP HERE?
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
    current = [] ## track current consumer of each group
    while True:
        print("\n")
        ## generate date
        msg = str(random.randint(0,100))
        
        # Init indexes of current consumer
        for group in consumerGroups:
            current.append(0)

        index = 0 ## index of current group in consumerGroups
        for group in consumerGroups:
            # send msg to consumers
            print("sending " + msg + " to consumer " + str(current[index]) + " in group " + group["groupID"] + " at address " + group["Consumers"][current[index]].getsockname()[0])
            group["Consumers"][current[index]].sendall(msg.encode()) ## send message
            current[index] = current[index] + 1 % len(group["Consumers"]) ## increment current
            index +=1 ## group
        time.sleep(2)


def acceptConsumer(consumer):
    mutex = Lock()
    ## GET GROUP ID
    groupID = consumer.recv(1024).decode()
    ## handle group ID
    ## IF GROUP EXISTS
    exists = next((group for group in consumerGroups if group["groupID"] == groupID), None)
    ## Prevent race condition
    mutex.acquire()
    if exists:
        exists["Consumers"].append(consumer)
        print("    Consumer added to group...")
    else:
        consumerGroups.append({"GroupID": groupID, "Consumers": [consumer]})
        print("    Group created, Consumer added...")
    mutex.release()
    return
    ## ELSE, CREATE GROUP
    # while True:
    #     msg = str(random.randint(0,100)) # random message for test
    #     consumer.sendall(msg.encode()) ## send to consumer
    #     time.sleep(1)
    

def exit_gracefully(signum, frame):
    ## SEND EXIT TO MASTER
    producer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## send MASTER the port we are listening on
    producer.connect((MASTER_IP, MASTER_PORT))
    msg = "exit \n" + str(LISTEN_PORT) ##Message to send to MASTER 
    producer.send(msg.encode())
    producer.close()
    ## SEND EXIT TO ALL CONSUMERS
    for group in consumerGroups:
        for consumer in group["Consumers"]
            consumer.send("".encode())

    # for consumer in consumerGroups:
    #     consumer.send("".encode())
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
    MASTER_PORT = 8080 ## MASTER PORT
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)
    run_producer()