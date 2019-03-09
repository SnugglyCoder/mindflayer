import socket
import sys
import signal
import threading

def run_consumer():
    print("Listening on port " + str(LISTEN_PORT))
    topicLabel = "random"
    print("Launching printing daemon...")
    newThread = threading.Thread(target=printData, args=())
    newThread.daemon = True
    newThread.start()

    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## send MASTER the port we are listening on
    master.connect((MASTER_IP, MASTER_PORT))
    msg = "consumer\n" + str(LISTEN_PORT) ##Message to send to MASTER 
    master.send(msg.encode())
    master_response = master.recv(1024).decode()
    print("Master: " + master_response)
    master.close()

    ## Start daemon to listen for master
    newThread = threading.Thread(target=listenForMaster, args=())
    newThread.daemon = True
    newThread.start()

    prods = master_response.split("\n")
    if len(prods) > 0:
        ## Use list of producers to get data
        for producer in prods:
            connectToProducer(producer)
    
    ## empty list from master, wait until master sends producers to daemon process    

    
    
## Daemon to print data when connected to producers
def printData():
    while True:
        if len(producers) > 0:
            for producer in producers:
                data = producer.recv(1024).decode()
                if data == "":
                    ## remove producer from list of producers
                    producers.remove(producer)
                else:
                    print(data)
        
## connect to producer, send groupID, and add socket to list of producer sockets
def connectToProducer(producer):
    print(producer)
    ip, port = producer.split(":")
    print(ip)
    print(port)
    prodConn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    prodConn.connect((ip, int(port)))
    producers.append(producer) ## add to list
    # Listen on Port for consumer
    prodConn.send(groupID.encode())
    return

## daemon to update producers list from master
def listenForMaster():
    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    master.bind((socket.gethostname(), LISTEN_PORT))
    master.listen(5)

    while True:
        master, addr = master.accept()
        producer = master.recv(1024).decode()
        ip, port = producer.split(":")
        producer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        producers.append(producer)
        master.close()


def exit_gracefully(signum, frame):
     ## SEND EXIT TO MASTER
    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## send MASTER the port we are listening on
    master.connect((MASTER_IP, MASTER_PORT))
    msg = "consumer\nexit\n" + topicLabel + "\n" + str(LISTEN_PORT) ##Message to send to MASTER 
    master.send(msg.encode())
    master.close()
    print("Exited from Master...")
    ## SEND EXIT TO ALL producers
    for producer in producers:
        producer.send("".encode())
    print("Exited from Producers...")
    # for consumer in consumerGroups:
    #     consumer.send("".encode())
    print("Exited Cleanly.\n")
    sys.exit(0)

## gets free port to listen on
def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port

if __name__ == "__main__":
    producers = [] 
    LISTEN_PORT = get_free_tcp_port() ## get port number
    MASTER_IP = sys.argv[1] ## MASTER IP from cmd line
    topicLabel = sys.argv[2] ## topic from cmdln
    groupID = sys.argv[3] ## groupID
    MASTER_PORT = 8080 ## MASTER PORT
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)
    run_consumer()