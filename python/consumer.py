import socket
import sys
import signal
import threading

def run_consumer():
    print("Listening on port " + str(LISTEN_PORT))
    print("Launching printing daemon...")
    newThread = threading.Thread(target=printData, args=())
    newThread.daemon = True
    newThread.start()

    master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## send MASTER the port we are listening on
    master.connect((MASTER_IP, MASTER_PORT))
    msg = "consumer\n" + str(LISTEN_PORT) + "\n" + topicLabel ##Message to send to MASTER 
    master.send(msg.encode())
    master_response = master.recv(1024)
    master_response = master_response.decode()
    master.send("ok".encode())
    print("Master: " + master_response)
    master.close()

    ## Start daemon to listen for master
    newThread = threading.Thread(target=listenForMaster, args=())
    newThread.daemon = True
    newThread.start()

    prods = master_response.split("\n")
    prods[:] = [x for x in prods if x != '']    # remove all whitespace
    if len(prods) > 0:
        ## Use list of producers to get data
        for producer in prods:
            connectToProducer(producer)
    count = 0
    while True:
        count += 1
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
                    ip, port = producer.getsockname()
                    print("Producer Removed: " + ip + ":" + str(port))
                    if(len(producers) == 0):
                        print("Waiting for more producers...")
                else:
                    print(data)
        
## connect to producer, send groupID, and add socket to list of producer sockets
def connectToProducer(producer):
    print("Producer Added: " + producer)
    producer = producer.strip("\n")
    ip, port = producer.split(":")
    prodConn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    prodConn.connect( (ip, int(port)) )
    producers.append(prodConn) ## add to list
    # Listen on Port for consumer
    prodConn.send(groupID.encode())
    return

## daemon to update producers list from master
def listenForMaster():
    consumer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    consumer.bind((socket.gethostname(), LISTEN_PORT))
    consumer.listen(5)

    while True:
        master, addr = consumer.accept()
        producer = master.recv(1024)
        master.send("ok".encode())
        producer = producer.decode()
        producer = producer.strip("\n")
        connectToProducer(producer)
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
    closingSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    for producer in producers:
        # closingSocket.connect(producer.getsockname())
        # closingSocket.send()
        print(producer)
        producer.send("".encode())
        producer.close()
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

#def handler(s):
  #  print("Broke")

if __name__ == "__main__":
    producers = [] 
    LISTEN_PORT = get_free_tcp_port() ## get port number
    MASTER_IP = sys.argv[1] ## MASTER IP from cmd line
    topicLabel = sys.argv[2] ## topic from cmdln
    groupID = sys.argv[3] ## groupID
    MASTER_PORT = 8080 ## MASTER PORT
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)
  #  signal(SIGPIPE, handler)
    run_consumer()