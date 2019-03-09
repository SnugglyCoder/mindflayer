import socket
import random
import sys
import signal
import threading
import time

def run_producer():
    print("Listening on port: " + str(LISTEN_PORT))
    ## CREATE CONNECTION TO MASTER
    # producer socket
    TOPIC = "" # TBD
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
    producer.listen(5)
    # WHILE LOOP HERE?
    while True:
        consumer, addr = producer.accept()
        consumers.append(consumer)
        newThread = threading.Thread(target=handleConsumer, args=(consumer,))
        newThread.daemon = True
        newThread.start()
## gets free port to listen on
def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port

def handleConsumer(consumer):
    print("    Connected to Consumer...")
    while True:
        msg = str(random.randint(0,100)) # random message for test
        consumer.sendall(msg.encode()) ## send to consumer
        time.sleep(1)
    

def exit_gracefully(signum, frame):
    ## SEND EXIT TO MASTER
    producer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## send MASTER the port we are listening on
    producer.connect((MASTER_IP, MASTER_PORT))
    msg = "exit \n" + str(LISTEN_PORT) ##Message to send to MASTER 
    producer.send(msg.encode())
    producer.close()
    ## SEND EXIT TO CONSUMERS
    for consumer in consumers:
        consumer.send("".encode())
    sys.exit(0)

if __name__ == '__main__':
    consumers = []
    LISTEN_PORT = get_free_tcp_port() ## get port number
    MASTER_IP = sys.argv[1] ## MASTER IP from cmd line
    MASTER_PORT = 8080 ## MASTER PORT
    original_sigint = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, exit_gracefully)
    run_producer()