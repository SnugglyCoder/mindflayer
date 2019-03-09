import socket
import random
import sys

MASTER_PORT = 8080
LISTEN_PORT = 9090

def main():

    MASTER_IP = sys.argv[1]
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
    print("    Listening for consumers...")
    producer.listen(5)
    consumer, addr = producer.accept()
    # WHILE LOOP HERE?
    msg = str(random.randint(0,100)) # random message for test
    producer.sendall(msg.encode()) ## send to consumer
    consumer_resp = producer.recv(1024)
    print("Consumer: " + consumer_resp.decode())

if __name__ == '__main__':
    main()