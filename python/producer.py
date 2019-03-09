import socket
import random
import sys

MASTER_PORT = 8080
LISTEN_PORT = 9090

def main():

    MASTER_IP = sys.argv[1]
    msg = str(random.randint(0,100)) # random message for test
    ## CREATE CONNECTION TO MASTER
    # producer socket
    TOPIC = "" # TBD
    producer = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ## send MASTER the port we are listening on
    producer.connect((MASTER_IP, MASTER_PORT))
    producer.sendall("producer \n" + str(LISTEN_PORT))
    producer.recv()
    producer.close()

    # Listen on Port for consumer
    producer.accept()
    producer.sendall(msg)

if __name__ == '__main__':
    main()