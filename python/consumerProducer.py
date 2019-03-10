import socket
import random
import sys
import signal
import threading
import time
from threading import Lock

class Consumer:
    def __init__(self, IP, port, producers, topic, groupID,masterListener=None):
        self.IP = IP
        self.port = port
        self.topic = topic
        self.groupID = groupID
        self.producers = producers
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.masterListener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def getProducers(self):
        self.socket.connect((MASTER_IP,MASTER_PORT))
        msg = "consumer\n" + str(self.port) + "\n" + self.topic ##Message to send to MASTER 
        self.socket.send(msg.encode())
        prod = self.socket.recv(buffersize).decode()
        self.socket.send("ok".encode())
        self.socket.close()
        return prod

    # def spawnMasterListener(self):
    #     if(len(recvProd) < 1):
    #         newThread = threading.Thread(target=listenForMaster, args=())
    #         newThread.daemon = True
    #         recvProd.append(newThread)
    #         newThread.start()

    def listenForMaster(self):
        self.masterListener.listen(5)
        while True:
            master, addr = self.masterListener.accept()
            producer = master.recv(1024)
            master.send("ok".encode())
            producer = producer.decode()
            producer = producer.strip("\n")
            connectToProducer(producer)
            master.close()
    
    def connectToProducer(self, producer):
        print("Producer Added: " + producer)
        producer = producer.strip("\n")
        if not producer == '':
            ip, port = producer.split(":")
            prodConn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            prodConn.connect( (ip, int(port)) )
            self.producers.append(prodConn) ## add to list
        # Listen on Port for consumer
            prodConn.send(self.groupID.encode())
        return

    def recvData(self):
        while True:
            if len(self.producers) > 0:
                for producer in self.producers:
                    data = producer.recv(buffersize).decode()
                    if data == "":
                    ## remove producer from list of producers
                        self.producers.remove(producer)
                        ip, port = producer.getsockname()
                        print("Producer Removed: " + ip + ":" + str(port))
                        if(len(self.producers) == 0):
                            print("Waiting for more producers...")
                    else:
                        print("received: " + data) ## print for view
                        dataBuffer.append(data) ## add to buffer
                        

    # def spawnDataReceiver(self):
    #     if len(receiver) < 1:
    #         newThread = threading.Thread(target=recvData, args=(self))
    #         newThread.daemon = True
    #         receiver.append(newThread)
    #         newThread.start()
    #     return

    def destroy():
        # Join all threads, send master exit
        pass


class Producer:
    def __init__(self, IP, port, consumerGroups, topic, masterListener = None):
        self.IP = IP
        self.port = port
        self.topic = topic
        print("MY TOPIC IS: " + topic)
        self.consumerGroups = consumerGroups
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.masterListener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

#    def ready():
    def addConsumer(self, consumer):
        groupID = consumer.recv(buffersize).decode()
        mutex = Lock()
        mutex.acquire()
        group = next((group for group in self.consumerGroups if group["groupID"] == groupID), None)
        ## IF GROUP EXISTS
        if group:
            group["Consumers"].append(consumer)
            print("    Consumer added to group...")
        ## ELSE, CREATE GROUP
        else:
            self.consumerGroups.append({"groupID": groupID, "Consumers": [consumer]})
            print("    Group created, Consumer added...")
            print(consumer.getsockname()[0] + ":" +  str(consumer.getsockname()[1]))
        mutex.release()
        return
    
    def notifyMaster(self, ip, port):
        master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master.connect((ip,port))
        msg = "producer\n" + str(self.port) + "\n" + self.topic ##Message to send to MASTER 
        master.send(msg.encode())
        data = master.recv(buffersize).decode()
        master.send("ok".encode())
        master.close()
        return data

    def listenForMaster(self):
        pass

    def sendToGroups(self):
        data = getNext() ## get data from buffer
        mutex = Lock()
        current = [] ## track current consumer of each group
        while True:
            if not data == False:
                
             ## modify data
                if len(self.consumerGroups) > 0:
                    data = int(data) * int(data)
                    print("sending " + str(data))
                    print("\n")
            ## generate date
        
        # Init indexes of current consumer
                    for group in self.consumerGroups:
                        current.append(0)

                    index = 0 ## index of current group in consumerGroups
                    
                    for group in self.consumerGroups:
            # send msg to consumers
                        mutex.acquire()
                        print("sending " + str(data) + " to consumer " + str(current[index]) + " in group " + group["groupID"] + " at address " + group["Consumers"][current[index]].getsockname()[0])
                        group["Consumers"][current[index]].sendall(str(data).encode()) ## send message
                        current[index] = (current[index] + 1) % len(group["Consumers"])  ## increment current
                        mutex.release()            
                        index +=1 ## group
            data = getNext()
        print("Buffer is empty...")

def CandP():
    ### INIT CONSUMER
    consumerPort = get_free_tcp_port()
    producers = []
    consumer = Consumer(socket.gethostname(), consumerPort, producers, topicIn, 'A')
    print("Consumer created...")
    prods = consumer.getProducers()
    print(prods)
    consumer.connectToProducer(prods)
    print("Waiting for data...")
    newThread = threading.Thread(target=consumer.recvData, args=())
    newThread.daemon = True
    receiver.append(newThread)
    newThread.start()
    
    if(len(recvProd) < 1):
        newThread = threading.Thread(target=consumer.listenForMaster, args=())
        newThread.daemon = True
        recvProd.append(newThread)
        newThread.start()

    ### INIT PRODUCER
    producerPort = get_free_tcp_port()
    print(str(producerPort))
    consumerGroups = []
    producer = Producer(socket.gethostname(), producerPort, consumerGroups, topicOut)
    print("Producer created...")
    print(producer)
    producer.notifyMaster(MASTER_IP, MASTER_PORT)
    print("Master notified...")
    newThread = threading.Thread(target=producer.sendToGroups, args=())
    newThread.daemon = True
    newThread.start()
    print("Producer Sender Spawned...")
    
    print("Waiting for consumer...")
    producer.socket.bind((producer.IP, producer.port))
    producer.socket.listen(5)
    while True:
        c, addr = producer.socket.accept()
        newThread = threading.Thread(target=producer.addConsumer, args=(c,))
        newThread.daemon = True
        newThread.start()



def getNext():
    if len(dataBuffer) > 0:
        return dataBuffer.pop()
    else:
        return False


def get_free_tcp_port():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp.bind(('', 0))
    addr, port = tcp.getsockname()
    tcp.close()
    return port


if __name__ == "__main__":
    MASTER_IP = sys.argv[1]
    MASTER_PORT = 8080
    topicIn = sys.argv[2]
    topicOut = sys.argv[3]
    dataBuffer = []
    senders = []#?
    recvProd = []#?
    receiver = []
    buffersize = 1024
    CandP()