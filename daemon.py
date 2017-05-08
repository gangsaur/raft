import psutil
import time
import json
import requests
import socket
from threading import Thread

#Assume you have internet access, used to find local IP
#http://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
def getOwnIpAddress():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

#Get index / id from worker list
def getWorkerId():
    url="http://"+getOwnIpAddress()+":13337/"
    print (url)
    print(workerList.index(url))
    return workerList.index(url)

#Read configuration file which contains
#address and port of workers, daemons, and nodes
def load_config():
    #Open config file
    try:
        config=open("config.txt","r")
    except Exception:
        print(Exception.__str__())
        exit()

    #Read first line, the amount of workers
    numWorker=int(config.__next__())
    for i in range(numWorker):
        #rstrip to delete whitespace
        workerList.append(config.__next__().rstrip())

    #Read next line, the amound of nodes
    numBalancer=int(config.__next__())
    for i in range(numBalancer):
        #rstrip to delete whitespace
        balancerList.append(config.__next__().rstrip())

    config.close()
    print(workerList)
    print(balancerList)

#Get CPU workload from server
def getWorkload():
    return psutil.cpu_percent(interval=1)

#Global variable which contains URL to each workers
workerList=[]
#Global variable which contains URL to each nodes
balancerList=[]
#Timespan between each daemon broadcast
daemonDelay=2
#Interval of getting CPU usage
psutil.cpu_percent(interval=1)

#Get appropiate worker ID
#worker_id=getWorkerId()
worker_id=0

#Load config.txt
load_config()

#Loop to continously broadcast
while True:
    #Get current workload and construct messages
    current_workload=getWorkload().__str__()
    package={
        'worker_id':worker_id,
        'workload':current_workload,
    }

    #Send messages via get method, thread to prevent blocking execution
    for url in balancerList:
        try:
            print("Broadcast workload to: "+url+"workload/"+json.dumps(package))
            Thread(target=requests.get(url+"workload/"+json.dumps(package)))
        except Exception as e:
            if (type(e) != requests.exceptions.ConnectionError):
                print(str(e))
            else:
                print()