import psutil
import time
import json
import requests
import socket
from threading import Thread

#assumes you have an internet access, and that there is no local proxy
#http://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
def getOwnIpAddress():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    return s.getsockname()[0]

def getWorkerId():
    url="http://"+getOwnIpAddress()+":13337/"
    print (url)
    print(workerList.index(url))
    return workerList.index(url)

def load_config():
    #membaca file configuration
    #yang berisi daftar alamat dan port
    #seluruh worker, daemon, dan node

    try:
        config=open("config.txt","r")
    except Exception:
        print(Exception.__str__())
        exit()

    #Baca line pertama, jumlah worker
    numWorker=int(config.__next__())
    for i in range(numWorker):
        #rstrip untuk menghilangkan whitespace
        workerList.append(config.__next__().rstrip())
    numBalancer=int(config.__next__())
    for i in range(numBalancer):
        #rstrip untuk menghilangkan whitespace
        balancerList.append(config.__next__().rstrip())
    config.close()
    print(workerList)
    print(balancerList)


def getWorkload():
    return psutil.cpu_percent(interval=1)


#Global variable yang berisi list URL menuju masing" node
workerList=[]
balancerList=[]
#Timespan antar broadcast daemon
daemonDelay=2
psutil.cpu_percent(interval=1)
#gunakan ini jika sudah akan implementasi
#worker_id=getWorkerId()
worker_id=0

#Loac config.txt
load_config()
#Loop untuk melakukan broadcast terus menerus
while True:
    current_workload=getWorkload().__str__()
    package={
        'worker_id':worker_id,
        'workload':current_workload,
    }
    #mengirim via get ke seluruh balancer
    for url in balancerList:
        try:
            print("Broadcast workload to: "+url+"workload/"+json.dumps(package))
            Thread(target=requests.get(url+"workload/"+json.dumps(package)))
        except Exception:
            print(Exception)
    #Sleep selama daemonDelay agar tidak spamming
    #time.sleep(daemonDelay)