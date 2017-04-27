from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from threading import Thread
from time import sleep
from random import seed
import requests
import sys
PORT = 0
worker_address = "http://localhost:13337/"

waittime = 5 #time wait to next timeout in seconds
isTimeOut = False
TimeOutCounter = False
workerList=[]
balancerList=[]
nodeIndex
isLeader = False
votedFor = -1
currentTerm = -1
numVote = 0
numWorker
numBalancer

class TimeOutThread(Thread):
    def start_new_term(self):
        for url in balancerList:
            requests.get(url+"")

    def run(self):
        global TimeOutCounter
        global isTimeOut
        while True:
            if(isTimeOut) :
                print("Ending the Time Out Thread")
                break
            sleep(waittime)
            if not TimeOutCounter :
                
                isTimeOut = True
            else :
                TimeOutCounter = False
                print("Reset timeout")

class NodeHandler(BaseHTTPRequestHandler):
    def request_worker(self,n):
        #redirecting client request to worker
        request = worker_address + n.__str__();
        self.send_response(301)
        self.send_header('Location',request)
        self.end_headers()

    def do_GET(self):
        global TimeOutCounter
        args = self.path.split('/')
        #Merespon permintaan vote
        if args[1] == 'vote' :
            if currentTerm < args[2] :
                currentTerm = args[2]
                votedFor = args[3]
                requests.get(balancerList[args[3]]+"recVote/yes")
            else
                requests.get(balancerList[args[3]]+"recVote/no")
                
        #Menerima workload CPU
        else if args[1] == 'load' :
            TimeOutCounter = True
            self.send_response(200)
            self.end_headers()
            
        #Menerima respon permintaan vote
        else if args[1] == 'recVote' : 
            if args[2] == "yes" :
                numVote++
            if numVote > ((numBalancer + 1) / 2) :
                isLeader = True
                numVote = 0
        else :
            self.request_worker(int(args[1]))

def load_config():
    #membaca file configuration
    #yang berisi daftar alamat dan port
    #seluruh worker, daemon, dan node
    global numWorker
    global numBalancer

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

def StartNode(port):
    node = HTTPServer(("", port), NodeHandler)
    node.serve_forever()

def main():
    global PORT
    global nodeIndex
    global isLeader
    load_config()
    nodeIndex = int(sys.argv[1])
    start = balancerList[nodeIndex].find(":",8)
    end = balancerList[nodeIndex].find("/",8)
    PORT = int(balancerList[nodeIndex][start+1:end])
    try:
        node_thread = Thread(target=StartNode,args = (PORT, ))
        node_thread.daemon = True
        node_thread.start()
        print("Node is started in port " + PORT.__str__())
    except:
        print("Error spawning node")
        exit()

    tothread = TimeOutThread()
    tothread.daemon = True
    tothread.start()

    input("\nPress anything to exit..\n\n")



if __name__ == "__main__":
    main()