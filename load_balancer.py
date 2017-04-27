from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from threading import Thread
from time import sleep
import requests
import sys
import json
import urllib

class workerLoad:
    def __init__(self,numWorker):
        self.numWorker=numWorker
        self.workload=[]
        #Set initial cpu load ke 99999
        for i in range(int(self.numWorker)):
            self.workload.append(99999)


class logData:
    def __init__(self,term,id_worker,workload):
        self.term=term
        self.id_worker=id_worker
        self.workload=workload




class log:
    #numlog dan commited selalu mulai dari -1
    #hal ini perlu dimasukkan pada variable volatile di setiap server
    def __init__(self):
        self.commitedLog=-1
        self.numLog=-1
        self.logList = []

    def append_log(self,logData):
        self.logList.append(logData)
        self.numLog += 1

    def modify_log(self,logData,lastLogIndex):
        self.logList[lastLogIndex]=logData


    def insertLog(self,logData,lastLogIndex):
        #jika insert log data baru
        if (self.numLog==(lastLogIndex-1)):
            self.logList.append(logData)


        elif self.commitedLog>=int(lastLogIndex):
            raise Exception("Cannot change committed log")
        elif int(lastLogIndex)>self.numLog+1:
            print ("Lastlog: " + str(lastLogIndex))
            print("Numlog: " + str(self.numLog))
            raise Exception("Cannot skip log")
        #kasus jika insert di tengah" data belum dicommit
        #modify data yang lama dan hapus data" setelahnya
        else:
            self.modify_log(logData,lastLogIndex)
            for i in range (lastLogIndex+1,self.numLog):
                self.logList.pop()
            self.numLog=lastLogIndex


    def commit(self,lastLogIndex):
        if lastLogIndex>self.numLog:
            raise Exception("Index is too far")
        elif lastLogIndex<=self.commitedLog:
            raise Exception("Cannot commit ")
        elif lastLogIndex>self.commitedLog+1:
            raise Exception("Commit skipped")
        else:
            self.commitedLog=lastLogIndex






PORT = 0
worker_address = "http://localhost:13337/"

waittime = 5 #time wait to next timeout in seconds
isTimeOut = False #Election Timeout
TimeOutCounter = False
workerList=[]
balancerList=[]
nodeIndex = 0
status = 2 #0 = follower, 1 = candidate, 2 = leader
votedFor = -1
currentTerm = -1
numVote = 0
numWorker = 0
numBalancer = 0
nLog=-1
nCommited=-1
#log di setiap node
nodeLog=log()
#perbandingan workload antar worker
workerData=workerLoad(99)
#Index input terakhir
lastIndex=0

class TimeOutThread(Thread):
    def start_new_term(self):
        global currentTerm
        currentTerm += 1
        for url in balancerList:
            if balancerList.index(url)!=nodeIndex:
                requests.get(url.__str__() + "vote/" + currentTerm.__str__() + "/" + nodeIndex.__str__() )

    def run(self):
        global TimeOutCounter
        global isTimeOut
        global status
        global numVote
        while True:
            if(isTimeOut) :
                print("Ending the Time Out Thread")
                break
            sleep(waittime)
            if not TimeOutCounter :
                #timeout sebagai follower, menjadi kandidat lalu mulai election baru
                if(status == 0) :
                    status = 1
                    self.start_new_term()

                #timeout sebagai candidate, memulai election baru
                if(status == 1) :
                    self.start_new_term()
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
        global numVote
        global currentTerm
        global votedFor
        args = self.path.split('/')
        #Merespon permintaan vote
        if args[1] == 'vote' :
            self.send_response(200)
            self.end_headers()
            print("Received election vote request from node " + args[3])
            TimeOutCounter = True
            if currentTerm < int(args[2]) :
                currentTerm = int(args[2])
                votedFor = int(args[3])
                requests.get(balancerList[int(args[3])]+"recVote/yes")
            else :
                requests.get(balancerList[int(args[3])]+"recVote/no")
                
        #Menerima workload CPU
        elif args[1] == 'workload' :
            #hanya berjalan jika leader

            if status==2:
                global lastIndex
                # mengambil data json yang dikirim berupa workload
                data = json.loads(urllib.parse.unquote(args[2]))
                inputData=logData(currentTerm,int(data['worker_id']),float(data['workload']))
                nodeLog.insertLog(inputData,lastIndex)
                nodeLog.numLog+=1
                lastIndex+=1
                #Harusnya belum dicommit, untuk di tes terlebih dahulu
                print(int(data['worker_id']).__str__())
                workerData.workload[int(data['worker_id'])]=float(data['workload'])
                print(nodeLog.logList[nodeLog.numLog].id_worker)
                print(nodeLog.logList[nodeLog.numLog].workload)
            self.send_response(200)
            self.end_headers()
            
        #Menerima respon permintaan vote
        elif args[1] == 'recVote' :
            self.send_response(200)
            self.end_headers()
            if args[2] == 'yes' :
                numVote += 1
                print("Received yes vote")

            if numVote > ((numBalancer + 1) / 2) :
                isLeader = True
                numVote = 0
                print("Received no vote")
        elif args[1] == 'workload':
            self.send_response(200)
            self.end_headers()
            dict=json.dumps(urllib.parse.unquote(args[2]))

        else :
            self.request_worker(int(args[1]))

def load_config():
    #membaca file configuration
    #yang berisi daftar alamat dan port
    #seluruh worker, daemon, dan node
    global numWorker
    global numBalancer
    config = 0

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
    global workerData
    workerData = workerLoad(len(workerList))
    print(nodeLog.commitedLog)
    print(nodeLog.numLog)
    print(workerData.workload[0])
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