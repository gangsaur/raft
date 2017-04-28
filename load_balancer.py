from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from threading import Thread
from random import seed, uniform
from time import sleep
import requests
import sys
import json
import urllib

class heartBeat(Thread):

    def run(self):
        while True:
            #cuma berjalan jika leader
            if status==2:
                while True:

                    global TimeOutCounter
                    TimeOutCounter = True
                    print("Kirimkan hati")
                    #Mengirimkan panjang log dan mempersiapkan log apa saja yang akan dikirim
                    package={
                        #id of the leader
                        'leader_id': nodeIndex,
                        #last log number
                        'last_log' :nodeLog.numLog,
                        #Last leader term
                        'term' : currentTerm,
                        #Last commited by leader
                        'leader_commit' :nodeLog.commitedLog,
                    }
                    for url in balancerList:
                        try:
                            #Request ke follower lain untuk
                            #Meminta daftar log yang butuh mereka commit
                            if balancerList.index(url)!=nodeIndex:
                                someThread=Thread(target=requests.get(url+"appendLog/"+json.dumps(package),timeout=1))
                                someThread.daemon=True
                                someThread.start()
                        except Exception as e:
                            print(e.__str__())
                    sleep(2)


class workerLoad:
    def __init__(self,numWorker):
        self.numWorker=numWorker
        self.workload=[]
        #Set initial cpu load ke 99999
        for i in range(int(self.numWorker)):
            self.workload.append(99999)


class logData:
    def tojson(self):
        return json.dumps(self, default=lambda o: o.__dict__,
            sort_keys=True, indent=4)

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
            self.append_log(logData)
            print("APPEND LOG")

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





seed()

PORT = 0
worker_address = "http://localhost:13337/"

waittime = 6+ uniform(0, 4) #time wait to next timeout in seconds
isTimeOut = False #Election Timeout
TimeOutCounter = False
workerList=[]
balancerList=[]
nodeIndex = 0 #Index worker sesuai file config
status = 0 #0 = follower, 1 = candidate, 2 = leader
votedFor = -1
currentTerm = 0
numVote = 0
numWorker = 0
numBalancer = 0
nLog=-1
nCommited=-1
#log di setiap node
nodeLog=log()
#Variable yang menyimpan ID Leader saat ini


#perbandingan workload antar worker
workerData=workerLoad(99)
#Index input log terakhir
lastIndex=0

def requestvote(url) :
    print("sendind request vote " + url)
    url=url.strip()
    try :
        requests.get(url + "vote/" + currentTerm.__str__() + "/" + nodeIndex.__str__(),timeout=1)
    except Exception as e:
        print("error request exception" + e.__str__())

class TimeOutThread(Thread):
    def start_new_term(self):
        global numVote
        global currentTerm
        global votedFor
        #Set votedFor ke diri sendiri
        votedFor=nodeIndex
        currentTerm += 1
        numVote +=1
        print("starting election on term " + currentTerm.__str__())
        for url in balancerList:
            if balancerList.index(url)!=nodeIndex:
                url=url.strip()
                voteThread = Thread(target=requestvote,args = (url, ))
                voteThread.daemon = True
                voteThread.start()

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
                    print("Become a candidate starting a new election")
                    status = 1
                    self.start_new_term()
                    #Menunggu vote
                    sleep(1.5)

                #timeout sebagai candidate, memulai election baru
                elif(status == 1) :
                    print("number vote : " + numVote.__str__() )
                    if(numVote >= ((numBalancer+1)/2)) :
                        print("Have majority vote, now becoming a leader")
                        numVote = 0
                        status = 2
                    else :
                        print("Fail election, starting a new one")
                        numVote = 0
                        self.start_new_term()
                        sleep(1.5)
            if (status!=2) :
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
        global status
        args = self.path.split('/')
        #Merespon permintaan vote
        if args[1] == 'vote' :
            self.send_response(200)
            self.end_headers()
            print("Received election vote request from node " + args[3] + " on term " + args[2])
            print(" This node term " + currentTerm.__str__())
            TimeOutCounter = True
            if currentTerm < int(args[2]) :
                status = 0
                currentTerm = int(args[2])
                votedFor = int(args[3])
                try :
                    print("Vote yes")
                    requests.get(balancerList[int(args[3])]+"recVote/yes",timeout=1)
                    sleep(3)
                    TimeOutCounter=True
                except :
                    print("error request")
            else :
                try :
                    print("Vote no")
                    requests.get(balancerList[int(args[3])]+"recVote/no",timeout=1)
                except :
                    print("error request")

        #Menerima workload CPU
        elif args[1] == 'workload' :
            #hanya berjalan jika leader
            if status==2:
                global lastIndex
                # mengambil data json yang dikirim berupa workload
                data = json.loads(urllib.parse.unquote(args[2]))
                inputData=logData(currentTerm,int(data['worker_id']),float(data['workload']))
                nodeLog.insertLog(inputData,lastIndex)
                nodeLog.commitedLog+=1
                print(nodeLog.logList[nodeLog.numLog].workload)
                print(nodeLog.logList[nodeLog.numLog].id_worker)
                print("YOW WHAT THE FUCK")
                print("Panjang log leader saat ini: "+nodeLog.numLog.__str__())
                lastIndex+=1

                #Harusnya belum dicommit, untuk di tes terlebih dahulu

                workerData.workload[int(data['worker_id'])]=float(data['workload'])
                print("Worker Data:")
                print("Jumlah worker: "+workerData.numWorker.__str__())
                for i in range (workerData.numWorker):
                    print(workerData.workload[i].__str__())
                # print(nodeLog.logList[nodeLog.numLog].id_worker)
                # print(nodeLog.logList[nodeLog.numLog].workload)
            self.send_response(200)
            self.end_headers()

        #Menerima respon permintaan vote
        elif args[1] == 'recVote' :
            self.send_response(200)
            self.end_headers()
            if args[2] == 'yes' :
                TimeOutCounter=True
                numVote += 1
                print("Received yes vote")
            else :
                print("Received no vote")

        #Ketika leader meminta append log baru
        #Cek log yang sudah dimiliki oleh follower
        #Dan follower merequest log yang dibutuhkan menuju leader
        #Jangan lupa lakukan commit untuk data berdasarkan last commit leader
        elif args[1] == 'appendLog':
            print("MASUK APPENDLOG")
            numVote=0
            TimeOutCounter=True
            self.send_response(200)
            self.end_headers()
            data = json.loads(urllib.parse.unquote(args[2]))
            LeaderTerm=int(data['term'])
            currentTerm=LeaderTerm
            LeaderID = int(data['leader_id'])
            LeaderLastLog=int(data['last_log'])
            LeaderLastCommit=int(data['leader_commit'])
            url = balancerList[LeaderID]
            #if (LeaderTerm<currentTerm):
             #   raise Exception("Leader term is less than follower term")
            if LeaderLastLog<nodeLog.numLog:
                raise Exception("Leader log is lesser than follower log")
            elif LeaderLastCommit<nodeLog.commitedLog:
                raise Exception("Leader commit is lesser than follower log")
            else:
                #ubah commit follower sesuai leader
                nodeLog.commitedLog=LeaderLastCommit
                #Request log dari numlog saat ini, hingga leaderlast log

            requests.get(url+"reqLog/"+nodeLog.numLog.__str__()+"/"+LeaderLastLog.__str__()+"/"+nodeIndex.__str__(),timeout=1)

        elif args[1] == 'reqLog':
            self.send_response(200)
            self.end_headers()
            data = json.loads(urllib.parse.unquote(args[4]))
            SendLog=[]
            for i in range(int(args[2]),int(args[3])):
                SendLog.append(nodeLog.logList[i].tojson())
            jumlahData=int(args[3])-int(args[2])
            print("JUMLAH DATANYA: "+jumlahData.__str__())
            dict={
                'Log':SendLog,
                'NumData':jumlahData,
            }
            #Request append ke follower
            argument4=args[4]
            targetIndex=int(argument4)
            targetURL=balancerList[targetIndex]

            print("TARGETNYA : " + targetURL)
            print("FUCKING JSON DUMP "+json.dumps(dict))
            requests.get(targetURL+"append/"+json.dumps(dict))
        elif args[1] == 'append':
            print("MASUK APPEND FOR GOD SAKE")
            self.send_response(200)
            self.end_headers()
            data = json.loads(urllib.parse.unquote(args[2]))
            print("JUMLAH DATA: "+data['NumData'].__str__())
            for i in range(int(data['NumData'])):
                print("APPEND DATA BARU")
                print(data['Log'][i].__str__())
                nodeLog.append_log(data['Log'][i])
                workerData.workload[data['Log'][i].id_worker]=data['Log'][i].workload
                
            print("JUMLAH DATA FOLLOWER SAAT INI: " + nodeLog.numLog.__str__())



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
    Heart = heartBeat()
    Heart.daemon=True
    Heart.start()

    input("\nPress anything to exit..\n\n")



if __name__ == "__main__":
    main()