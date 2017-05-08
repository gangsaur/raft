from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from threading import Thread
from random import seed, uniform
from time import sleep
import requests
import sys
import json
import urllib

#Heart beat thread
class heartBeat(Thread):
    def run(self):
        while True:
            #Process only if leader
            if status==2:
                while True:
                    global TimeOutCounter
                    TimeOutCounter = True
                    print("Heartbeat Timeout")
                    #Check for log length and decide what to send
                    package={
                        #id of the leader
                        'leader_id': nodeIndex,
                        #Last log number
                        'last_log' :nodeLog.numLog,
                        #Last leader term
                        'term' : currentTerm,
                        #Last commited by leader
                        'leader_commit' :nodeLog.commitedLog,
                    }
                    for url in balancerList:
                        try:
                            #Request to other follower
                            #Request needed log entry
                            print("Heartbeat to " + url)
                            if balancerList.index(url)!=nodeIndex:
                                someThread=Thread(target=requests.get(url+"appendLog/"+json.dumps(package)), timeout = 0.1)
                                someThread.daemon=True
                                someThread.start()
                        except Exception as e:
                            # print(e.__str__())
                            print("Heartbeat to " + url + " timed out")
                    sleep(2)

#Class to store workload
class workerLoad:
    def __init__(self,numWorker):
        self.numWorker=numWorker
        self.workload=[]
        #Set initial cpu load ke 99999
        for i in range(int(self.numWorker)):
            self.workload.append(99999)

#Log entry class
class logData:
    #Function to turn data into JSON format
    def tojson(self):
        return json.dumps(self, default=lambda o: o.__dict__,
            sort_keys=True, indent=4)

    def __init__(self,term,id_worker,workload):
        self.term=term
        self.id_worker=id_worker
        self.workload=workload

#Log class, collection of log entry
class log:
    #numlog and commited start from -1
    def __init__(self):
        self.commitedLog=-1
        self.numLog=-1
        self.logList = []

    #Used to append log data into log
    def append_log(self,logData):
        self.logList.append(logData)
        self.numLog += 1

    #Used to change log data in log
    def modify_log(self,logData,lastLogIndex):
        self.logList[lastLogIndex]=logData

    #Check if position is right or not and insert
    def insertLog(self,logData,lastLogIndex):
        if (self.numLog==(lastLogIndex-1)):
            self.append_log(logData)
            print("APPEND LOG")
        #elif self.commitedLog>=int(lastLogIndex):
          # raise Exception("Cannot change committed log")
        elif int(lastLogIndex)>self.numLog+1:
            print ("Lastlog: " + str(lastLogIndex))
            print("Numlog: " + str(self.numLog))
            raise Exception("Cannot skip log")
        #"In the middle insert case"
        #Modify old data and erase the rest
        else:
            self.modify_log(logData,lastLogIndex)
            for i in range (lastLogIndex+1,self.numLog):
                self.logList.pop()
            self.numLog=lastLogIndex

    #Commit log
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


waittime = 6+ uniform(2, 5) #Wait time until next timeout in seconds, added random factor

isTimeOut = False #Election Timeout
TimeOutCounter = False
workerList=[]
balancerList=[]
nodeIndex = 0 #Curren worker index according to config
status = 0 #0 = follower, 1 = candidate, 2 = leader
votedFor = -1
currentTerm = 0
numVote = 0
numWorker = 0
numBalancer = 0
nLog=-1
nCommited=-1
#Log for every nodes
nodeLog=log()

#Array to store workload
workerData=workerLoad(99)
#Last log input index
lastIndex=0

#Used to request vote to node
def requestvote(url) :
    print("Sendind request vote " + url)
    url=url.strip()
    try :
        requests.get(url + "vote/" + currentTerm.__str__() + "/" + nodeIndex.__str__() + "/" + nodeLog.numLog.__str__(), timeout = 0.1)
    except Exception as e:
        # print("Asking for vote timed out" + e.__str__())
        print("Asking for vote from " + url + " timed out")

#Thread to process timeout
class TimeOutThread(Thread):
    #Used to start new term and election
    def start_new_term(self):
        global numVote
        global currentTerm
        global votedFor
        #Vote for myself
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

    #Main thread process
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

            #If timeout, process
            if not TimeOutCounter :
                print("Timed out")
                #Timeout as follower, ascend to candidate
                if(status == 0) :
                    print("Become a candidate starting a new election")
                    status = 1
                    self.start_new_term()
                    #Wait for vote
                    sleep(1.5)

                #Timeout as candidate, check number of vote
                elif(status == 1) :
                    print("Number vote : " + numVote.__str__() )
                    #Ascend to leader
                    
                    #Failed, start new term and election
                    if(numVote < (numBalancer+1)/2) :
                        print("Fail election, starting a new one")
                        numVote = 0
                        self.start_new_term()
                        sleep(1.5)
            #Prevent timeout as leader
            if (status!=2) :
                TimeOutCounter = False

#HTTPServer to response request
class NodeHandler(BaseHTTPRequestHandler):
    #Response to get method
    def do_GET(self):
        global TimeOutCounter
        global numVote
        global currentTerm
        global votedFor
        global status
        #Get input, split by /
        args = self.path.split('/')
        #Response to 'vote for me' request
        if args[1] == 'vote' :
            self.send_response(200)
            self.end_headers()
            print("Received election vote request from node " + args[3] + " on term " + args[2])
            print("This node term " + currentTerm.__str__())
            TimeOutCounter = True
            #Reply yes if condition is met
            if ((currentTerm < int(args[2])) and (nodeLog.numLog <= int(args[4]))):
                status = 0 #Demote to follower
                currentTerm = int(args[2]) #Update current term
                votedFor = int(args[3]) #Update voted for
                try :
                    print("Vote yes to " + balancerList[int(args[3])])
                    requests.get(balancerList[int(args[3])]+"recVote/yes", timeout = 0.1)
                    sleep(3)
                    TimeOutCounter=True
                except Exception as e:
                    print("Sending vote yes to " + balancerList[int(args[3])] + " timed out")
            #Reply no if condition is not met
            else :
                try :
                    print("Vote no to " + balancerList[int(args[3])])
                    requests.get(balancerList[int(args[3])]+"recVote/no", timeout = 0.1)                    
                except Exception as e:
                    print("Sending vote no to " + balancerList[int(args[3])] + " timed out")

        #Response to sent CPU workload and store it
        elif args[1] == 'workload' :
            self.send_response(200)
            self.end_headers()
            #Process only if leader node
            if status==2:
                global lastIndex
                #Process sent json data
                data = json.loads(urllib.parse.unquote(args[2]))
                inputData=logData(currentTerm,int(data['worker_id']),float(data['workload']))

                #Put data into log
                nodeLog.insertLog(inputData,lastIndex)
                nodeLog.commitedLog+=1
                print(nodeLog.logList[nodeLog.numLog].workload)
                print(nodeLog.logList[nodeLog.numLog].id_worker)
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

        #Notify requester about voting decision
        elif args[1] == 'recVote' :
            self.send_response(200)
            self.end_headers()
            if args[2] == 'yes' :
                TimeOutCounter=True
                numVote += 1
                if(numVote >= ((numBalancer+1)/2)) :
                        print("Have majority vote, now becoming a leader")
                        numVote = 0
                        status = 2
                print("Received yes vote")
            else :
                print("Received no vote")
        
        #When leaders ask to append new log, check the log which follower has.
        #Follower request needed log entry. Commit according to leader last commit
        elif args[1] == 'appendLog':
            self.send_response(200)
            self.end_headers()
            numVote=0
            TimeOutCounter=True
            
            #Process sent json data
            data = json.loads(urllib.parse.unquote(args[2]))
            LeaderTerm=int(data['term'])
            
            #Update term
            currentTerm=LeaderTerm
            LeaderID = int(data['leader_id'])
            LeaderLastLog=int(data['last_log'])
            LeaderLastCommit=int(data['leader_commit'])
 
            url = balancerList[LeaderID]
            if (LeaderTerm<currentTerm):
                raise Exception("Leader term is less than follower term")
            elif LeaderLastLog<nodeLog.numLog:
                raise Exception("Leader log is lesser than follower log")
            elif LeaderLastCommit<nodeLog.commitedLog:
                raise Exception("Leader commit is lesser than follower log")
            else:
                #ubah commit follower sesuai leader
                nodeLog.commitedLog=LeaderLastCommit
            
            #Request log dari numlog saat ini, hingga leaderlast log
            try :
                print("Request log from leader")
                requests.get(url+"reqLog/"+nodeLog.numLog.__str__()+"/"+LeaderLastLog.__str__()+"/"+nodeIndex.__str__(), timeout = 0.1)
            except Exception as e :
                print("Requesting log from leader timed out")

        #Request append to follower
        elif args[1] == 'reqLog':
            self.send_response(200)
            self.end_headers()
            #Parse json data
            data = json.loads(urllib.parse.unquote(args[4]))
            #Process data into array
            SendLog=[]
            for i in range(int(args[2]),int(args[3])):
                SendLog.append(nodeLog.logList[i].tojson())
            jumlahData=int(args[3])-int(args[2])
            print("JUMLAH DATANYA: "+jumlahData.__str__())
            #Make dict which will be used for mesage
            dict={
                'Log':SendLog,
                'NumData':jumlahData,
            }
            #Request append ke follower
            argument4=args[4]
            targetIndex=int(argument4)
            targetURL=balancerList[targetIndex]
            
            #Send request
            try :
                requests.get(targetURL+"append/"+json.dumps(dict), timeout = 0.1)
            except Exception as e :
                print("Sending log data to " + targetURL + " timed out")
        
        #Append new data for follower from received message
        elif args[1] == 'append':
            self.send_response(200)
            self.end_headers()
            #Parse JSON
            data = json.loads(urllib.parse.unquote(args[2]))

            #Append every new sent data
            for i in range(int(data['NumData'])):
                print(data['Log'][i].__str__())
                t = json.loads(data['Log'][i])
                nodeLog.append_log(t)
                workerData.workload[t['id_worker']] = t['workload']
                print(workerData.workload.__str__())

        #Search for prime numbers
        #formatnya: LaodBalancerURL/AngkaYangDicari
        else :
            bilanganDicari=int(args[1])
            indexWorkerTerkecil=0
            #Loop to search smallest worker
            for i in range(workerData.numWorker):
                if workerData.workload[indexWorkerTerkecil]>workerData.workload[i]:
                    indexWorkerTerkecil=i
            url=workerList[indexWorkerTerkecil]
            url=url+bilanganDicari.__str__()

            #Redirect request to worker
            self.send_response(301)
            self.send_header('Location', url)
            self.end_headers()

#Read configuration file which contains
#address and port of workers, daemons, and nodes
def load_config():
    global numWorker
    global numBalancer
    config = 0

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

#Used to start HTTPServer
def StartNode(port):
    node = HTTPServer(("", port), NodeHandler)
    node.serve_forever()

def main():
    global PORT
    global nodeIndex
    global isLeader
    global workerData
    
    #Load list of workers and balancers
    load_config()

    #Get appropiate port from index which user input
    nodeIndex = int(sys.argv[1])
    start = balancerList[nodeIndex].find(":",8)
    end = balancerList[nodeIndex].find("/",8)
    PORT = int(balancerList[nodeIndex][start+1:end])
    
    #Create worker data to store workloads
    workerData = workerLoad(len(workerList))

    #Start server to request response
    try:
        node_thread = Thread(target=StartNode,args = (PORT, ))
        node_thread.daemon = True
        node_thread.start()
        print("Node is started in port " + PORT.__str__())
    except:
        print("Error spawning node")
        exit()

    #Start time out thread
    tothread = TimeOutThread()
    tothread.daemon = True
    tothread.start()
    
    #Start Heart Beat thread
    Heart = heartBeat()
    Heart.daemon=True
    Heart.start()

    #Prevent program closing
    input("")

#To start main()
if __name__ == "__main__":
    main()