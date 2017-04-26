from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
from threading import Thread
from time import sleep
from random import seed
import sys
PORT = 0
worker_address = "http://localhost:13337/"

waittime = 5 #time wait to next timeout in seconds
isTimeOut = False
TimeOutCounter = False

class TimeOutThread(Thread):
    def start_new_term(self):
        print("TIME OUT at port " + PORT.__str__())

    def run(self):
        global TimeOutCounter
        global isTimeOut
        while True:
            if(isTimeOut) :
                print("Ending the Time Out Thread")
                break
            sleep(waittime)
            if not TimeOutCounter :
                self.start_new_term()
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
        if args[1] == 'load' :
            TimeOutCounter = True
            self.send_response(200)
            self.end_headers()
        else :
            self.request_worker(int(args[1]))


def StartNode(port):
    node = HTTPServer(("", port), NodeHandler)
    node.serve_forever()

def main():
    global PORT
    PORT = int(sys.argv[1])
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