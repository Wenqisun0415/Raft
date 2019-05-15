import json
import socket

class Client:
    def __init__(self):

        self.server_address = ("127.0.0.1", 6001)
            
        

    def request(self, message):

        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client.connect(self.server_address)
        print("connecting")
        self.client.send(json.dumps(message).encode("utf-8"))
        
        response = bytes()
        response += self.client.recv(1024)

        self.client.close()
        response = response.decode("utf-8")
        response = json.loads(response)
        if response['type'] == 'redirect':
            self.server_address = tuple(response["leader_address"])
            self.request(message)
        elif response["success"]:
            print("{} succeed!".format(message["command"]))

    def append(self, data):
        self.request({'type':'client_upload','command':data})

    def t(self):
        self.append("upload")

