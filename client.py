import json
import socket
import random
import select

class Client:
    def __init__(self):

        self.server_addresses = [("10.12.43.212", 6001), ("10.12.231.81", 6001), ("10.13.61.65", 6001)]
        
            
        

    def request(self, message):

        self.server_address = random.choice(self.server_addresses)

        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.client.setblocking(0)

        try:
            self.client.connect(self.server_address)
            self.client.send(json.dumps(message).encode("utf-8"))

            ready = select.select([self.client], [], [], 1)
            if ready[0]:
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
            else:
                self.request(message)
        except ConnectionRefusedError:
            self.request(message)
        

    def append(self, data):
        self.request({'type':'client_'+str(data) ,'command':data})

    def t(self):
        self.append("upload")

