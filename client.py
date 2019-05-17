import json
import socket
import random
import select

class Client:
    def __init__(self):

        self.server_addresses = [("10.12.43.212", 6001), ("10.12.231.81", 6001), ("10.13.61.65", 6001)]
        socket.setdefaulttimeout(1)
        

    def request(self, message):

        self.server_address = random.choice(self.server_addresses)

        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        

        try:
            self.client.connect(self.server_address)
            self.client.send(json.dumps(message).encode("utf-8"))


            response = bytes()
            response += self.client.recv(1024)

            self.client.close()
            response = response.decode("utf-8")
            response = json.loads(response)
            if response['type'] == 'redirect':
                self.server_address = tuple(response["leader_address"])
                self.request(message)
            else:
                print(response["result"])

        except (ConnectionRefusedError, socket.timeout):
            self.request(message)
        
    def insert(self, key, value):
        self.request({
            "type": "client_request",
            "command": "insert",
            "key": key,
            "value": value
        })

    def get(self, key):
        self.request({
            "type": "client_request",
            "command": "get",
            "key": key
        })

    def delete(self, key):
        self.request({
            "type": "client_request",
            "command": "delete",
            "key": key
        })
