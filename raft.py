'''
Project: 
Distributed KV storage system with Raft algorithm

Author:
Wenqi Sun 928630	Huiya Chen 894933
Yishan Shi 883166	Shaobo Wang 935596
'''

from state import Follower
from persist import Persist
import json
import logging

logging.config.fileConfig(fname='file.conf', disable_existing_loggers=False)
logger = logging.getLogger("raft")

class Raft:

    def __init__(self, config, port, state=Follower):
        self.persist = Persist(port, reset=False)
        self.state = state(self)
        self.cluster = config["cluster"]
        self.address = config["address"]
        self.commit_index = 0
        self.last_applied = 0
        self.leader = None
        self.transport= None
        self.client_transport = None
        self.state_machine = {}
        self.state_file = "state_" + str(port)
        self.result = {}
        
    # Server will change its role from follower, candidate and leader by its state
    def change_state(self, new_state):
        self.state.leave_state()
        self.state = new_state(self)
        
    # Message received between servers
    def receive_peer_message(self, peer, message):
        self.state.receive_peer_message(peer, message)
        
    # Message received by client
    def receive_client_message(self, message, transport):
        self.state.receive_client_message(message, transport)

    def send_peer_message(self, peer, message):
        json_msg = json.dumps(message)
        if peer != self.address:
            self.transport.sendto(json_msg.encode(), peer)

    def send_client_message(self, message, transport):
        json_msg = json.dumps(message)
        transport.write(json_msg.encode())
    
    # Send message from leader to followers
    def broadcast(self, message):
        for peer in self.cluster:
            self.send_peer_message(peer, message)
            
    # Increase term of server
    def increment_term(self):
        self.persist.increment_term()

    def get_current_term(self):
        return self.persist.get_current_term()

    def set_current_term(self, term):
        self.persist.set_current_term(term)
        
    # Voting
    def get_vote_for(self):
        return self.persist.get_vote_for()

    def set_vote_for(self, candidate):
        self.persist.set_vote_for(candidate)

    def get_leader(self):
        return self.leader

    def set_leader(self, new_leader):
        self.leader = new_leader

    def get_address(self):
        return self.address
    
    # get the index of recent log
    def get_last_log_index(self):
        return self.persist.get_last_log_index()
    
    # get the index of recent term
    def get_last_log_term(self):
        return self.persist.get_last_log_term()

    def get_cluster(self):
        return self.cluster
   
    def get_commit_index(self):
        return self.commit_index
    
    def set_commit_index(self, index):
        self.commit_index = index
    
    # Get the last command that state_machine has finished
    def get_last_applied(self):
        return self.last_applied

    def set_last_applied(self, applied):
        self.last_applied = applied
        
    # get the recent log term
    def get_log_term(self, index):
        return self.persist.get_log_term(index)

    def append_entries(self, index, entry):
        self.persist.append_entries(index, entry)
    
    # according to 'commit_index' and 'last_applied' index, apply commands in log entries to state machine.
    def apply_action(self, commit_index):

        logs = self.persist.data["log_manager"].log[self.last_applied:self.commit_index]
        index = self.last_applied
        for log in logs:
            if log["command"] == "insert":
                self.state_machine[log["key"]] = log["value"]
                self.result[index] = "OK"
            elif log["command"] == "get":
                if log["key"] in self.state_machine:
                    self.result[index] = self.state_machine[log["key"]]
                else:
                    self.result[index] = "(nil)"
            elif log["command"] == "delete":
                if log["key"] in self.state_machine:
                    del self.state_machine[log["key"]]
                    self.result[index] = "(integer) 1"
                else:
                    self.result[index] = "(integer) 0"
            else:
                logger.error("Client command fatal error")
            index += 1
            
            self.last_applied += 1
        #write the contents of the state machine to disk, for checking consistency between servers.
        with open(self.state_file, 'w') as f:
            f.write(str(self.state_machine))
