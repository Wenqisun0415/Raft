from state import Follower
from persist import Persist
from snapshot import Snapshot
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
        self.snapshot = Snapshot(self, port)

    def change_state(self, new_state):
        self.state.leave_state()
        self.state = new_state(self)

    def receive_peer_message(self, peer, message):
        self.state.receive_peer_message(peer, message)

    def receive_client_message(self, message, transport):
        self.state.receive_client_message(message, transport)

    def send_peer_message(self, peer, message):
        json_msg = json.dumps(message)
        if peer != self.address:
            self.transport.sendto(json_msg.encode(), peer)

    def send_client_message(self, message, transport):
        json_msg = json.dumps(message)
        transport.write(json_msg.encode())

    def broadcast(self, message):
        for peer in self.cluster:
            self.send_peer_message(peer, message)

    def increment_term(self):
        self.persist.increment_term()

    def get_current_term(self):
        return self.persist.get_current_term()

    def set_current_term(self, term):
        self.persist.set_current_term(term)

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

    def get_last_log_index(self):
        return self.persist.get_last_log_index() + self.snapshot.get_last_include_index()

    def get_last_log_term(self):
        return self.persist.get_last_log_term()

    def get_cluster(self):
        return self.cluster
    
    def get_commit_index(self):
        return self.commit_index
    
    def set_commit_index(self, index):
        self.commit_index = index

    def get_last_applied(self):
        return self.last_applied

    def set_last_applied(self, applied):
        self.last_applied = applied

    def get_log_term(self, index):
        return self.persist.get_log_term(index)

    def get_state_machine(self):
        return self.state_machine

    def set_state_machine(self, new_state_machine):
        self.state_machine = new_state_machine

    def append_entries(self, index, entry):
        self.persist.append_entries(index, entry)

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

        # with open(self.state_file, 'w') as f:
        #     f.write(str(self.state_machine))

    def snapshot_change_log(self, last_include_index):
        self.persist.snapshot_change_log(last_include_index)

    def get_last_include_index(self):
        return self.snapshot.get_last_include_index()

    def get_last_include_term(self):
        return self.snapshot.get_last_include_term()

    def compact_check(self):
        last_include_index = self.get_last_include_index()
        if self.last_applied - last_include_index > 50:
            self.snapshot.start_compact()