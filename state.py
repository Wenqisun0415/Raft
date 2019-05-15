from persist import Persist
import random
import asyncio
from statistics import median
import logging.config
import json

logging.config.fileConfig(fname='file.conf', disable_existing_loggers=False)
logger = logging.getLogger("raft")

class State:

    def __init__(self, raft):
        self.raft = raft
    
    def receive_peer_message(self, peer, message):
        
        logger.info(f"Receive {message['type']} from {peer}")
        if self.raft.get_current_term() < message["term"]:
            self.raft.set_current_term(message["term"])
            if not type(self) is Follower:
                logger.info("Turn into follower due to higher term from other peer")
                self.raft.change_state(Follower)
                if message["type"] != "append_entries_response":
                    self.raft.state.receive_peer_message(peer, message)
                return
        print("Message type is {}".format(message["type"]))
        called_method = getattr(self, message["type"], None)
        called_method(peer, message)

    def receive_client_message(self, message, transport):
        if self.raft.get_leader() != self.raft.get_address():
            logger.info("Redirecting message to leader")
            new_message = {
                "type": "redirect",
                "leader_address": self.raft.get_leader()
            }
            self.raft.send_client_message(new_message, transport)
        else:
            msg = message["type"]
            #logger.info(f"Received {msg}")
            called_method = getattr(self, message["type"], None)
            called_method(message, transport)

class Follower(State):

    def __init__(self, raft):
        super().__init__(raft)
        self.raft.set_vote_for(None)
        self.reset_timer()

    def reset_timer(self):
        if hasattr(self, "follower_timer"):
            self.follower_timer.cancel()
        
        timeout = random.randint(200, 400)/200
        loop = asyncio.get_event_loop()
        self.follower_timer = loop.call_later(timeout, self.raft.change_state, Candidate)

    def leave_state(self):
        self.follower_timer.cancel() 

    def request_vote(self, peer, message):
        self.reset_timer()
        can_vote = self.raft.get_vote_for() == None or self.raft.get_vote_for() == message["candidate_id"]
        deny = (self.raft.get_last_log_term() > message["last_log_term"]) or\
            (self.raft.get_last_log_term() == message["last_log_term"] and self.raft.get_last_log_index() > message["last_log_index"])
        response = {"type": "vote_result", "term": self.raft.get_current_term()}
        if can_vote and not deny:
            response["vote_granted"] = True
        else:
            response["vote_granted"] = False
        self.raft.send_peer_message(peer, response)

    def append_entries(self, peer, message):
        self.reset_timer()
        self.raft.leader = tuple(message["leader_id"])
        response = {"type": "append_entries_response", "term": self.raft.get_current_term()}
        if message["term"] < self.raft.get_current_term():
            response["success"] = False
        elif self.raft.get_log_term(message["prev_log_index"]) != message["prev_log_term"]:
            response["success"] = False
        else:
            response["success"] = True
            self.raft.append_entries(message["prev_log_index"], message["entries"])

            #self.raft.commit_index = min(message["prev_log_index"], self.raft.get_last_log_index())
            self.raft.commit_index = min(message["leader_commit"], self.raft.get_last_log_index())
            #print("Commit index is {}".format(self.raft.commit_index))
            self.raft.apply_action(self.raft.get_commit_index())

        response["match_index"] = self.raft.get_last_log_index()
        self.raft.send_peer_message(peer, response)
        

class Candidate(State):

    def __init__(self, raft):
        super().__init__(raft)
        logger.info("Turning to candidate")
        self.received_vote_num = 0
        self.raft.increment_term()
        self.vote_self()
        self.reset_election_timer()
        self.send_request_vote()

    def vote_self(self):
        self.raft.set_vote_for(self.raft.get_address())
        self.received_vote_num += 1

    def leave_state(self):
        self.election_timer.cancel()

    def reset_election_timer(self):
        if hasattr(self, "election_timer"):
            self.election_timer.cancel()
        
        timeout = random.randint(200, 400)/200
        loop = asyncio.get_event_loop()
        self.election_timer = loop.call_later(timeout, self.raft.change_state, Candidate)

    def send_request_vote(self):
        logger.info("Sending out request vote")
        message = {
            "type": "request_vote",
            "term": self.raft.get_current_term(),
            "candidate_id": self.raft.get_address(),
            "last_log_index": self.raft.get_last_log_index(),
            "last_log_term": self.raft.get_last_log_term()
        }
        self.raft.broadcast(message)

    def vote_result(self, peer, message):
        if message["vote_granted"]:
            self.received_vote_num += 1
            logger.info(f"Current vote number: {self.received_vote_num}")
            if(self.received_vote_num > len(self.raft.cluster)/2):
                self.raft.change_state(Leader)
                logger.info("I'm the new leader")
            
    def append_entries(self, peer, message):
        logger.info("Turning to follower due to receiving from leader")
        self.raft.change_state(Follower)
        self.raft.state.append_entries(peer, message)

class Leader(State):

    def __init__(self, raft):
        super().__init__(raft)
        self.next_index = {}
        self.match_index = {}
        self.waiting_list = {}
        for peer in raft.get_cluster():
            self.next_index[peer] = self.raft.get_last_log_index() + 1
            self.match_index[peer] = 0
        self.send_append_entries()
        self.reset_heartbeat_timer()
        self.raft.set_leader(self.raft.get_address())

    def leave_state(self):
        self.heartbeat_timer.cancel()

    def send_append_entries(self):
        for peer in self.raft.get_cluster():
            if peer == self.raft.get_address():
                continue
            prev_log_index = self.next_index[peer] - 1
            message = {
                "type": "append_entries",
                "term": self.raft.get_current_term(),
                "prev_log_index": prev_log_index,
                "prev_log_term": self.raft.get_log_term(prev_log_index),
                "entries": self.raft.persist.data["log_manager"].log[prev_log_index:],
                "leader_commit": self.raft.get_commit_index(),
                "leader_id": self.raft.get_address()
            }
            self.raft.send_peer_message(peer, message)
        self.reset_heartbeat_timer()
        
    def reset_heartbeat_timer(self):
        if hasattr(self, "heartbeat_timer"):
            self.heartbeat_timer.cancel()
        
        timeout = 0.7
        loop = asyncio.get_event_loop()
        self.heartbeat_timer = loop.call_later(timeout, self.send_append_entries)

    def append_entries_response(self, peer, message):
        if message["success"]:
            self.next_index[peer] = message["match_index"] + 1
            self.match_index[peer] = message["match_index"]
            self.match_index[self.raft.get_address()] = self.raft.get_last_log_index()
            self.next_index[self.raft.get_address()] = self.raft.get_last_log_index() + 1

            # The median of the commit_index is the maximum log that appears on majority of servers
            self.raft.commit_index = median(self.match_index.values())
            #TODO commit
            self.raft.apply_action(self.raft.get_commit_index())
            self.respond_to_client()

        else:
            self.next_index[peer] -= 1

    def client_upload(self, message, transport):
        index = self.raft.get_last_log_index()
        entries = [{
            "term": self.raft.get_current_term(),
            "command": "upload"
        }]
        self.waiting_list[index] = transport
        self.raft.append_entries(index, entries)
        logger.info("Upload is recorded")

    def client_download(self, message, transport):
        index = self.raft.get_last_log_index()
        entries = [{
            "term": self.raft.get_current_term(),
            "command": "download"
        }]
        self.waiting_list[index] = transport
        self.raft.append_entries(index, entries)
        logger.info("Download is recorded")

    def client_delete(self, message, transport):
        index = self.raft.get_last_log_index()
        entries = [{
            "term": self.raft.get_current_term(),
            "command": "delete"
        }]
        self.waiting_list[index] = transport
        self.raft.append_entries(index, entries)
        logger.info("Delete is recorded")

    def respond_to_client(self):
        message = {
            "type": "result",
            "success": True
        }
        servered = []
        for client in self.waiting_list:
            if client <= self.raft.commit_index:
                self.raft.send_client_message(message, self.waiting_list[client])
                logger.info("Sending client result")
                servered.append(client)
        for client in servered:
            self.waiting_list.pop(client)
