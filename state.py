#this file includes the logic processign of three states in Raft.
from persist import Persist
import random
import asyncio
from statistics import median
import logging.config
import json

logging.config.fileConfig(fname='file.conf', disable_existing_loggers=False)
logger = logging.getLogger("raft")


class State:
# 'State' class is the basic class of 'Follower', 'Candidate' and 'Leader'.
    def __init__(self, raft):
        self.raft = raft
    
    def receive_peer_message(self, peer, message):
        # this method deal with messages received from peer server node.
        logger.info(f"Receive {message['type']} from {peer}")
                    
        #when 'term' received is larger than node own term, update, and turn to 'Follower'.
        if self.raft.get_current_term() < message["term"]:
            self.raft.set_current_term(message["term"])
            if not type(self) is Follower:
                logger.info("Turn into follower due to higher term from other peer")
                self.raft.change_state(Follower)
                self.raft.state.receive_peer_message(peer, message)
                return
        print("Message type is {}".format(message["type"]))
        
        #according to message type, call correspondent methods.
        called_method = getattr(self, message["type"], None)
        called_method(peer, message)

    def receive_client_message(self, message, transport):
        #this method deal with messages received from client.
        #if the node is not a leader, it returns leader address to client. else calling correspondent methods.
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
#this class is responsible for follower's processing logic.
    def __init__(self, raft):
        super().__init__(raft)
        self.raft.set_vote_for(None)
        self.reset_timer()

    def reset_timer(self):
    # if timeout, follower will start a new countdown. 
        if hasattr(self, "follower_timer"):
            self.follower_timer.cancel()
        
        timeout = random.randint(300, 400)/200
        loop = asyncio.get_event_loop()
        self.follower_timer = loop.call_later(timeout, self.raft.change_state, Candidate)

    def leave_state(self):
        self.follower_timer.cancel() 

    def request_vote(self, peer, message):
    #when receive vote request from Candidate, reply false if its own term > received term.
    #if vote_for is None or received "candidate_id", and candidate's log is at least as up-to-date as own's, reply True.
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
    # receive "append entries" request from leader.
        self.reset_timer()
        self.raft.leader = tuple(message["leader_id"])
               
        #return current term, for leader to update itself.
        response = {"type": "append_entries_response", "term": self.raft.get_current_term()}                   
        print("Prev_log_index is {}".format(message["prev_log_index"]))
                    
        #if follower's current term > leader's term, return false.
        #if follower's log does not contain an entry at "prev_log_index" whose term matches "prev_log_term", return false.
        #else,return true.
        if message["term"] < self.raft.get_current_term():
            response["success"] = False
        elif self.raft.get_log_term(message["prev_log_index"]) != message["prev_log_term"]:
            response["success"] = False
        else:
            response["success"] = True
            logger.info(f"Now appending entries {message['entries']} with index {message['prev_log_index']}")
            #append new entry
            self.raft.append_entries(message["prev_log_index"], message["entries"])

            #update follower's commit index.
            self.raft.commit_index = min(message["leader_commit"], self.raft.get_last_log_index())
            #apply to state machine according to commit index.
            self.raft.apply_action(self.raft.get_commit_index())
        #return match index to leader.
        response["match_index"] = self.raft.get_last_log_index()
        self.raft.send_peer_message(peer, response)

    def append_entries_response(self, peer, message):
        pass
        
    def vote_result(self, peer, message):
        pass

class Candidate(State):
#this class is responsible for candidate's processing logic.
    def __init__(self, raft):
        super().__init__(raft)
        logger.info("Turning to candidate")
        self.received_vote_num = 0
        self.raft.increment_term()
        self.vote_self()
        self.reset_election_timer()
        self.send_request_vote()

    def vote_self(self):
    #candidate votes itself.
        self.raft.set_vote_for(self.raft.get_address())
        self.received_vote_num += 1

    def leave_state(self):
        self.election_timer.cancel()

    def reset_election_timer(self):
    #if timeout, start a new election.
        if hasattr(self, "election_timer"):
            self.election_timer.cancel()
        
        timeout = random.randint(300, 400)/200
        loop = asyncio.get_event_loop()
        self.election_timer = loop.call_later(timeout, self.raft.change_state, Candidate)

    def send_request_vote(self):
    # broadcast vote request to all servers.
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
    #if "vote_granted" is true, means get one vote.
    #if more than half of the servers respond with true, turn to leader.
        if message["vote_granted"]:
            self.received_vote_num += 1
            logger.info(f"Current vote number: {self.received_vote_num}")
            if(self.received_vote_num > len(self.raft.cluster)/2):
                self.raft.change_state(Leader)
                logger.info("I'm the new leader")
            
    def append_entries(self, peer, message):
    #if receive 'append entry' request from a leader which has the same term, candidate turns to follower.
        logger.info("Turning to follower due to receiving from leader")
        self.raft.change_state(Follower)
        self.raft.state.append_entries(peer, message)

    def append_entries_response(self, peer, message):
        pass

    def request_vote(self, peer, message):
        pass

class Leader(State):
#this class is responsible for leader's processing logic.
    def __init__(self, raft):
        super().__init__(raft)
        self.next_index = {}
        self.match_index = {}
        self.waiting_list = {}
        #initial 'next_index' and 'match_index'.
        for peer in raft.get_cluster():
            self.next_index[peer] = self.raft.get_last_log_index() + 1
            self.match_index[peer] = 0
        self.send_append_entries()
        self.reset_heartbeat_timer()
        self.raft.set_leader(self.raft.get_address())

    def leave_state(self):
        self.heartbeat_timer.cancel()

    def send_append_entries(self):
    #send 'append entry' request to all other nodes.
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
    #set timeout for heartbeat package, heartbeat packets are sent continuously.
        if hasattr(self, "heartbeat_timer"):
            self.heartbeat_timer.cancel()
        
        timeout = 0.8
        loop = asyncio.get_event_loop()
        self.heartbeat_timer = loop.call_later(timeout, self.send_append_entries)

    def append_entries_response(self, peer, message):
    #receive response of 'append entries' from other nodes.
        if message["success"]:
        #if append successfully, update leader's 'next_index' (+1) and 'match_index'.
            self.next_index[peer] = message["match_index"] + 1
            self.match_index[peer] = message["match_index"]
            self.match_index[self.raft.get_address()] = self.raft.get_last_log_index()
            self.next_index[self.raft.get_address()] = self.raft.get_last_log_index() + 1

            # The median of the commit_index is the maximum log that appears on majority of servers
            self.raft.commit_index = median(self.match_index.values())
            # after updating 'commit_index' of leader, apply and response to client.
            self.raft.apply_action(self.raft.get_commit_index())
            self.respond_to_client()

        else:
        #if append unsuccessfully, update leader's 'next_index' (-1).
            self.next_index[peer] -= 1

    def client_request(self, message, transport):
    #receive client's request message.
        entries = [{
            "term": self.raft.get_current_term(),
            "command": message["command"],
            "key": message["key"],
            "value": message["value"] if "value" in message else None
        }]
        #according to length of log, update waiting list of cllent.
        #hence, one client can receive its own response of 'append entry'.
        index = self.raft.get_last_log_index()
        self.waiting_list[index] = transport
        self.raft.append_entries(index, entries)
        logger.info("Upload is recorded")

    def respond_to_client(self):
    #after apply several entries to state machine, leader sends responses to correspondent clients.
    #then delete from waiting list.
        message = {
            "type": "result"
        }
        servered = []
        for client in self.waiting_list:
            if client <= self.raft.commit_index:
                message["result"] = self.raft.result[client]
                self.raft.send_client_message(message, self.waiting_list[client])
                logger.info("Sending client result")
                servered.append(client)
        for client in servered:
            self.waiting_list.pop(client)
            self.raft.result.pop(client)

    def vote_result(self, peer, message):
        pass

    def append_entries(self, peer, message):
        pass
