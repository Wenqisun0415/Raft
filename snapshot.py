import asyncio
import pickle

class Snapshot:
    def __init__(self, raft, port, last_include_index=0, last_include_term=0, data={}):
        self.raft = raft
        self.snapshot_file = "snapshot_" + str(port)
        self.last_include_index = last_include_index
        self.last_include_term = last_include_term
        self.data = data

    def get_last_include_index(self):
        return self.last_include_index

    def get_last_include_term(self):
        return self.last_include_term

    #todo...compact log
    def compact(self):
        self.last_include_index = self.raft.get_last_applied()#lastappliedindex
        self.last_include_term = self.raft.get_log_term(self.last_include_index) #term of lastappliedindex
        self.data = self.raft.get_state_machine() #state of state machine
        self.store_snapshot()
        self.change_log() #delete log <=commitindex
        
    # call this function when commit a log
    def start_compact(self):
        loop = asyncio.get_event_loop()
        loop.call_soon(self.compact)
       
    def store_snapshot(self):
        snapshot_msg = {
            "last_include_index": self.last_include_index,
            "last_include_term": self.last_include_term,
            "data": self.data
        }
        fileObject = open(self.snapshot_file, "wb")
        pickle.dump(snapshot_msg, fileObject)
        fileObject.close()
    #delete log <=lastsappliedindex

    def change_log(self):
        self.raft.snapshot_change_log(self.last_include_index)