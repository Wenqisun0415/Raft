class Snapshot:
    def __init__(self, port,last_include_index=0,last_include_term=0,data={}):
        self.snapshot_file = "Snapshot_" + str(port)
        self.last_include_index = last_include_index
        self.last_include_term = last_include_term
        self.data = data

    def get_last_include_index(self):
        return self.last_include_index


    #todo...compact log
    def compact(self,last_applied_index,last_include_term,data):
        self.last_include_index = last_applied_index;#lastappliedindex
        self.last_include_term = last_include_term; #term of lastappliedindex
        self.data=data; #state of state machine
        self.storesnapshot();
        self.changelog(); #delete log <=commitindex
        

    # call this function when commit a log
    def start_compact(self):
        loop = asyncio.get_event_loop()
        last_applied_index=self.raft.get_last_applied_index()
        last_include_term=self.raft.get_log_term(last_applied_index)
        data=self.raft.get_state_machine_data()
        loop.call_soon(compact,last_applied_index,last_include_term,data)
       
    def storesnapshot(self):
        snapshot_msg={
        "last_include_index": self.last_include_index
        "last_include_term": self.last_include_term
        "data": self.data
        }
        fileObject = open(self.snapshot_file, "wb")
        pickle.dump(snapshot_msg, fileObject)
        fileObject.close()
    #delete log <=lastsappliedindex
    def changelog(self):
        self.raft.snapshot_change_log(last_include_index)
