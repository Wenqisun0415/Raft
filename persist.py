from log import LogManager
import pickle
import os.path
import logging
import logging.config

logging.config.fileConfig(fname='file.conf', disable_existing_loggers=False)
logger = logging.getLogger("raft")

# State the basic interfaces between follower, candidate and leader
class Persist:

    def __init__(self, port, reset=False):
        self.log_file = "persist_" + str(port)
        if reset or not os.path.isfile(self.log_file):
            self.data = {
                "current_term": 0,
                "vote_for": None,
                "log_manager": LogManager()
            }
        else:
            self.data = self.deserialized()

    def increment_term(self):
        self.data["current_term"] += 1
        self.serialize()
        logger.info(f"Current term increased to {self.get_current_term()}")

    def vote(self, candidate):
        self.data["vote_for"] = candidate
        self.serialize()
        logger.info(f"Vote for {self.get_vote_for()}")

    def append_entries(self, index, entry):
        self.data["log_manager"].append_entries(index, entry)
        self.serialize()
        logger.info("New entry appended")

    def get_current_term(self):
        return self.data["current_term"]

    def set_current_term(self, term):
        self.data["current_term"] = term

    def get_vote_for(self):
        return self.data["vote_for"]

    def set_vote_for(self, candidate):
        self.data["vote_for"] =  candidate

    def get_last_log_index(self):
        return self.data["log_manager"].get_last_log_index()

    def get_last_log_term(self):
        return self.data["log_manager"].get_last_log_term()

    def get_log_term(self, index):
        return self.data["log_manager"].get_log_term(index)


    def serialize(self):
       
        # Store object into the disk
  
        fileObject = open(self.log_file, "wb")
        pickle.dump(self.data, fileObject)
        fileObject.close()

    def deserialized(self):
    
        # Get object from the disk
        
        fileObject = open(self.log_file, "rb")
        data = pickle.load(fileObject)
        return data



