from log import LogManager
import pickle
import os.path
import logging
import logging.config

logging.config.fileConfig(fname='file.conf', disable_existing_loggers=False)
logger = logging.getLogger("raft")

class Persist:

    def __init__(self, reset=False):
        if reset or not os.path.isfile("persist"):
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

    def append_entry(self, index, entry):
        self.data["log_manager"].append_entry(index, entry)
        self.serialize()
        logger.info("New entry appended")

    def get_current_term(self):
        return self.data["current_term"]

    def get_vote_for(self):
        return self.data["vote_for"]

    def serialize(self):
        """
        Store object into the disk
        data -- the object to be stored
        """
        fileObject = open("persist", "wb")
        pickle.dump(self.data, fileObject)
        fileObject.close()

    def deserialized(self):
        """
        Retrieve object from the disk
        """
        fileObject = open("persist", "rb")
        data = pickle.load(fileObject)
        return data


def test():
    persist = Persist()
    persist.append_entry(2, "update")
    persist.increment_term()
    newP = Persist()
    print(newP.data["log_manager"].get_log())


