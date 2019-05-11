import pickle
import logging



class LogManager:

    def __init__(self, log, reset=False):
        if reset:
            self.log = []
        else:
            self.log = log

    def append_entry(self, index, entry):
        """
        Here index starts from 1
        """
        if index == len(self.log) + 1:
            self.log.append(entry)
        elif index <= len(self.log):
            self.log = self.log[:index-1].append(entry)
        else:
            logger.error("Append entry error!")

    @property
    def log(self):
        return self.log
    

def serialize(data):
    """
    Store object into the disk
    data -- the object to be stored
    """
    fileObject = open("persist", "wb")
    pickle.dump(data, fileObject)
    fileObject.close()

def deserialized():
    """
    Retrieve object from the disk
    """
    fileObject = open("persist", "rb")
    data = pickle.load(fileObject)
    return data


