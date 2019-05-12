
import logging



class LogManager:

    def __init__(self, log=[]):
        self.log = log

    def append_entry(self, index, entry):
        """
        Here index starts from 1
        """
        if index == len(self.log) + 1:
            self.log.append(entry)
        elif index <= len(self.log):
            self.log = self.log[:index-1]
            self.log.append(entry)
        else:
            #logger.error("Append entry error!")
            pass

    def get_log(self):
        return self.log


