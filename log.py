'''
Project: 
Distributed KV storage system with Raft algorithm

Author:
Wenqi Sun 928630	Huiya Chen 894933
Yishan Shi 883166	Shaobo Wang 935596
'''

import logging.config

logging.config.fileConfig(fname='file.conf', disable_existing_loggers=False)
logger = logging.getLogger("raft")

class LogManager:

    def __init__(self, log=[]):
        self.log = log
    
    # State the function for retriving content and index of log stored by each server
    def append_entries(self, index, entry):
       
       # Here index starts from 1
       
        if index == len(self.log):
            self.log = self.log[:] + entry
        elif index < len(self.log):
            self.log = self.log[:index] + entry
        else:
            logger.error("Append entry error!")

    def get_last_log_index(self):
        return len(self.log)

    def get_last_log_term(self):
        if self.log:
            return self.log[-1]["term"]
        else:
            return 0

    def get_log_term(self, index):
        if index == 0:
            return 0
        elif index > len(self.log):
            return 0
        return self.log[index-1]["term"]

    def get_log(self):
        return self.log
