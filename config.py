import os

class Config:
    def __init__(self, config={}):
        if config is None:
            self.__dict__ = {}

        elif config:
            self.__dict__ = config


